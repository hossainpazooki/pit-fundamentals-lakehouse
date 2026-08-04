# 0002 -- dbt serving layer over the gold Delta table

Status: Accepted.

## Context

ADR 0001 built the local Airflow DAG that sequences silver -> gold -> audit
-> publish and Gate B (see `docs/GATE-B-WAP-EVIDENCE.md`) published the same
gold table to Databricks. Both leave VANTAGE with a single wide fact table --
`cik, tag, ddate, qtrs, uom, value, accepted, adsh, version, valid_from,
valid_to` -- and no serving layer of its own. Every consumer question --
"what did this filer report as of date D", "how many quarters of history do
we hold for a filer", "how often does a given fact get restated" -- currently
means writing a bespoke script that re-derives the as-of visibility rule
(`valid_from <= T < valid_to`) from a raw Delta scan by hand, with no shared,
tested place that rule lives. This work stream (WS2) adds a dbt project that
expresses that as-of semantics once, as SQL models with tests attached, and
builds two summary marts on top of it.

Two repo facts constrain the design:

- **The gold table is the single natural key surface.** The PIT key is the
  5-tuple `(cik, tag, ddate, qtrs, uom)`; a restatement is the same key
  reappearing under a different `adsh` with a later `accepted`, carrying its
  own `valid_from`/`valid_to` pair. Any serving model that collapses history
  to "current" necessarily throws the restatement history away -- which is
  fine for an as-of view and wrong for a mart that reports on restatement
  frequency.
- **The gold table already lives outside the repo, as Delta.** WS1 and Gate B
  both write and read it as a Delta table under `vantage-data/`
  (`lake-backfill/gold` for the full history, `scratch-airflow/lake/gold` for
  the small dev/twin copy). A serving layer therefore needs a query engine
  that can read Delta directly, not a second copy of the data in a different
  format.

## Decision

Build a dbt project at `transform/dbt/` (`dbt_project.yml` name `vantage`,
profile `vantage`) with a linear model chain --
`stg_gold` -> `as_of_facts` -> `{filing_coverage, restatement_freq}` -- and
three singular tests encoding the PIT invariants, wired as follows.

**dbt-duckdb over dbt-databricks.** duckdb's `delta_scan()` reads the gold
Delta table directly off disk, so the whole transform layer -- staging model,
as-of view, both marts, and all three tests -- runs locally, hermetically,
offline, and at no cost, and CI needs no cloud credential to exercise it. This
is a CI-hermeticity and cost choice, not a capability limit: the Databricks
serverless path is proven to work in this repo already (Gate B's WAP publish,
credited 2026-07-19), so dbt-databricks against that same workspace remains
an option if a warehouse-native serving layer is ever wanted. Revisiting this
choice is explicitly left open, not closed off by this ADR.

**The as-of boundary is a dbt var rendered through one macro, and a literal
UTC offset was rejected after verifying why it's wrong.** `as_of_date` is a
`dbt_project.yml` var (default `2026-03-31`), and every model or test that
needs an as-of instant calls `{{ as_of_ts() }}` -- never its own copy of the
boundary logic. The macro is exactly:

```sql
{% macro as_of_ts() %}
  (TIMESTAMP '{{ var("as_of_date") }} 23:59:59' AT TIME ZONE 'America/New_York')
{% endmacro %}
```

The alternative -- hardcoding an offset like `'2026-03-31 23:59:59-04'` --
was rejected on a verified, not assumed, basis: duckdb's `AT TIME ZONE
'America/New_York'` was checked live and yields `-04` for a summer date and
`-05` for a winter one, so a single literal offset silently moves the as-of
instant by an hour for roughly half the calendar. That is the exact bug class
this project's Gate A (`docs/no-lookahead` invariant) exists to catch, just
relocated into the serving layer instead of the pipeline -- a wrong-but-quiet
boundary shift is worse than a loud failure, because every downstream row
still looks plausible. Routing every as-of instant through one macro means
the DST rule is verified once and every model/test inherits it, rather than
each one re-deriving (and potentially re-breaking) it.

**Half-open visibility window: `valid_from <= T < valid_to`.** `as_of_facts`
selects rows where the as-of instant falls in `[valid_from, valid_to)`, not a
closed or open-open interval. This matters exactly at the seam between two
versions of the same natural key: a restatement's `valid_from` is set to the
instant the prior version's `valid_to` closes, and a closed interval on both
ends (or an open one on both ends) would either double-count or drop the row
whose boundary lands exactly on `T`. Half-open, closed-at-`valid_from`, keeps
consecutive versions of a key from ever overlapping and from ever leaving a
gap, at any instant.

**`restatement_freq` is built from `stg_gold`, not `as_of_facts`.** This mart
answers "how often does this fact get restated", which is a question about
the full version history of a natural key -- but `as_of_facts` is defined to
hold exactly one visible row per natural key at a single instant by
construction (that's what the visibility window and
`assert_as_of_key_unique` both enforce). Building `restatement_freq` on top
of `as_of_facts` would make it structurally incapable of ever seeing more
than one version per key, no matter what the underlying data contains. Going
back to `stg_gold` -- the untouched passthrough of every row, every version,
every `adsh` -- is the only place that history is still visible, which is the
whole reason this mart exists as a separate model rather than a query against
the as-of view.

**No dbt packages, no `dbt deps`.** Reaching for a packaged dbt-utils macro
(e.g. for uniqueness or interval checks) would put a resolved-at-build-time
dependency between the test and the SQL it runs, and pin the PIT invariants --
the repo's load-bearing guarantees -- to a third-party macro's semantics.
Keeping them as plain SQL means the assertion a reader sees is the assertion
that executes.

Be precise about what this does NOT buy: the CI job is **not offline**. It
`pip install`s dbt, and duckdb fetches the `delta` extension from
`extensions.duckdb.org` on first use. "Hermetic" here means no cloud
credential, no external warehouse, and no cluster -- not the absence of a
network. Instead, the three PIT invariants -- no lookahead, as-of key uniqueness,
interval sanity -- are written as plain-SQL singular tests
(`assert_no_lookahead.sql`, `assert_as_of_key_unique.sql`,
`assert_interval_sane.sql`) that select the offending rows directly, with no
package indirection between the test and the SQL it runs.

**CI's Delta fixture is built by running the production pipeline code over
synthesized filings, not by hand-authoring a fixture Delta table.** The real
SEC source data lives in `vantage-data/`, outside the repo, and cannot be
checked into CI. `ci-fixture/gold` is instead produced by invoking
`Pipeline.runFromFrames` and `GoldRebuild` -- the same code path WS1's DAG and
Gate B both drive against the real lake -- over a small set of synthesized
filing frames. This means the dbt project's CI target is tested against a
Delta table shaped by the real gold-write code (the MERGE logic, the
`valid_from`/`valid_to` interval-closing, the schema), not against a table
whose structure a test author guessed at by reading the schema doc. Be
precise about what this does and does not establish: it proves the dbt models
and tests behave correctly against *a* gold table with the right shape and
plausible restatement patterns; it says nothing about their behavior against
the real SEC-wide value distribution, tag vocabulary, or restatement
frequency, because none of that is synthesized into the fixture.

## Consequences

- **CI gets slower.** Every CI run that touches the dbt project first has to
  run the Scala pipeline code to materialize `ci-fixture/gold` before dbt can
  build anything on top of it, rather than reading a fixture that was
  checked in as static files.
- **The duckdb serving copy is single-node and read-only.** duckdb is an
  embedded, single-process engine; it is not a shared serving endpoint
  multiple consumers can query concurrently the way the Databricks workspace
  is, and it only reads the gold Delta table -- it never writes back to it.
  Anyone who needs a shared or concurrent-write serving surface still needs
  the Databricks path, not this one.
- **`as_of_date` is a build-time parameter, not a runtime one.** Because the
  as-of instant is baked into `as_of_facts` (and both marts built on top of
  it) at `dbt run` time via a var, serving two different as-of dates at once
  means two separate dbt builds against two separate duckdb target databases
  -- there is no single build that can answer an as-of query for an arbitrary
  date supplied at query time.
- **The `dev`/`full`/`ci` targets share one profile but very different data
  volumes**, and `full` points at the read-only 86,615,392-row production
  lake (`vantage-data/lake-backfill/gold`) with no guard in the dbt project
  itself preventing an accidental `--target full` run from a workflow that
  meant to run against `dev`. The gold path is always supplied on the command
  line via `--vars` rather than defaulted in `profiles.yml` specifically so
  that no target silently falls back to the wrong lake, but nothing stops a
  caller from passing the wrong path explicitly. This is a named residual
  gap, not a claimed guard.
- **The CI fixture's honesty boundary (see Decision above) is a permanent
  property of this design, not a temporary gap to be closed.** The real SEC
  source data cannot move into CI, so CI coverage of this dbt project will
  always be a real-code/synthetic-data test, never a real-code/real-data one;
  only local runs against `dev` or `full` targets exercise the real value
  distribution.
