# WS2 — dbt serving layer, evidence record

**Date:** 2026-08-03 · **Code state:** the WS2 working tree on `main` (see the
commit that lands `transform/dbt/`). Decisions recorded in
[ADR-0002](adr/0002-dbt-transform-layer.md).

This is the runnable-artifact record for the dbt transform layer: four models
(`stg_gold` → `as_of_facts` → `filing_coverage` / `restatement_freq`) over the
gold Delta table via duckdb `delta_scan`, plus three singular tests encoding the
PIT invariants. Every number below was recomputed from the duckdb file or from
raw Delta log arithmetic at the time of writing — none is restated from a
subagent's summary.

## Verdict: **models green, invariants non-vacuous, negative control fires**

| Part | Requirement | Status |
|---|---|---|
| 1 | Full `dbt build` green on the dev lake | **[VALIDATED]** 2026-08-03 — `PASS=17 WARN=0 ERROR=0 SKIP=0`, exit 0 |
| 2 | The green is not vacuous — models carry real rows | **[VALIDATED]** — `stg_gold` 113,228; `as_of_facts` 111,931; `filing_coverage` 3,509; `restatement_freq` 1,297 |
| 3 | The as-of predicate actually filters | **[VALIDATED]** — 113,228 → 111,931; the 1,297 hidden rows equal the 1,297 restated keys exactly |
| 4 | `assert_as_of_key_unique` demonstrably red on a mutated twin | **[VALIDATED]** — planted 7, test reported `FAIL 7`; independently re-run at a different violation count than the build's own run |
| 5 | The Scala suite still green with the fixture main added | **[VALIDATED]** — 40 tests, 0 failed, `sbt scalafmtAll` + `scalafixAll --check` clean |
| 6 | CI fixture path runs end to end | **[VALIDATED]** — fixture built by `Pipeline.runFromFrames`; `dbt build --target ci` `PASS=17` |
| 7 | The read-only backfill lake untouched | **[VALIDATED]** — nothing under `lake-backfill/` modified 2026-08-03; newest artifact is 2026-07-08 |

A test never seen red is unevaluable. Part 4 exists so this record cannot claim
anything from a green-only run.

## The dev-lake build, verbatim

```
Finished running 2 table models, 13 data tests, 2 view models in 0 hours 0 minutes and 0.75 seconds (0.75s).
Completed successfully
Done. PASS=17 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=17
```

Row counts, read back out of `transform/dbt/target/vantage_dev.duckdb`:

```
stg_gold           113,228
as_of_facts        111,931
filing_coverage      3,509
restatement_freq     1,297

hidden by as-of filter : 1,297
restated keys          : 1,297   <- equal
SUM(fact_count) over filing_coverage = 111,931 = as_of_facts   <- reconciles
```

`stg_gold = 113,228` was cross-checked two independent ways: duckdb over the
model, and add-file arithmetic across the gold `_delta_log` live snapshot
(10272+10741+10682+10217+10564+10758+10749+10718+10266+18261 = 113,228).

The three invariants, recomputed directly against the duckdb rather than through
the dbt test wrapper:

```
duplicate as-of keys = 0    lookahead rows = 0    bad intervals = 0
```

The equality in part 3 is the structurally interesting one: on this lake every
restated natural key carries exactly two versions, so the count of rows the
as-of window hides is precisely the count of keys `restatement_freq` reports.

## Negative control — the test proven able to fail

`scripts/dbt_mutate_twin.py` copies gold into a twin (refusing any path without
`twin` in it, exit 3) and overwrites the *superseded* row's `valid_to` with the
open sentinel for N natural keys, making two versions of the same key visible at
once. Run at N=7:

```
9 of 17 FAIL 7 assert_as_of_key_unique ......................................... [FAIL 7 in 0.11s]
Done. PASS=13 WARN=0 ERROR=1 SKIP=3 NO-OP=0 REUSED=0 TOTAL=17
```

Manifest planted 7 → test reported `FAIL 7`. This was run at a violation count
different from the one used during the build itself, so the reconciliation
tracks the planted number rather than reproducing a fixed constant. The three
downstream nodes SKIP because the failing test blocks them — the same
structural unreachability the WS1 DAG relies on.

The mutator needs `pyarrow`, which is **not** in `.venv-dbt`; run it under the
system Python, as with `gate_b_mutate_twin.py`.

## CI fixture — what it does and does not prove

The SEC source data lives outside the repo and cannot be checked in, so CI
cannot ingest a real quarter. `src/test/scala/pit/tools/DbtFixtureMain.scala`
instead drives **`Pipeline.runFromFrames`** — the same production path
`PipelineRestatementSpec` exercises, which runs the silver stage and then
`GoldRebuild.rebuild` — over synthesized filings, across two batches so that a
genuine same-key restatement lands in gold.

```
DbtFixtureMain: rows=29 distinct_ciks=3 restated_keys=2
dbt build --target ci -> Done. PASS=17 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=17
```

Read back from `vantage_ci.duckdb`: `stg_gold` 29, `as_of_facts` 27,
`filing_coverage` 9, `restatement_freq` 2 — the two restated keys being
`(1001, Assets, 20230331, 1)` and `(1002, Revenues, 20230630, 1)`, each with 2
versions and 2 distinct `adsh`.

**The honesty boundary:** the code path that writes this Delta table is the
production one; the filings fed into it are not real SEC data. CI therefore
proves the dbt models and PIT tests run correctly against genuine pipeline
output of a known shape. It does not prove anything about real-quarter data —
that remains the job of the local `dev`/`full` targets and Gate A.

Nor is the CI job offline: it `pip install`s dbt and duckdb fetches the `delta`
extension from `extensions.duckdb.org` on first use. "Hermetic" here means no
cloud credential, no external warehouse, no cluster.

## Scope not covered

- The `full` target (86,615,392-row backfill lake) has **not** been run end to
  end in this record. Only `dev` and `ci` are evidenced above.
- `assert_no_lookahead` and `assert_interval_sane` are green and were shown
  non-vacuous by recomputation, but neither has been driven red by a planted
  violation. Only `assert_as_of_key_unique` has a mutator. Extending
  `dbt_mutate_twin.py` with a lookahead mutation is the obvious follow-up —
  `gate_b_mutate_twin.py` already plants exactly that shape for the Gate B
  audit, so the mechanism exists and is not being claimed here.
