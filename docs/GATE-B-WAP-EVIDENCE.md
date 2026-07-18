# Gate B — WAP-shaped publish evidence record

**Date:** 2026-07-18 · **Code state:** HEAD `85ab2cb` + this session's uncommitted work.
This is the runnable-artifact record for the first WAP-shaped firing of the
three-part publish-credit rule (rigor ADR-0005 resolution 3 — **Proposed, not
ratified**; this firing exercises the shipped skills `verify-the-effect`,
`data-quality-fail-closed`, `no-lookahead`, and quotes the ADR as direction, not
settled practice). VANTAGE is the origin repo for those skills: this firing earns
**no promotion credit** and does not unlock ADR-0005's bridge doc.

## Credit verdict, fail-closed: **NOT YET CREDITED**

| Part | Requirement | Status |
|---|---|---|
| 1 | Audit green on the staged candidate | **[VALIDATED]** locally (below) |
| 2 | Same audit demonstrably red on a mutated twin | **[VALIDATED]** locally (below) |
| 3 | Post-publish consumer-path probe + negative control | **[BASELINE]** machinery proven non-vacuous locally; the real probe against workspace gold is **blocked on operator auth** |

An audit never seen red is unevaluable and unevaluable halts — part 2 exists so
this record can never claim credit from a green-only audit. Because part 3 has
not run against the *published* state, the Databricks publish is **not credited**;
nothing here says otherwise.

## The audit (parts 1–2 artifact)

`scripts/gate_b_audit.py` — data-facing PIT invariants over a lake's gold Delta
snapshot, parameterized by `--lake` so the identical audit runs on candidate and
twin. Three-outcome fail-closed: exit 0 green / 1 red / 2 unevaluable (missing
table, missing columns, zero rows — halts, never coerced). Checks: A1
no-lookahead (`valid_from < accepted`), A2 no inverted interval, A2b every empty
interval has a same-instant winner, A3 no overlap among non-empty intervals, A4
exactly one open interval per key, A5 no duplicate fact, A6 registry sanity.

**Part 1 — candidate green** (real lake `~/dev/vantage-data/lake-backfill`,
run 2026-07-18, verbatim):

```
[PASS       ] precondition: gold readable, 86615392 active rows
[PASS       ] A1 no-lookahead: rows visible before their filing was accepted: 0
[PASS       ] A2 interval-shape: rows with inverted validity interval: 0
[PASS       ] A2b empty-superseded: empty intervals with no same-instant winning sibling: 0
[PASS       ] A3 no-overlap: adjacent non-empty intervals that overlap within a key: 0
[PASS       ] A4 one-open-interval: keys without exactly one open interval: 0
[PASS       ] A5 no-dup-fact: duplicate (key, valid_from, adsh) rows: 0
[PASS       ] A6 registry-sane: batches=69 distinct_ids=69 distinct_quarters=69
gate_b_audit lake=C:/Users/hossa/dev/vantage-data/lake-backfill outcome=GREEN exit=0
```

Code-facing audit component, same day: `sbt test` → `Total number of tests run:
40 · succeeded 40, failed 0` · exit 0.

**Part 2 — twin red** (`scripts/gate_b_mutate_twin.py` shifted `accepted`
+5 years on one active gold file of a full twin copy, `valid_from` untouched —
lookahead at the as-of seam, the natural target; manifest at
`~/dev/vantage-data/gate-b/mutation-manifest.json`, 429,949 rows mutated).
Audit on the twin, verbatim:

```
[PASS       ] precondition: gold readable, 86615392 active rows
[FAIL       ] A1 no-lookahead: rows visible before their filing was accepted: 429949
[PASS       ] A2 interval-shape: rows with inverted validity interval: 0
[PASS       ] A2b empty-superseded: empty intervals with no same-instant winning sibling: 0
[PASS       ] A3 no-overlap: adjacent non-empty intervals that overlap within a key: 0
[PASS       ] A4 one-open-interval: keys without exactly one open interval: 0
[PASS       ] A5 no-dup-fact: duplicate (key, valid_from, adsh) rows: 0
[PASS       ] A6 registry-sane: batches=69 distinct_ids=69 distinct_quarters=69
gate_b_audit lake=C:/Users/hossa/dev/vantage-data/gate-b/twin-lake outcome=RED exit=1
```

The red count equals the manifest's `rows_mutated` exactly, and every other
check stayed green — the twin differs from the candidate only at the designed
seam.

### Honest history: two defects found while producing this evidence

1. **The audit's first candidate run was RED for a spec error** (A2 flagged
   `valid_to <= valid_from`, 23,315 rows; A3 counted zero-measure intervals as
   overlaps, 5,757). Diagnosis from raw data: 0 strict inversions; all 23,315
   were empty `[T, T)` intervals — the documented representation of a filing
   superseded at its own accepted instant (accepted-tie loser / same-instant
   restatement), invisible to any as-of query, and every one has a same-instant
   winning sibling. The audit was corrected, and the strictness was kept by
   adding A2b (an *orphaned* empty interval is an anomaly) rather than merely
   loosening A2.
2. **The mutation tool initially corrupted more than the designed seam**:
   pyarrow read Spark's int96 timestamps as nanoseconds, and the year-9999 open
   sentinel overflows `timestamp[ns]`, wrecking `valid_to` wholesale (extra reds
   on A2/A4). Fixed with `coerce_int96_timestamp_unit="us"` + UTC-instant tags;
   the twin was rebuilt from a fresh copy and re-mutated, giving the clean red
   above.

## Part 3 — consumer-path probe (machinery proven; real firing pending)

`scripts/gate_b_probe.py` queries gold exactly as a consumer would (as-of
semantics over the Delta snapshot) and passes only if an *independently derived*
expected fact comes back. Rehearsal 2026-07-18 (records at
`~/dev/vantage-data/gate-b/probe-records.json`):

- **Probe vs real gold: PASS** — `as_of(2025-06-01)` for cik 1750
  `AccruedLiabilitiesCurrent` ddate 20250228 → `236700000.0`,
  adsh `0001410578-25-000519`, interval `[2025-03-27 21:40, 9999-12-31)`.
- **Negative control vs effect-absent state (no gold table): FAIL** — as it must.
- `node ~/dev/rigor/scripts/check-effect-probe.mjs` over the records: exit 0,
  `effect-probe: clean` — the probe demonstrably discriminates presence from
  absence; it is not vacuous.

This rehearsal credits the **machinery only**. The publish-credit probe must run
against the *workspace* gold after the bundle job publishes it, with its negative
control captured against the pre-publish workspace state.

## Bundle state (defects fixed this session)

- **Jar (defect 1):** `sbt assembly` rerun 2026-07-18 → `target/scala-2.12/`
  `vantage-assembly-0.1.0.jar` (2026-07-18 18:14; postdates the 2026-07-08 fixes).
- **GoldRebuild task (defect 2):** `databricks.yml` now has `run_gold_rebuild`
  (`pit.gold.GoldRebuild`) strictly after `run_pipeline` on a shared job
  cluster — gold stays a pure once-after rebuild.
- **Env wiring (defect 3):** `spark_env_vars` carries the full PIT_* contract
  (`silver_sub` derives from `PIT_SILVER_ROOT` in code). Config-level only —
  **unprobed until a real run**.
- `databricks bundle validate` parses the bundle (name/target resolve; the only
  error is missing auth). Full validation, deploy, and run are operator-gated.

## Operator runbook — the real part-3 firing

Order matters: the negative control must be captured while the workspace is
still in the effect-absent state.

1. `databricks auth login --host <workspace-url>` (interactive; agent-forbidden).
2. `databricks bundle validate` — must pass cleanly now.
3. **Capture the negative control FIRST** (pre-publish, effect-absent):
   `python scripts/gate_b_probe.py --gold <workspace-gold-path> ... --role control --record ...`
   (must FAIL).
4. `databricks bundle deploy -t dev` then run job `vantage-pipeline`; both tasks
   must succeed (`run_pipeline`, then `run_gold_rebuild` printing
   `gold_rebuild rows=... delta_version=...`).
5. Run the audit against the workspace lake (adapt `--lake` to the workspace
   path or run the checks in a workspace notebook) — green required.
6. **Probe** the workspace gold through the consumer path with expectations
   derived from the LOCAL lake (e.g. the cik-1750 fact above) — must PASS.
7. `node ~/dev/rigor/scripts/check-effect-probe.mjs <records>` → must print
   `effect-probe: clean`. Only then is the publish credited.

## Adversarial verification of this record (2026-07-18)

Five independent skeptics (Workflow fan-out, `scripts/gate_b_refute_workflow.mjs`)
were each prompted to REFUTE one load-bearing claim by recomputing from raw
artifacts. All five returned refuted=false with independent recomputation —
notably: the audit-green skeptic re-ran the audit end-to-end and observed exit 0
live; the twin-red skeptic proved the lookahead violation is confined to exactly
the mutated file (429,949 inside, 0 outside). Caveats each skeptic logged are in
the run record. Stated residual limits: the jar's *contents* were dated by mtime,
not decompiled; the twin was not byte-diffed against the candidate beyond the
seam confinement query.

**ADR-0006 criterion-2 receipts (this run was the candidate test):** tiers were
read from rigor `config/models.json` and passed via `args.tiers`; every
`agent()` call carried an expression pin (`model: tiers[c.tier]`);
`check-tier-placement.mjs` on the script: clean, exit 0. Every worker returned a
`model_self_report` receipt, and **`answered` named the pinned tier's model in
all five cases** — the two judgment workers self-reported `claude-fable-5`, and
all three build/cheap workers self-reported `claude-sonnet-5`, i.e. the build
tier, not the session model. No silent tier collapse.
`check-dispatch.mjs` over the dispatch+receipt log
(`~/dev/vantage-data/gate-b/dispatch-log.json`): `dispatch: clean (10 records)`,
exit 0. (One recording note: the probe-nonvacuous dispatch ran on the `build`
tier; the log records `dispatch_tier: "cheap"` because the lint schema admits
only judgment|cheap and current config maps both to the same model id.)

## Where the machine artifacts live

Outside the repo (data-dir convention, not clonable):
`~/dev/vantage-data/gate-b/` — `audit-candidate.json`, `audit-twin.json`,
`mutation-manifest.json`, `probe-records.json`, `twin-lake/` (disposable, 5.3G).
