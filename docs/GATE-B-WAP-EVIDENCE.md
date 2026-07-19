# Gate B — WAP-shaped publish evidence record

**Dates:** 2026-07-18 (local firing) · 2026-07-19 (workspace publish) · **Code
state:** commits `35e277e`/`deed12f` + the serverless-port work of 07-19.
This is the runnable-artifact record for the first WAP-shaped firing of the
three-part publish-credit rule (rigor ADR-0005 resolution 3 — **Proposed, not
ratified**; this firing exercises the shipped skills `verify-the-effect`,
`data-quality-fail-closed`, `no-lookahead`, and quotes the ADR as direction, not
settled practice). VANTAGE is the origin repo for those skills: this firing earns
**no promotion credit** and does not unlock ADR-0005's bridge doc.

## Credit verdict, fail-closed: **CREDITED 2026-07-19**

| Part | Requirement | Status |
|---|---|---|
| 1 | Audit green on the staged candidate | **[VALIDATED]** — local lake GREEN (86,615,392 rows) 07-18 AND published workspace lake GREEN (1,464,407 rows) 07-19, all 8 checks |
| 2 | Same audit demonstrably red on a mutated twin | **[VALIDATED]** 07-18 — RED with A1 = 429,949, exactly the mutated row count, all else green |
| 3 | Post-publish consumer-path probe + negative control | **[VALIDATED]** 07-19 — probe PASS through the published workspace gold; negative control captured pre-publish FAILED against the effect-absent state; `check-effect-probe.mjs` → `effect-probe: clean`, exit 0 |

An audit never seen red is unevaluable and unevaluable halts — part 2 exists so
this record can never claim credit from a green-only audit.

## The workspace publish (2026-07-19)

The workspace (`dbc-a430129d-60c9`, serverless-only) rejected classic clusters,
which forced a real port (§Serverless port below). The publish run
(`jobs/997431786577151/runs/617741392256248`), verbatim:

```
run batch_id=3adacaf4370d6f96aa63c425fcf05dc0591ac68be92a6fc69d0c7134646732ed rows_in=3690955 rows_out=1464407 scoped_out=2185031 rejected=41517 quarantined=41517 gate=Pass delta_version=0 wall_clock_ms=44977
gold_rebuild rows=1464407 delta_version=0 wall_clock_ms=31872
```

Every count **matches the local 2026q1 backfill row-for-row** (rows_in 3,690,955;
scoped_out 2,185,031; silver/gold 1,464,407; null-value quarantine 41,517) — a
cross-machine reproduction of the pipeline on independent compute.

**Part-3 probe, ordered correctly:** the negative control ran BEFORE the publish
(workspace gold absent: listing error, download failure, probe FAIL recorded);
after the publish, the same probe through the published gold returned the
expectation derived from the LOCAL lake in advance — AAPL (cik 320193) `Assets`
ddate 20251231 as-of 2026-07-01 → `379297000000.0000`, adsh
`0000320193-26-000006`, valid_from `2026-01-30 11:02` — PASS. Records in
`~/dev/vantage-data/gate-b/probe-records-workspace.json`; lint clean.

**Audit on the published lake** (identical `gate_b_audit.py`, downloaded bytes):
all 8 checks PASS over 1,464,407 active rows, registry 1/1/1
(`audit-workspace.json`).

**Lineage cross-check:** workspace vs local 2026q1 registry manifests —
`source_sha256` and `schema_version` IDENTICAL (same SEC bytes), `batch_id`
differs solely via `code_sha` (workspace ran with `VANTAGE_CODE_SHA` unset →
`unknown`), which is content-addressed identity behaving as specified.

Residual gaps, stated: (1) workspace `code_sha` is `unknown` — serverless has no
env vars; pass it as a `KEY=VALUE` arg in a follow-up; (2) the open-interval
sentinel's stored instant differs across environments by the session-zone offset
(local `9999-12-31 05:00Z`-equivalent vs workspace `9999-12-31 00:00-05`) — a
representation difference in the far-future sentinel only, no as-of query before
year 9999 can observe it; (3) the audit ran on downloaded bytes of the published
tables, not in-workspace compute.

## Serverless port (2026-07-19) — what the workspace constraint forced

"Only serverless compute is supported in the workspace" (400 on job create), and
serverless JAR tasks require Scala 2.13 + Databricks Connect (environment v4).
Port, one source tree, both profiles green:

- **Dual build profiles:** 2.12 classic (local dev + the 40-test suite) and 2.13
  Databricks Connect 17.3 `Provided` (the serverless jar). The suite cannot run
  on the connect profile; **2.12 green is the parity gate** for shared code.
- **Deequ removed; native DQ gate.** Deequ is classic-internals-only and
  2.12-only. `DataQualityGate` now evaluates the same contract (completeness,
  6-col natural-key uniqueness, non-negativity) with plain aggregations behind
  an explicit `GateContract`; identical Pass/Fail/Unevaluable fail-closed
  semantics, pinned by the pre-existing gate tests.
- **`io.delta.tables` → SQL (`pit.util.DeltaIO`):** merges as `MERGE INTO
  delta.`path``, versions via `DESCRIBE HISTORY`, existence probe via read
  resolution — one code path for classic and Connect.
- **Config via JAR-task args:** serverless has no env vars;
  `AppConfig.loadFromArgs` turns `PIT_*=value` parameters into system properties
  HOCON resolves like env vars. Entrypoints keep the America/New_York session
  pin via runtime conf (builder-time static confs are rejected on Connect).
- **Bundle:** serverless environment v4 with `java_dependencies` pointing at the
  jar uploaded to `/Volumes/workspace/default/vantage/jars/` (the bundle does
  not rewrite local paths inside environment specs — learned from a live 400 +
  `cp: cannot stat` failure); source data + lake live under the
  `workspace.default.vantage` UC volume (public DBFS root is disabled).
- Suite after the port: **40/40, exit 0** (2026-07-19). 2.13 assembly clean.

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

## Part 3 — consumer-path probe machinery (local rehearsal, 2026-07-18)

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

This rehearsal credited the **machinery only**; the real firing against the
published workspace state (2026-07-19, §above) is what credits the publish. The
three original bundle defects all closed along the way: jar rebuilt (twice —
2.12 on 07-18, then the 2.13 connect jar on 07-19), `run_gold_rebuild` task
added, and config wiring probed live (as JAR-task parameters; the original
`spark_env_vars` design died with classic compute).

## Re-running the publish

```powershell
$env:DATABRICKS_HOST='https://dbc-a430129d-60c9.cloud.databricks.com'
sbt "++2.13.16 assembly"
databricks fs cp --overwrite target/scala-2.13/vantage-assembly-0.1.0.jar dbfs:/Volumes/workspace/default/vantage/jars/vantage-assembly-0.1.0.jar
databricks bundle deploy -t dev --var="source_dir=/Volumes/workspace/default/vantage/source" --var="lake_root=/Volumes/workspace/default/vantage/lake"
databricks bundle run vantage_pipeline -t dev --var="source_dir=..." --var="lake_root=..."
```

Credit discipline for any future publish: negative control against the
pre-publish state FIRST, then run, then audit + probe the published state, then
`check-effect-probe.mjs` must print `effect-probe: clean`.

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
`audit-workspace.json`, `mutation-manifest.json`, `probe-records.json`,
`probe-records-workspace.json`, `dispatch-log.json`, `twin-lake/` (disposable,
5.3G), `workspace-lake/` (downloaded published tables). In the workspace:
`/Volumes/workspace/default/vantage/{source,lake,jars,artifacts}`.
