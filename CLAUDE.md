# VANTAGE — repo working notes for Claude

A point-in-time-correct SEC-fundamentals lakehouse. Scala 2.12.18 / Spark 3.5.1 /
Delta 3.x. The load-bearing property: `as_of(D)` returns only what was **filed and
accepted on or before D**, including across restatements — no lookahead. See
`README.md` for the full framing and the claim ladder (respect it; don't upgrade
claims past what the tests prove).

## Read these first (session records — LOCAL ONLY)

The audit trail for how this repo got built lives **gitignored** under
`docs/superpowers/plans/` — it is NOT in a fresh clone. On this machine, start with:

- `docs/superpowers/plans/2026-07-09-HANDOFF-session-logging.md` — where every piece
  of the build session is logged, and how to re-verify each claim.
- `docs/superpowers/plans/2026-07-07-SESSION-STATE.md` — the substantive record of the
  seam-test + full-history-backfill session (2026-07-07→08).
- `docs/superpowers/plans/2026-07-0*-FINDING-*.md` + `NEGATIVE-CONTROL-RECORD.md` — the
  two real defects caught and fixed, and the non-vacuity evidence.

If those files are absent, you're on a clone that never had them — regenerate context
from git history + `docs/BACKFILL-METRICS.md` (which *is* tracked), not from assuming
they were lost.

## Data lives OUTSIDE the repo

Lake, source TSVs, and the backfill run ledger are at `~/dev/vantage-data/` — not
clonable, not committed. `lake-backfill/` is the full-history lake;
`backfill-ledger.jsonl` is the per-quarter run log (ISO-8601 `ts`, code SHA, wall-clock).

## Build & test

Requires **JDK 17** (Temurin 17; the system JDK is Spark-3.5-incompatible) and Spark's
Hadoop runtime. On Windows, `winutils.exe` + `hadoop.dll` must be on `HADOOP_HOME`.

- **Windows (PowerShell)** — env is per-invocation, so prefix every sbt call:
  ```powershell
  $env:HADOOP_HOME='C:\Users\hossa\hadoop'; $env:PATH="$env:HADOOP_HOME\bin;$env:PATH"; sbt test
  ```
  (sbt is at `AppData/Local/Coursier/data/bin/sbt.bat`. Run sbt via PowerShell, not Git
  Bash — it isn't on the Bash PATH.)
- **Linux/web** — `sbt test` directly; set `HADOOP_HOME` only if the runtime lacks it.

`sbt test` runs the full property suite; `sbt assembly` builds the fat jar. CI (GitHub
Actions) runs suite + scalafmt/scalafix + assembly on every push — that gate is the
final word, never an assumption. Exact test count lives in `README.md` Status.

## Running the pipeline

Two steps by design (gold is a pure rebuild, done once — never per quarter):
1. `pit.Pipeline` — ingests quarters through the **silver stage** (bronze, registry, DQ
   gate, silver + `SilverSubStore` merges).
2. `pit.gold.GoldRebuild` — rebuilds gold from the whole lake, once.

Config is env-driven (`PIT_SOURCE_DIR`, `PIT_*_ROOT`, `PIT_REGISTRY_PATH`,
`PIT_QUARTERS` as a comma list — see `README.md` Run and `application.conf`). Session
timezone is pinned to `America/New_York` in the entrypoint; do not remove it — SEC
`accepted` timestamps are zoneless US-Eastern and local-zone parsing moves the PIT
boundary across machines.

`scripts/recompute_metrics.py` recomputes every backfill metric from artifacts and
cross-checks against the logs (exit 0 = match). It's the tool to re-derive any number in
`docs/BACKFILL-METRICS.md` — don't hand-recount.

## Hard rules

- **Git-guard: never write history.** Emit commit commands for the operator to run
  (grouped by repo, Git Bash / POSIX paths); do not `git commit`/`push`. `databricks.yml`
  carries a pre-session uncommitted modification left to the operator — don't fold it in.
- **Gate-green ≠ data-correct.** Verify effects by probing gold tables / registry rows,
  never an exit log.
- **Uniqueness is measured, not assumed.** A natural-key collision is a finding to surface
  and gate (the DQ gate refuses the quarter whole), never silently deduped. Five 2011–2012
  quarters are refused for exactly this — see `BACKFILL-METRICS.md`.
- **Banned vocabulary** in outward docs: "scale", "at scale", "high-throughput", "TB". The
  §10 "named at 100×, not built" maintenance-ops note stays restated, not softened.

## Next: Gate B (Databricks) — not started

`databricks.yml` is a configured-not-deployed Asset Bundle. Three known defects to fix
*during* deploy where they can be probed: the referenced assembly jar is stale (predates
the 2026-07-08 fixes — rebuild it); the job has one task and never runs `GoldRebuild` (so
no gold is built); the `spark_env_vars` wiring is unprobed. Needs workspace auth
(operator-only). This is the only open workstream; the local build session is complete
and fully logged.
