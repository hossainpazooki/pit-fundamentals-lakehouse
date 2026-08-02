# 0001 -- Airflow orchestration for the local pipeline

Status: Accepted. Evidence: `docs/AIRFLOW-ORCHESTRATION-EVIDENCE.md` (2026-07-22)
— green/red/unevaluable/backfill legs run live against
`vantage-data/scratch-airflow/`, `sbt test` 40/40 unaffected.

## Context

VANTAGE had no orchestration layer before this sprint. `pit.Pipeline` (silver)
and `pit.gold.GoldRebuild` (gold) are standalone `spark-submit` entry points
invoked by hand or from ad hoc scripts, and `scripts/gate_b_audit.py` is a
separate manual step run after the fact. Nothing sequenced silver ->
gold -> audit -> publish, nothing enforced that a red or unevaluable audit
blocks promotion, and repeat quarter runs had no scripted rerun/backfill
path.

Two repo facts constrain any orchestration design:

- **Gold is a pure rebuild, not an incremental append.** `GoldRebuild`'s own
  docstring names the failure mode directly: building gold from a single
  batch's `sub` frame scopes the `adsh` join to that batch and drops every
  other filing's facts from the overwritten table (the gold sub-scoping
  finding, 2026-07-07). Gold must always be rebuilt from the full silver
  table plus the accumulated filing index, never per-quarter.
- **The fat jar excludes Spark.** `build.sbt` marks `spark-core` and
  `spark-sql` `% Provided` (kept off the assembled jar; Spark is supplied by
  whatever runs it), while `delta-spark` ships inside the jar. Whatever
  orchestrates `Pipeline`/`GoldRebuild` must supply a compatible Spark
  runtime itself -- the jar cannot run standalone.

`gate_b_audit.py` already encodes a write-audit-publish (WAP) discipline
with three outcomes -- PASS, FAIL, UNEVALUABLE (exit 0/1/2) -- established
for the Databricks Gate B publish path and documented in
`docs/GATE-B-WAP-EVIDENCE.md`. This ADR extends that same fail-closed
discipline to a local, Airflow-driven promotion path; it does not replace or
re-run Gate B's Databricks publish.

## Decision

Build a single Airflow 3.0.2 DAG (`vantage_pipeline`), LocalExecutor,
under `orchestration/airflow/`, wired as follows.

**Negative control as a first-class DAG leg (added 2026-07-22 after the
verify phase refuted the original design).** The DAG has a
`negative_control` param (none | twin | uneval) driving a task between
`gold_rebuild` and `audit`: `twin` plants the lookahead mutation
(`gate_b_mutate_twin.py`, unmodified, path-guarded to "twin" lakes),
`uneval` hides `gold/_delta_log`. WHY in-run rather than pre-run: gold is a
pure rebuild, so any defect planted before the run is healed by
`gold_rebuild` itself -- confirmed live when the first negative-control
attempt ran the full DAG over a pre-mutated twin and the audit came back
green on the rebuilt gold. The only window a gold-layer defect can survive
to be audited is after the rebuild, which is where the control now runs.
Publish unreachability is therefore provable from single self-contained DAG
runs recorded in the scheduler's own metadata (publish `upstream_failed` on
both red and unevaluable legs).

**Sibling-container `spark-submit` via DockerOperator + host `docker.sock`
mount, not a fat Airflow image with Spark/JDK baked in.** The alternative --
building an Airflow worker image with JDK17 and Spark 3.5 preinstalled -- was
the documented fallback if container-to-container access to the host Docker
daemon didn't work on this machine. It was spiked first: mounting
`/var/run/docker.sock` into a container and driving `docker` against the
host daemon from inside it was confirmed live (`docker:cli` listing the host
container set from inside a sibling container). Given that route works,
building and maintaining a second custom image whose only job is bundling a
JDK+Spark toolchain Airflow itself doesn't need is unjustified weight. Each
`silver_<quarter>` and `gold_rebuild` task instead runs
`apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` as a sibling container,
mounting the repo (for the assembled jar) and `vantage-data` (for the
scratch lake), and invoking `/opt/spark/bin/spark-submit` by its full path
inside that image (it is not on the image's `PATH`).

**`gold_rebuild` is one task, after all silver tasks, never per-quarter.**
Direct consequence of the pure-rebuild fact in Context: a per-quarter gold
task would silently reproduce the 2026-07-07 sub-scoping bug on every
quarter after the first, because each run would overwrite gold scoped only
to that quarter's `sub` frame. Gold reads the whole accumulated silver +
sub store regardless of how many quarters were just loaded, so it is
correct -- and cheaper -- to rebuild it exactly once per DAG run, downstream
of every `silver_<quarter>` task.

**`silver` tasks run sequentially, not in parallel.** `Pipeline.runSilverStage`
MERGEs into a single silver Delta table and a single `SilverSubStore` per
`lake_root`; two concurrent MERGE writers against the same Delta table race
under Delta's optimistic concurrency control, and one will retry or fail.
The repo's existing backfill/registry convention (see
`docs/BACKFILL-METRICS.md`) is itself sequential per quarter, so sequential
silver processing also matches the ledger shape the rest of the codebase
already assumes. The mechanism is a single dynamically-mapped (`.expand()`)
`DockerOperator` task (one map index per entry in the `quarters` param) with
`max_active_tis_per_dagrun=1`, not a literal `silver_<quarter>[i] ->
silver_<quarter>[i+1]` static edge list: Airflow's task graph is fixed at
DAG-parse time, before any trigger-time `params.quarters` override exists,
so a literal per-quarter chain could only ever serve one hardcoded quarter
list and would break the requirement that the same DAG serve the
green/twin/unevaluable legs and an arbitrary-length backfill window from
trigger-time params alone. The mapped-task-with-concurrency-cap-1 shape
gives the identical no-concurrent-writers guarantee for any `quarters`
value, including the single-quarter default and the two-quarter backfill
window, with zero DAG redeploys.

**Audit and publish get zero retries; silver and gold get bounded retries
(2, exponential backoff).** `scripts/gate_b_audit.py` returns one of three
outcomes -- PASS (exit 0), FAIL (exit 1), UNEVALUABLE (exit 2, e.g. no
`gold/_delta_log` yet, per the script's own precondition check) -- and a
result at any of those three is a verdict about the state of the lake, not a
transient fault. Retrying a FAIL or UNEVALUABLE audit doesn't change the
answer; it just hides the first result under a rerun. Silver and gold tasks,
by contrast, run inside a sibling container reached over the Docker Engine
API and a Windows bind mount -- both are real sources of transient failure
(container start races, daemon hiccups) unrelated to the correctness of the
data being written, and gold in particular is a bulk-idempotent rebuild, so
retrying it is safe. `publish` inherits the audit's fail-closed behavior
structurally: it keeps Airflow's default `trigger_rule` (`all_success`) with
no override and no branch operator around it, so it is unreachable whenever
`audit` fails for either reason -- there is no path that routes around a
red or unevaluable audit.

**Local promotion, not Gate B's Databricks publish.** `publish` appends one
JSON record (`published_at_utc`, `dag_run_id`, `lake_root`, `quarters`,
`audit_json_path`, `audit_outcome`, `gold_delta_version`, `code_sha`) to
`{lake_root}/registry/publish-log.jsonl`. This is a local scratch-lake
promotion marker for this orchestration exercise, distinct from and not a
substitute for Gate B's Databricks WAP publish.

## Consequences

- **Local-only claim boundary.** Everything in this DAG operates on
  `vantage-data/scratch-airflow/`, a disposable scratch copy. Passing the
  green/red/unevaluable/backfill legs here demonstrates the orchestration
  and the fail-closed gate; it does not re-validate Gate B's Databricks
  publish path, and the evidence doc for this work must not imply that it
  does.
- **Windows bind-mount performance bounds backfill window size.** Bind
  mounts under Docker Desktop on Windows are slow relative to a native Linux
  mount; the discovery doc calls this out explicitly for the full lake (tens
  of GB). The backfill exercise in this design is deliberately scoped to a
  small quarter window (e.g. `2009q2` + `2009q3`, a few thousand and roughly
  126k source rows respectively) to keep sibling-container runs fast enough
  to iterate on, not because the DAG shape itself has a size limit.
- **Scratch-lake discipline (never touch `lake-backfill/` or `source/`) is
  enforced by convention and documentation, not by a technical guard.**
  `orchestration/airflow/dags/vantage_pipeline.py`'s `lake_root`/`source_dir`
  `Param` defaults point at `scratch-airflow/`, and every builder/evidence
  doc in this sprint names the rule explicitly, but no code in the DAG
  itself refuses a `lake_root` or `source_dir` value that resolves outside
  `scratch-airflow/` -- a misconfigured trigger-time param override could
  still point at the real `vantage-data/lake-backfill` or `vantage-data/source`
  trees. This is a named residual gap, not a claimed guard; a later pass
  could add an explicit path check (e.g. refusing any `lake_root`/
  `source_dir` that doesn't resolve under `scratch-airflow/`) and should
  revise this ADR to say so once it exists.
- **A real (non-local) deployment would need more than this DAG covers**:
  credential-managed Docker access instead of a raw `docker.sock` mount, a
  non-default-credential Airflow web user, a real object store instead of a
  Windows bind mount, and a publish target that is Gate B's actual
  Databricks workspace rather than a local JSONL file. None of that is in
  scope here; this ADR documents a local exercise of the orchestration
  pattern, not a production design.
