# Interview notes -- Airflow orchestration (WS1)

## 30-second version

I put an Airflow DAG in front of the existing SEC-fundamentals pipeline:
wait for source quarters, load each into silver sequentially, rebuild gold
once from the whole accumulated store, run the existing
data-quality audit, and only publish if that audit comes back clean. Spark
runs in sibling Docker containers the DAG launches on the host daemon, not
inside the Airflow worker itself. A red or unevaluable audit structurally
blocks publish -- there's no branch that routes around it.

## 90-second version

VANTAGE's pipeline was already two Spark jobs -- a silver-stage loader and a
gold rebuild -- plus a separate audit script, all run by hand. The DAG
(`vantage_pipeline`, one DAG, three Airflow params: `quarters`, `lake_root`,
`source_dir`) sequences them: a deferrable sensor per quarter waits for its
source files, then `silver_<quarter>` tasks run one at a time -- sequential
on purpose, because silver MERGEs into a single Delta table and concurrent
writers would race under Delta's optimistic concurrency. After every quarter
lands, one `gold_rebuild` task runs -- also on purpose singular, because
gold is documented as a pure rebuild of the whole accumulated filing index;
doing it per-quarter reproduces a real bug this repo already found and fixed
(scoping the join to one batch's `sub` frame silently drops every other
filing's facts). Then `audit` runs the repo's existing three-outcome data
quality check (pass/fail/unevaluable), and `publish` -- which only fires on
Airflow's default all-success trigger rule, no override -- appends a record
to a local promotion log if and only if the audit was clean. The Spark
tasks themselves run as sibling containers: the Airflow worker mounts the
host's Docker socket and launches a Spark 3.5/JDK17 container per task,
because the pipeline's fat jar deliberately excludes Spark from the build
(it's marked `Provided` and supplied at runtime), and the host's own Java is
too new for Spark 3.5 to run in-process.

## Hardest questions

**Q: Why not run Spark inside the Airflow worker instead of spinning up a
sibling container for every task?**

Two independent reasons rule that out, both visible in the repo. First,
`build.sbt` marks `spark-core` and `spark-sql` `% Provided` -- the assembled
fat jar deliberately does not bundle Spark, so whatever runs it has to
supply a Spark runtime itself; there's nothing to `spark-submit` without
one. Second, this machine's system JDK (21) is incompatible with Spark 3.5,
which needs JDK17 -- running Spark in-process inside the Airflow worker
would mean building and maintaining a second custom image just to carry a
pinned JDK17 + Spark 3.5 toolchain the rest of Airflow doesn't need. Since
mounting the host's Docker socket into the worker and driving sibling
containers on the host daemon was confirmed to work directly (listing the
host's live containers from inside a mounted container), that route avoids
building the second image entirely: each Spark task launches
`apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` fresh, with the correct
JDK and Spark version pinned by the image tag, and throws it away when the
task finishes.

**Q: What happens if the audit comes back unevaluable instead of red --
does publish still block?**

Yes, identically. `scripts/gate_b_audit.py` has three outcomes, not two:
PASS (exit 0), FAIL (exit 1), and UNEVALUABLE (exit 2) -- for example, its
very first precondition check fails UNEVALUABLE if there's no
`gold/_delta_log` yet to audit at all, before it evaluates a single data
quality check. The DAG doesn't special-case exit 2 into a softer outcome:
any nonzero exit fails the `audit` task the same way, so from Airflow's
point of view FAIL and UNEVALUABLE are indistinguishable failures. Since
`publish` keeps Airflow's default `all_success` trigger rule with no
override, it's unreachable whenever `audit` fails for either reason --
"I couldn't tell" is treated exactly as fail-closed as "I checked and it's
wrong," which matters because coercing an unevaluable result into a pass
is exactly the failure mode a fail-closed gate exists to prevent.

**Q: Your negative control mutates the lake mid-run. Why not just point the
DAG at a pre-corrupted twin and watch it go red?**

Because that experiment was actually run, and the DAG passed it green --
which is the interesting part. Gold in this lakehouse is a pure rebuild:
`gold_rebuild` reconstructs the entire gold table from silver plus the
accumulated filing index on every run. Point the DAG at a twin whose GOLD
was corrupted, and the rebuild replaces the corrupted gold with a fresh one
derived from the clean silver before the audit ever looks at it -- the
pipeline heals the damage, the audit is honestly green, and publish
legitimately runs. That is a property of the architecture, not a hole in
the test. The consequence is that a real negative control must plant the
defect in the only window where it can survive to be audited: after the
rebuild, before the audit. So the DAG carries a `negative_control`
parameter -- a task between `gold_rebuild` and `audit` that either runs the
repo's twin mutator (lookahead planted in gold; audit fails with the A1
count equal to the planted row count exactly) or hides the gold Delta log
(audit unevaluable, exit 2). Both legs run as ordinary full DAG runs, so
the scheduler's own metadata records publish as upstream_failed -- the
unreachability claim comes from the metadata DB, not from a screenshot.
