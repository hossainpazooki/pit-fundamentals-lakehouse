# WS1 -- Airflow orchestration evidence record

**Date:** 2026-07-22. **Scope:** local Airflow 3.0.2 orchestration
(orchestration/airflow/) of the existing pit.Pipeline / pit.gold.GoldRebuild
/ scripts/gate_b_audit.py pipeline over the scratch lake
vantage-data/scratch-airflow/. Evidence-doc house style follows
docs/GATE-B-WAP-EVIDENCE.md -- verbatim command blocks, verbatim tool output,
an explicit verdict table, and an honest history of what broke and was fixed
while producing this record (not a polished retelling).

This is a LOCAL scratch-lake exercise, not a re-run of Gate B's Databricks
publish. See the "What this does NOT prove" section at the end.

## Verdict table

| Gate | Requirement | Status |
|---|---|---|
| 1 | docker compose up -d healthy, CLI reachable | [VALIDATED] -- all 5 services healthy, `airflow version` returns 3.0.2, DAG imports with zero errors |
| 2 | GREEN leg: full DAG run, audit GREEN, publish writes promotion record | [VALIDATED] -- original run F92HtpcV (8 tasks); re-validated post-correction on the negative_control-aware DAG: run 5Td7r5N3, 12/12 task instances success (see the CORRECTED section) |
| 3a | Negative control (twin, mutated): audit FAILS red | [VALIDATED -- CORRECTED 2026-07-22] full DAG run fAVUCfgs: in-run mutation planted 10,217 rows, audit outcome=RED exit=1 with A1 == 10,217 exactly; the original isolated-task evidence was REFUTED by the verify phase (see CORRECTED section) |
| 3b | Unevaluable leg: audit exits 2, publish unreachable | [VALIDATED -- CORRECTED 2026-07-22] full DAG run Xb18mg9K: audit outcome=UNEVALUABLE exit=2, publish upstream_failed in the metadata DB |
| 3c | Full DAG run against the twin: publish never runs on a failed audit | [VALIDATED -- CORRECTED 2026-07-22] single self-contained runs now prove it: publish_task = upstream_failed in the metadata DB on BOTH red (fAVUCfgs) and unevaluable (Xb18mg9K) legs; the earlier "two combined pieces of evidence" construction is superseded |
| 4 | Backfill/idempotency: same window rerun into the same scratch lake | [VALIDATED] -- registry/bronze/silver/gold counts identical or reconciled across the two runs; quarantine growth is a named residual (not exactly explained) |
| 5 | sbt test still green (40/40, no weakened guarantees) | [VALIDATED] -- SBT_EXIT_CODE=0, "Tests: succeeded 40, failed 0, canceled 0, ignored 0, pending 0", "All tests passed." |

## Gate 1 -- stack up, healthy, CLI reachable

    cd orchestration/airflow
    docker compose up -d

All five services reached "healthy" (postgres, airflow-apiserver,
airflow-scheduler, airflow-dag-processor, airflow-triggerer):

    vantage-airflow-airflow-apiserver-1 healthy
    vantage-airflow-airflow-dag-processor-1 healthy
    vantage-airflow-airflow-scheduler-1 healthy
    vantage-airflow-airflow-triggerer-1 healthy
    vantage-airflow-postgres-1 healthy

    $ docker compose exec airflow-scheduler airflow version
    3.0.2
    $ docker compose exec airflow-scheduler airflow dags list-import-errors
    No data found
    $ docker compose exec airflow-scheduler airflow dags list
    dag_id           | fileloc                               | owners      | is_paused
    vantage_pipeline | /opt/airflow/dags/vantage_pipeline.py | vantage-ws1 | True

## Gate 2 -- GREEN leg

Bounded backfill window per the WS1 contract: quarters=["2009q2","2009q3"]
(3,999 and 126,650 source data rows respectively). 2009q3 was copied into
vantage-data/scratch-airflow/source/2009q3/ for this exercise (only 2009q2
pre-existed there).

    $ docker compose exec airflow-scheduler airflow dags trigger vantage_pipeline \
        -c '{"quarters": ["2009q2","2009q3"], "lake_root": "/vantage-data/scratch-airflow/lake", "source_dir": "/vantage-data/scratch-airflow/source"}'

Run manual__2026-07-22T19:15:27.096462+00:00_F92HtpcV, task states
(airflow tasks states-for-dag-run vantage_pipeline <run_id>):

    gold_rebuild          success  19:16:20.950209 -> 19:16:39.367923
    audit                 success  19:16:39.990172 -> 19:16:41.253744
    publish_task          success  19:16:42.337681 -> 19:16:42.494208
    source_filepaths       success
    silver_environments     success
    wait_for_source (x4)    success  (map indices 0-3)
    silver (x2)             success  (map indices 0-1)

Audit's own console output (captured in the task log):

    [PASS       ] precondition: gold readable, 113228 active rows
    [PASS       ] A1 no-lookahead: rows visible before their filing was accepted: 0
    [PASS       ] A2 interval-shape: rows with inverted validity interval: 0
    [PASS       ] A2b empty-superseded: empty intervals with no same-instant winning sibling: 0
    [PASS       ] A3 no-overlap: adjacent non-empty intervals that overlap within a key: 0
    [PASS       ] A4 one-open-interval: keys without exactly one open interval: 0
    [PASS       ] A5 no-dup-fact: duplicate (key, valid_from, adsh) rows: 0
    [PASS       ] A6 registry-sane: batches=2 distinct_ids=2 distinct_quarters=2
    gate_b_audit lake=/vantage-data/scratch-airflow/lake outcome=GREEN exit=0

publish-log.jsonl entry (verbatim):

    {"published_at_utc": "2026-07-22T19:16:42.482373+00:00", "dag_run_id": "manual__2026-07-22T19:15:27.096462+00:00_F92HtpcV", "lake_root": "/vantage-data/scratch-airflow/lake", "quarters": ["2009q2", "2009q3"], "audit_json_path": "/vantage-data/scratch-airflow/lake/registry/audit-manual__2026-07-22T19:15:27.096462+00:00_F92HtpcV.json", "audit_outcome": "GREEN", "gold_delta_version": 0, "code_sha": "unknown"}

code_sha: unknown because VANTAGE_CODE_SHA was not set for this exercise --
matches the residual-gap convention already recorded in
docs/GATE-B-WAP-EVIDENCE.md.

## Gate 3 -- negative control (the WAP proof)

### Building the twin

Per the WS1 contract's build order: run the GREEN leg to completion (done
above), copy lake/ -> twin-lake/ wholesale, then run
scripts/gate_b_mutate_twin.py against the copy from the HOST (PowerShell/
Git Bash, pyarrow available -- pyarrow.__version__ == 24.0.0 on this
machine):

    $ cp -r lake/* twin-lake/
    $ python scripts/gate_b_mutate_twin.py --twin-lake /c/Users/hossa/dev/vantage-data/scratch-airflow/twin-lake \
        --manifest /c/Users/hossa/dev/vantage-data/scratch-airflow/twin-lake/mutation-manifest.json
    {
      "mutated_at_utc": "2026-07-22T19:18:27.055497+00:00",
      "twin_lake": "C:/Users/hossa/dev/vantage-data/scratch-airflow/twin-lake",
      "file": "part-00004-be664c09-5aad-4086-8eb7-2faeddcc741a-c000.snappy.parquet",
      "rows_mutated": 11690,
      "mutation": "accepted += 5 years; valid_from untouched -> valid_from < accepted (lookahead) on every row of this file",
      "accepted_sample_before": "2009-08-07 13:27:00+00:00",
      "accepted_sample_after": "2014-08-06 13:27:00+00:00",
      "expected_audit_outcome": "A1 no-lookahead FAIL with count == 11690"
    }

Honest history -- a real defect found here, not in gate_b_mutate_twin.py
itself (never modified): the mutation script rewrites the target parquet
file (via pyarrow.parquet.write_table) and patches the Delta commit JSON's
add.size field (via a plain text write) directly, bypassing Hadoop's
checksummed local filesystem writer. That writer normally keeps a .crc
sidecar file in sync with every write it performs; a hand-write from outside
that path leaves the OLD .crc (computed from the pre-mutation bytes) stale,
and the next Spark read throws ChecksumException. Confirmed live:
gold_rebuild against the twin failed with "Checksum error: file:/vantage-data/scratch-airflow/twin-lake/gold/_delta_log/00000000000000000000.json at 0 exp: -1737625499 got: -1799528601".
Fixed by deleting the two stale sidecars after mutation (host-side, not a
script change):

    $ rm -f twin-lake/gold/.part-00004-be664c09-5aad-4086-8eb7-2faeddcc741a-c000.snappy.parquet.crc \
            twin-lake/gold/_delta_log/.00000000000000000000.json.crc

### The true negative control: audit alone against the frozen, mutated twin

Structural finding, stated plainly: the WS1 contract's instruction to "run
the SAME DAG pointed at [the twin]" cannot be taken literally without
defeating the whole negative control. gold_rebuild is, by design (ADR-0001,
Builder B's task doc string, GoldRebuild.scala's own docstring), an
UNCONDITIONAL pure rebuild of gold from silver + the accumulated registry on
every DAG run -- there is no DAG parameter that skips it. gate_b_mutate_twin.py
corrupts data at the GOLD layer only (never silver). A full DAG run against
the twin therefore silently REBUILDS gold from the twin's (unmutated) silver
before audit ever runs, erasing the injected defect. This was confirmed
live (see the self-healing run below) -- it is not a hypothetical.

Given that, the true negative-control evidence is the audit task run in
isolation against the frozen, mutated twin (airflow tasks test -- the same
route Builder B's own module docstring names as the audit invocation path,
and the same gate_b_audit.py script/CLI the DAG's audit task runs verbatim):

    $ docker compose exec airflow-scheduler airflow tasks test vantage_pipeline audit \
        -t '{"lake_root": "/vantage-data/scratch-airflow/twin-lake", "quarters": ["2009q2","2009q3"], "source_dir": "/vantage-data/scratch-airflow/source"}'
    ...
    [PASS       ] precondition: gold readable, 113228 active rows
    [FAIL       ] A1 no-lookahead: rows visible before their filing was accepted: 11690
    [PASS       ] A2 interval-shape: rows with inverted validity interval: 0
    [PASS       ] A2b empty-superseded: empty intervals with no same-instant winning sibling: 0
    [PASS       ] A3 no-overlap: adjacent non-empty intervals that overlap within a key: 0
    [PASS       ] A4 one-open-interval: keys without exactly one open interval: 0
    [PASS       ] A5 no-dup-fact: duplicate (key, valid_from, adsh) rows: 0
    [PASS       ] A6 registry-sane: batches=2 distinct_ids=2 distinct_quarters=2
    gate_b_audit lake=/vantage-data/scratch-airflow/twin-lake outcome=RED exit=1
    ...
    AirflowException: Bash command failed. The command returned a non-zero exit code 1.

A1's fail count (11,690) exactly equals the mutation manifest's
rows_mutated (11,690) -- the audit is demonstrably catching the exact
injected defect, not an unrelated one.

### Publish never runs on a failed audit -- evidence

Two pieces of evidence combine for this, stated separately and honestly
rather than fabricated as one clean run:

1. The isolated audit-red result above proves gate_b_audit.py itself
   returns exit 1 (task-failing) against the twin.
2. A real DagRun of this exact DAG object, earlier in this same session
   (debugging an unrelated infra bug, before the fixes below landed),
   empirically shows the structural behavior: when audit's task instance
   is failed, publish_task's task instance is upstream_failed -- never
   running, never success. Verbatim (airflow tasks states-for-dag-run,
   run manual__2026-07-22T19:04:58.139827+00:00_Tbrt5hHK):

       audit               failed          19:06:12.753677 -> 19:06:14.097925
       publish_task        upstream_failed 19:06:14.802492 -> 19:06:14.802492

   publish_task's default trigger_rule (all_success, no override, no
   branch operator) is exactly why: Airflow marks it upstream_failed
   automatically whenever its sole upstream (audit) does not succeed. This
   is a property of the DAG's static structure, independent of why audit
   failed -- the same mechanism blocks publish whether the cause is a real
   RED/UNEVALUABLE audit verdict or (as it was in this particular captured
   run) an infrastructure bug being debugged. Combined with piece 1 (the
   real audit verdict against the twin), this demonstrates the intended
   fail-closed WAP behavior.

### Self-healing finding (honest, not swept aside)

To confirm the structural claim above rather than merely assert it, the full
DAG WAS also run against the twin end-to-end
(manual__2026-07-22T19:20:48.573867+00:00_McujAkRr, same quarters/source_dir).
Result: silver (x2) success, gold_rebuild success (after one transient
retry -- see honest history below, unrelated to the twin), audit SUCCESS
(GREEN -- because gold_rebuild rebuilt gold from the twin's unmutated
silver, erasing the injected defect before audit read it), publish_task
SUCCESS. This is the concrete, live confirmation of the "gold_rebuild is
unconditional" structural finding above -- not a failure of WS1, but a real
property of this DAG's design worth naming plainly: a full-DAG rerun against
a gold-only-mutated twin self-heals and cannot be used as the negative
control; the isolated audit-task-against-the-frozen-twin route above is the
one that actually proves the fail-closed gate.

(Honest history, unrelated to the self-heal finding: this same twin run's
gold_rebuild failed once with the stale-.crc ChecksumException above, before
the sidecar cleanup was applied -- retried and succeeded on the second
attempt, consistent with gold_rebuild's retries=2 being appropriate for this
bulk-idempotent rebuild task.)

### Unevaluable leg

Fixture: a copy of the GREEN leg's bronze/, silver/, silver_sub/,
registry/, quarantine/ WITHOUT gold/ at all (uneval-lake/), matching
gate_b_audit.py's own documented first precondition
(if not (gold / "_delta_log").exists(): yield UNEVALUABLE, lines 59-60).
Newly built for this exercise -- no such fixture pre-existed in the repo.

    $ docker compose exec airflow-scheduler airflow tasks test vantage_pipeline audit \
        -t '{"lake_root": "/vantage-data/scratch-airflow/uneval-lake", "quarters": ["2009q2","2009q3"], "source_dir": "/vantage-data/scratch-airflow/source"}'
    ...
    [UNEVALUABLE] precondition: no Delta table at /vantage-data/scratch-airflow/uneval-lake/gold
    gate_b_audit lake=/vantage-data/scratch-airflow/uneval-lake outcome=UNEVALUABLE (halt -- not coerced into pass or fail) exit=2
    ...
    AirflowException: Bash command failed. The command returned a non-zero exit code 2.

Exit 2, task FAILED, same all_success trigger-rule reasoning as the RED
leg makes publish structurally unreachable. BashOperator's default
behavior (any nonzero exit fails the task) already treats exit 1 and exit 2
identically -- no special-casing was added or needed.

## Gate 4 -- backfill / idempotency

The SAME window (quarters=["2009q2","2009q3"]) was run through the DAG a
SECOND time into the SAME scratch lake (lake/, no wipe between runs) --
run manual__2026-07-22T19:28:42.893679+00:00_mzRaMHTO, all 8 tasks
succeeded (including audit GREEN and publish).

scripts/idempotency_diff.py's CLI does not fit this window as-is: its own
docstring and --pre-silver/rerun-commit-lookup logic assume rerunning
exactly ONE quarter (it looks up the single Delta commit file at
pre_silver_version + 1 for the rerun's MERGE metrics) -- this exercise
reran TWO quarters in one DAG run, which would advance the silver version by
2, not 1, and the exact-quarantine-growth invariant that script checks is
scoped to a single-quarter rerun. Per the WS1 contract's own fallback
instruction, the simpler check named in the discovery doc was used instead:
identical registry/batch counts, checked directly via duckdb
(INSTALL delta; LOAD delta; + delta_scan(...)), before vs. after the
second run.

| Metric | Pre-rerun | Post-rerun | |
|---|---|---|---|
| bronze/num row count | 130,649 | 130,649 | unchanged |
| registry batch count | 2 | 2 | unchanged (MERGE update, not insert) |
| silver row count | 113,228 | 113,228 | unchanged |
| gold row count | 113,228 | 113,228 | unchanged (pure rebuild reconciles) |
| silver Delta version | 2 | 4 | advanced (new MERGE commits written even though content is unchanged -- expected) |
| gold Delta version | 0 | 1 | advanced (rebuild always writes a new version) |
| quarantine/rows count | 65 | 121 | grew by 56, not doubled to 130 -- see residual below |

The load-bearing invariants the contract asks for -- registry/bronze/silver/
gold counts identical or reconciled -- all hold. Residual, stated honestly
rather than papered over: quarantine/rows is documented (in
idempotency_diff.py's own docstring) as an append-only audit lane that
should grow by exactly the rerun's quarantined-row count on a clean rerun.
A GROUP BY _batch_id, _ingest_ts breakdown of the post-rerun table shows
THREE distinct _ingest_ts waves for the 2009q2 batch (9+9+9=27 rows) and
TWO for 2009q3 (47+47=94 rows), rather than the expected two-and-two. The
extra 2009q2 append is most likely attributable to an ad hoc docker run
probe issued directly against the real lake/ path earlier in this session
(while diagnosing the Hadoop umask-mode permission fix, before the lake
was wiped for the official GREEN leg) -- this was NOT root-caused with
certainty, and is named here as an open, non-blocking observation rather
than asserted as understood. The invariants WS1 actually requires (registry/
bronze/silver/gold identical-or-reconciled) are unaffected either way.

## Gate 5 -- existing suite still green

    $ powershell -NoProfile -Command "$env:HADOOP_HOME='C:\Users\hossa\hadoop'; $env:PATH=\"$env:HADOOP_HOME\bin;$env:PATH\"; $env:JAVA_HOME=(cs java-home --jvm temurin:17); cd C:\Users\hossa\dev\vantage; sbt test; Write-Output \"SBT_EXIT_CODE=$LASTEXITCODE\""
    ...
    [info] Total number of tests run: 40
    [info] Suites: completed 13, aborted 0
    [info] Tests: succeeded 40, failed 0, canceled 0, ignored 0, pending 0
    [info] All tests passed.
    [success] Total time: 272 s (04:32), completed Jul 22, 2026, 3:47:36 PM
    SBT_EXIT_CODE=0

No Scala source, gate_b_*.py, dqx_*.py, or test file was modified during
this integration pass -- the suite's green state was never at risk of being
"weakened," only re-confirmed.

## Cross-file fixes made during integration (all in orchestration/airflow/, never Scala/tests)

All of the following were infrastructure/wiring bugs surfaced by actually
running the stack, not design changes to what the builders specified:

1. docker-compose.yaml: missing shared api_auth.jwt_secret / core.secret_key.
   Airflow 3.x's execution API authenticates every task-instance state
   change (e.g. marking a task running) with a JWT. Without an explicit
   shared secret, each service process (scheduler, apiserver) generated its
   OWN random secret at startup -- confirmed live (airflow config
   get-value api_auth jwt_secret differed between the two containers) --
   so every task failed instantly with a 403 before running any code.
   Fixed by pinning both values in docker-compose.yaml's airflow-common-env
   (dev-only values, documented inline to rotate for any non-local use).
2. docker-compose.yaml (airflow-init): no default fs_default Connection.
   Airflow 3.0.2 seeds zero default connections; the DAG's wait_for_source
   sensor uses fs_conn_id="fs_default", which does not exist out of the box
   (AirflowNotFoundException). Fixed by having airflow-init create it
   idempotently on every docker compose up.
3. dags/vantage_pipeline.py (gold_rebuild): missing PIT_SOURCE_DIR.
   pit.config.AppConfig.PathsCfg.sourceDir is a required (non-Optional)
   String even though GoldRebuild.scala never reads it -- config parsing
   failed (Key not found: 'source-dir') without it. Fixed by adding it to
   gold_rebuild's environment dict (the contract's spike note said only
   PIT_QUARTERS should be dropped for gold_rebuild; this was missed).
4. dags/vantage_pipeline.py (silver/gold_rebuild write permissions).
   The Spark sibling container's default user (uid 185 "spark") writes
   every lake directory/file at mode 0755/0644, which the Airflow worker
   (uid 50000, gid 0) cannot then add sibling files into (confirmed as a
   PermissionError from audit's --json write). Matching the containers'
   uid/gid was tried and reverted -- it breaks Spark's own Hadoop
   UserGroupInformation login (KerberosAuthException, the image has no
   /etc/passwd entry for an arbitrary uid) even with HOME/-Duser.name
   workarounds, since UnixLoginModule resolves the OS user via a native
   call, not JVM properties. A shell-level umask 000 also does NOT work --
   Hadoop's RawLocalFileSystem applies permissions via its OWN
   fs.permissions.umask-mode config (default 022), independent of the
   process umask (confirmed live: directories stayed 0755 under a
   bash -c 'umask 000 && ...' wrapper). The fix that actually works:
   --conf spark.hadoop.fs.permissions.umask-mode=000 on both spark-submit
   invocations, confirmed live to leave every Spark-written dir/file
   world-writable (0777/0666).
5. dags/vantage_pipeline.py (gold_rebuild XCom / gold_delta_version).
   DockerOperator's default XCom push is the container's LAST stdout line;
   Spark's shutdown logging (SparkContext is stopping, ShutdownHookManager,
   ...) interleaves into the same captured stream AFTER GoldRebuild.scala's
   own summary println, so the true last line is shutdown noise, not the
   delta_version=... line -- confirmed live as "gold_delta_version": null
   in an otherwise fully GREEN publish record. Fixed with xcom_all=True
   plus a publish_task change to search every captured line, not assume
   the last one.
6. docs/adr/0001-airflow-orchestration.md: drift from the actual
   implementation. The Decision section originally described a literal
   silver[i] -> silver[i+1] static task chain; the DAG as actually built
   (Builder B's documented deviation) is a single dynamically-mapped
   DockerOperator with max_active_tis_per_dagrun=1 (Airflow's task graph
   is fixed at parse time, before any trigger-time quarters override
   exists, so a literal per-quarter chain can't serve an arbitrary-length
   quarters param). Corrected to describe the actual mechanism and why.
   Also updated the "stub" language in the Consequences section, stale
   since Builder B's DAG file is now fully built.

## .gitignore

orchestration/airflow/logs/, orchestration/airflow/plugins/,
orchestration/airflow/config/, and orchestration/airflow/.env were already
added (Integration did not need to add more). Checked, not assumed:
vantage-data/ (and therefore scratch-airflow/ within it) lives inside a
much larger git repository rooted at C:\Users\hossa\dev (confirmed via
git rev-parse --show-toplevel from inside vantage-data), which has no
.gitignore of its own and shows vantage-data/ as wholesale untracked
(?? vantage-data/) -- this predates WS1 (visible in this session's
starting git status) and is out of scope for this sprint; no gitignore
change was needed there.

## What this does NOT prove

- This is a LOCAL scratch-lake exercise (vantage-data/scratch-airflow/),
  not a re-run of Gate B's Databricks publish (docs/GATE-B-WAP-EVIDENCE.md)
  -- the two are independent WAP-shaped exercises over different compute
  and different promotion targets (a local JSONL file here, the actual
  Databricks workspace there).
- (SUPERSEDED 2026-07-22 -- kept for honest history) The negative-control
  claim was originally proven by combining an isolated audit-red result
  with a structural observation, because a pre-run gold mutation is healed
  by gold_rebuild's pure rebuild. The verify phase refuted that
  construction (isolated `airflow tasks test` runs persist no task states,
  so structural unreachability was never demonstrated). The corrected
  design plants the mutation IN-RUN, between gold_rebuild and audit, via
  the DAG's `negative_control` param -- see the CORRECTED section at the
  end: publish unreachability is now shown by single self-contained DAG
  runs recorded in the metadata DB.
- The exact magnitude of quarantine/rows growth in the backfill exercise
  (Gate 4) was not root-caused with certainty (see the residual note there).
- code_sha in every publish record is "unknown" -- VANTAGE_CODE_SHA was not
  set for this exercise, matching the residual-gap convention already named
  in docs/GATE-B-WAP-EVIDENCE.md.
- Scratch-lake discipline (never touch lake-backfill/source/) is enforced
  by convention/documentation only, not by a technical guard in the DAG
  itself (named as a residual gap in ADR-0001's Consequences).

## Final independent re-verification (not trusting the DAG's own task-success color)

Per the WS1 contract's closing instruction, gate_b_audit.py was re-run
directly (plain python, no Airflow involved) against the actual, final
on-disk state of all three scratch lakes, immediately before closing this
record:

    $ python scripts/gate_b_audit.py --lake .../scratch-airflow/lake
    ... all 8 checks PASS ...
    gate_b_audit lake=.../lake outcome=GREEN exit=0

    $ python scripts/gate_b_audit.py --lake .../scratch-airflow/twin-lake
    [FAIL       ] A1 no-lookahead: rows visible before their filing was accepted: 11164
    ... rest PASS ...
    gate_b_audit lake=.../twin-lake outcome=RED exit=1

    $ python scripts/gate_b_audit.py --lake .../scratch-airflow/uneval-lake
    [UNEVALUABLE] precondition: no Delta table at .../uneval-lake/gold
    gate_b_audit lake=.../uneval-lake outcome=UNEVALUABLE exit=2

Note: twin-lake was rebuilt fresh for this final check (copy lake/ ->
twin-lake/ + re-run gate_b_mutate_twin.py) because the earlier twin (11,690
rows mutated) had already been self-healed by the full-DAG run documented
in the "Self-healing finding" section above -- its on-disk state at the time
of this final check would otherwise have read GREEN, which would correctly
reflect ITS current state but would be confusing next to this record's
earlier RED evidence. The fresh twin mutated a different file (11,164 rows,
same mutation logic) purely because gate_b_mutate_twin.py picks the
currently-smallest active gold file, which differs slightly between two
independently-rebuilt gold tables -- not a discrepancy in the mutation
mechanism itself. All three exit codes (0/1/2) are confirmed correct on the
actual, final lake states as of this record's close.

## Gate 3 -- CORRECTED (2026-07-22, after the verify phase refuted the original)

Refutation, stated plainly (rigor: a correction is a dated addition, not a
silent rewrite). The fanout's verify phase (skeptic run against the live
Airflow metadata DB) refuted the original Gate 3 claim: the only full
twin-lake DAG run on record had audit GREEN and publish SUCCESS (the DAG
self-healed the pre-run gold mutation via the pure rebuild); the "red"
evidence came from isolated `airflow tasks test` invocations that persist no
task states, so structural publish-unreachability was never demonstrated in
a real run; and the twin's registry artifacts had been overwritten by a
post-run re-copy. The self-healing observation itself was correct -- it is
exactly WHY the negative control must plant its defect inside the run.

Fix: `negative_control` is now a first-class DAG param/task (none | twin |
uneval), running AFTER gold_rebuild and BEFORE audit -- the only window a
gold-layer defect can survive to be audited. `pyarrow` was added to the
Airflow image so scripts/gate_b_mutate_twin.py (unmodified) runs in-container.
The mutator's own path guard still refuses any lake without "twin" in it,
and the uneval branch carries the same guard.

Evidence -- three self-contained full DAG runs, all persisted in the
metadata DB (psql task_instance, quoted verbatim):

GREEN leg, run manual__2026-07-22T20:07:51.207343+00:00_5Td7r5N3,
dag_run state success, 12/12 task instances success (negative_control logged
"negative_control=none -- no mutation (green leg)"), publish ran.

TWIN leg, run manual__2026-07-22T20:34:17.675964+00:00_fAVUCfgs,
dag_run state failed:

    task_id             | map_index | state
    audit               |        -1 | failed
    gold_rebuild        |        -1 | success
    negative_control    |        -1 | success
    publish_task        |        -1 | upstream_failed
    silver 0,1 / sensors 0-3 / env+paths tasks: success

    negative_control log: "rows_mutated": 10217
    audit log: [PASS] precondition: gold readable, 113228 active rows
               [FAIL] A1 no-lookahead: rows visible before their filing was
                      accepted: 10217        <- EXACTLY the planted count
               A2/A2b/A3/A4/A5/A6 all PASS
               gate_b_audit lake=.../twin-lake outcome=RED exit=1

UNEVAL leg, run manual__2026-07-22T20:36:28.619722+00:00_Xb18mg9K,
dag_run state failed:

    audit               |        -1 | failed
    negative_control    |        -1 | success
    publish_task        |        -1 | upstream_failed
    (all other instances success)

    negative_control log: "negative_control=uneval -- hid gold/_delta_log
                           under /vantage-data/scratch-airflow/uneval-twin-lake"
    audit log: [UNEVALUABLE] precondition: no Delta table at
               .../uneval-twin-lake/gold
               gate_b_audit ... outcome=UNEVALUABLE (halt -- not coerced into
               pass or fail) exit=2

The WAP conjunction now holds from single runs: publish ran ONLY in the
green leg; on both the red and unevaluable legs publish_task is
upstream_failed -- structural unreachability via the default all_success
trigger rule, recorded by the scheduler itself, with the audit red at
exactly the planted magnitude.
