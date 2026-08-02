"""vantage_pipeline -- WS1 Airflow orchestration for the VANTAGE PIT lakehouse.

See the WS1 contract (docs/superpowers/plans/2026-07-22-gap-closure-discovery.md
and the pasteable contract text handed to builders) for the full spec this DAG
implements. This module owns ONLY orchestration -- it never touches
src/main/scala, scripts/gate_b_*.py, or scripts/dqx_*.py; it wraps them.

WAP invariant (Write-Audit-Publish)
------------------------------------
  wait_for_source -> silver (per quarter) -> gold_rebuild (once)
      -> negative_control -> audit -> publish

Gold is written speculatively every run; it is only ever PROMOTED (the
`publish` task appends a promotion record to the registry) after `audit`
comes back GREEN. `audit` and `publish` both run with retries=0 -- a
red or unevaluable audit is a VERDICT, not a transient flake, and retrying
it would hide that. `publish` is left at Airflow's default trigger_rule
(all_success), so it is structurally unreachable on anything but a fully
green audit -- no branch operator routes around a failed/skipped audit.

Why gold_rebuild runs exactly once, after ALL silver tasks (never per
quarter): GoldRebuild.scala's own docstring says gold is a "PURE REBUILD:
full silver joined to the ACCUMULATED filing index. Building gold from a
single batch's sub frame... scopes the adsh join to that batch and drops
every other filing's facts" (the 2026-07-07 gold sub-scoping finding).
Rebuilding gold per quarter would reproduce that exact defect.

Why silver tasks never run concurrently: Pipeline.runSilverStage MERGEs into
a SINGLE silver Delta table and a single SilverSubStore per lake_root. Two
concurrent MERGE writers against the same Delta table race (Delta's
optimistic concurrency retries/fails one of them), and the repo's own
backfill-ledger convention (docs/BACKFILL-METRICS.md) is itself sequential
per quarter. This DAG enforces "at most one silver container running at a
time" via `max_active_tis_per_dagrun=1` on the mapped `silver` task (see
"Deliberate deviations" below for why that -- not a static
`silver_<quarter>` task chain -- is what actually implements this for an
arbitrary runtime-supplied `quarters` list).

Deliberate deviations from the contract's literal wording (flagged, not
silent)
------------------------------------------------------------------------
1. "ONE task per quarter, explicit upstream/downstream chaining
   quarter[i] -> quarter[i+1]": Airflow Params cannot reshape a DAG's
   static task graph -- the graph is fixed at file-parse time, before any
   DagRun (and therefore any params override) exists. A literal
   `silver_2009q2 >> silver_2009q3` edge list can only support a
   parse-time-fixed `quarters` list, which breaks the contract's own
   requirement that the SAME DAG serve the green/twin/uneval legs AND an
   arbitrary-length backfill window via trigger-time param overrides
   (section 2/4). Instead, `silver` is ONE dynamically-mapped
   (`.expand()`) DockerOperator, one map index per quarter in
   `params.quarters`, with `max_active_tis_per_dagrun=1` forcing strictly
   one map instance to run at a time (in ascending map-index order, which
   is Airflow's default TI-queueing order once concurrency is capped at
   1). This is the standard Airflow mechanism for a runtime-length
   homogeneous per-item sequential chain, and it gives the SAME
   no-concurrent-writers guarantee the literal wording was protecting,
   for any `quarters` override -- including the single-quarter default and
   the two-quarter backfill window -- with zero DAG redeploys.
2. "wait_for_source_<quarter> (one sensor per quarter... checking
   {source_dir}/{quarter}/num.txt and sub.txt exist)": for the same
   parse-time-vs-runtime reason, this is implemented as ONE dynamically
   mapped, deferrable `FileSensor` task (`wait_for_source`) expanded over
   the flattened list of every quarter's num.txt AND sub.txt path (2 *
   len(quarters) map instances). Every required file is still gated before
   `silver` starts; the grouping is by flat file list, not by a nested
   per-quarter task group, because Airflow's dynamic-task-group mapping
   does not compose cleanly with a preceding TaskFlow value being
   string-concatenated into ANOTHER operator's templated field inside the
   group-builder function.
3. Audit route: this file assumes the "in-process" route named in the
   contract (BashOperator running scripts/gate_b_audit.py inside the
   Airflow worker container itself), matching the (currently commented
   out) `pip install duckdb` line already staged in
   orchestration/airflow/Dockerfile. This is Integration's call to
   confirm/activate, not silently assumed to be already live -- if
   Integration instead routes the audit through a DockerOperator sibling
   container, this task needs to change from BashOperator to
   DockerOperator; nothing else in the DAG depends on which route is used.
4. Import surface: `airflow.sensors.filesystem.FileSensor`,
   `airflow.operators.bash.BashOperator`, `airflow.decorators.task`, and
   `airflow.models.param.Param` are used via Airflow's documented 2.x ->
   3.0 compatibility aliases (the underlying implementations now ship in
   `apache-airflow-providers-standard`, which apache/airflow:3.0.2 installs
   by default). `apache-airflow-providers-docker` (for DockerOperator and
   `docker.types.Mount`) is NOT bundled by default and must be added by
   Builder A's Dockerfile (already staged there, commented out). This file
   was built and proven with `python -m py_compile` / `ast.parse` only --
   no local Airflow install was available to this task -- so Integration
   must confirm these imports actually resolve once the compose stack is
   up, per verification-and-honesty discipline; this is stated as an
   assumption, not asserted as verified.

Params
------
  quarters:   list[str], default ["2009q2"]. Any length at trigger time;
              drives both the wait_for_source and silver map expansions.
  lake_root:  str, default "/vantage-data/scratch-airflow/lake"
              (container-side path, as seen by BOTH the Airflow worker
              container and every Spark sibling container -- both mount
              VANTAGE_DATA_PATH at /vantage-data). Twin leg overrides to
              .../twin-lake; uneval leg overrides to a silver-only fixture.
  source_dir: str, default "/vantage-data/scratch-airflow/source"

  negative_control: str, default "none". "twin" mutates the (twin) lake's
              gold AFTER the rebuild and BEFORE the audit
              (gate_b_mutate_twin.py -- its own guard refuses lakes without
              "twin" in the path); "uneval" hides gold/_delta_log the same
              way. WHY in-run: gold is a PURE REBUILD, so any mutation
              planted before the run is healed by gold_rebuild itself --
              confirmed live 2026-07-22, when the first negative-control
              attempt ran the full DAG over a pre-mutated twin and audit
              came back GREEN on the rebuilt gold (evidence doc, Gate 3).
              The audit can only be seen red from a defect planted between
              rebuild and audit, which is exactly what this leg does.

Task dependency expression (see bottom of file):
  wait_for_source >> silver >> gold_rebuild >> negative_control >> audit >> publish
`silver` is a mapped task (one map index per quarter); `gold_rebuild`,
being a normal downstream task, waits on ALL map indices reaching a
terminal state by Airflow's default behavior -- no extra trigger_rule
needed there.

Hard rules this file honors (see CLAUDE.md / the WS1 contract): never reads
or writes vantage-data/lake-backfill or vantage-data/source (only
scratch-airflow/ paths flow through PIT_* env vars and --lake); ASCII-only
in every string this file prints; never runs git; never edits Scala
sources, gate_b_*.py, dqx_*.py, or the test suite -- this file only
orchestrates them.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.filesystem import FileSensor
from docker.types import Mount

DAG_ID = "vantage_pipeline"

# ---------------------------------------------------------------------------
# Static config sourced from the Airflow container's own environment (set by
# orchestration/airflow/docker-compose.yaml / .env -- Builder A). These are
# stable per deployment, not per-DagRun, so they're read once at parse time
# rather than templated per task.
# ---------------------------------------------------------------------------

# HOST-side paths -- used as Mount(source=...) for sibling containers that
# DockerOperator launches on the HOST docker daemon (via the docker.sock
# mount). These are NOT the same as the /vantage and /vantage-data paths
# below, which are container-internal mount targets inside the Airflow
# worker container itself.
VANTAGE_REPO_PATH_HOST = os.environ.get("VANTAGE_REPO_PATH", "C:/Users/hossa/dev/vantage")
VANTAGE_DATA_PATH_HOST = os.environ.get("VANTAGE_DATA_PATH", "C:/Users/hossa/dev/vantage-data")

VANTAGE_SPARK_IMAGE = os.environ.get(
    "VANTAGE_SPARK_IMAGE", "apache/spark:3.5.3-scala2.12-java17-python3-ubuntu"
)
# Container-side path once /vantage is mounted into the Spark sibling image.
VANTAGE_JAR_PATH = os.environ.get(
    "VANTAGE_JAR_PATH", "/vantage/target/scala-2.12/vantage-assembly-0.1.0.jar"
)

# Container-internal mount targets inside the AIRFLOW WORKER container
# itself (per orchestration/airflow/docker-compose.yaml): the repo is
# mounted read-only at /vantage, the data tree read-write at /vantage-data.
# The audit task (running in-process in this container, see deviation #3
# above) reads scripts/gate_b_audit.py from here.
AIRFLOW_REPO_MOUNT = "/vantage"
GATE_B_AUDIT_SCRIPT = f"{AIRFLOW_REPO_MOUNT}/scripts/gate_b_audit.py"

SPARK_SUBMIT_BIN = "/opt/spark/bin/spark-submit"  # not on PATH in the Spark image -- full path required
SPARK_CONF_TIMEZONE = "spark.sql.session.timeZone=America/New_York"


def _spark_submit_command(spark_class: str) -> list[str]:
    """Build the sibling container's command, wrapped with `umask 000`.

    INTEGRATION FIX (2026-07-22): the Spark sibling container runs as the
    image's own default user (uid 185 "spark" -- see the silver task's
    comment for why an arbitrary/matched uid is NOT used here), so every
    file/dir it writes under {lake_root} (bronze, silver, gold, quarantine,
    registry) is owned by uid 185 with the image's default umask (022 ->
    mode 0755 dirs / 0644 files). The Airflow worker (uid 50000, gid 0)
    cannot then create NEW files in those directories -- confirmed live as
    a PermissionError from the `audit` task writing its --json output, and
    the same failure mode would hit `publish`'s publish-log.jsonl write.
    Directory-level Unix permissions gate this at the directory itself
    (owner-only write bit); matching uid/gid between the two containers was
    tried and reverted (breaks Spark's own Hadoop UserGroupInformation
    login, see above). A plain shell `umask 000` does NOT fix this --
    confirmed live: Hadoop's RawLocalFileSystem.mkdirs()/create() apply
    permissions via the SEPARATE `fs.permissions.umask-mode` Hadoop config
    (default "022"), not the OS process umask, so a shell-level umask has
    no effect on directories/files Spark's own filesystem layer creates
    (registry, bronze, silver, gold, quarantine, _delta_log, parquet parts
    all stayed mode 0755/0644 under a `bash -c 'umask 000 && ...'` wrapper
    in an earlier attempt). Setting `spark.hadoop.fs.permissions.umask-mode
    =000` is the config Hadoop itself actually reads; confirmed live this
    makes every dir/file Spark creates world-writable (dirs 0777, files
    0666), so the Airflow worker (uid 50000) can add its own sibling files
    (audit's --json output, publish's publish-log.jsonl) into the same
    directories afterward.
    """
    return [
        SPARK_SUBMIT_BIN,
        "--class",
        spark_class,
        "--master",
        "local[*]",
        "--conf",
        SPARK_CONF_TIMEZONE,
        "--conf",
        "spark.hadoop.fs.permissions.umask-mode=000",
        VANTAGE_JAR_PATH,
    ]


SILVER_COMMAND = _spark_submit_command("pit.Pipeline")
GOLD_COMMAND = _spark_submit_command("pit.gold.GoldRebuild")

GOLD_STDOUT_RE = re.compile(r"delta_version=(\d+)")


def _sibling_mounts() -> list[Mount]:
    """Bind mounts for every DockerOperator sibling container (Spark image).

    source= is a HOST path (resolved by the host docker daemon, not by the
    Airflow worker container's own filesystem) -- see the module docstring's
    note on VANTAGE_REPO_PATH_HOST / VANTAGE_DATA_PATH_HOST vs
    AIRFLOW_REPO_MOUNT.
    """
    return [
        Mount(source=VANTAGE_REPO_PATH_HOST, target="/vantage", type="bind", read_only=True),
        Mount(source=VANTAGE_DATA_PATH_HOST, target="/vantage-data", type="bind", read_only=False),
    ]


with DAG(
    dag_id=DAG_ID,
    description="WS1: VANTAGE silver->gold->audit->publish over a scratch lake (WAP, fail-closed).",
    schedule=None,  # manually triggered per params leg -- see contract section 2/4
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "vantage-ws1",
        "retries": 0,  # per-task overrides below where the contract calls for retries
    },
    params={
        "quarters": Param(
            default=["2009q2"],
            type="array",
            items={"type": "string"},
            description="Quarters to ingest through silver, e.g. 2009q2. Sequential, one map index each.",
        ),
        "lake_root": Param(
            default="/vantage-data/scratch-airflow/lake",
            type="string",
            description="Container-side scratch lake root. Override to .../twin-lake or an "
            "uneval fixture for the red/unevaluable legs. NEVER point this at "
            "lake-backfill or source.",
        ),
        "source_dir": Param(
            default="/vantage-data/scratch-airflow/source",
            type="string",
            description="Container-side scratch source root (copied quarter dirs, never the real source/ tree).",
        ),
        "negative_control": Param(
            default="none",
            type="string",
            enum=["none", "twin", "uneval"],
            description="In-run negative control: 'twin' plants a lookahead mutation in gold "
            "between rebuild and audit (lake_root must contain 'twin'); 'uneval' hides "
            "gold/_delta_log so the audit is unevaluable. 'none' = green leg, no-op.",
        ),
    },
    tags=["vantage", "ws1", "gate-b"],
) as dag:

    @task
    def source_filepaths(params: dict) -> list[str]:
        """Flatten every requested quarter's required source files into one list.

        One list entry per (quarter, filename) pair -- see deviation #2 in
        the module docstring for why this replaces a literal
        one-sensor-task-per-quarter shape.
        """
        source_dir = params["source_dir"]
        quarters = params["quarters"]
        paths: list[str] = []
        for quarter in quarters:
            paths.append(f"{source_dir}/{quarter}/num.txt")
            paths.append(f"{source_dir}/{quarter}/sub.txt")
        return paths

    # Deferrable so each poll doesn't hold a worker slot. Uses the
    # "fs_default" Connection (base path "/") -- filepath here is already
    # absolute, so the base path contributes nothing, but the connection
    # must still exist/resolve. INTEGRATION NOTE (2026-07-22): confirmed
    # live that Airflow 3.0.2 does NOT seed this by default (this module's
    # original assumption that it "ships with every Airflow install" was
    # wrong and was caught by a live run: AirflowNotFoundException, conn_id
    # `fs_default` isn't defined). Fixed at the compose layer instead of
    # here -- see orchestration/airflow/docker-compose.yaml's airflow-init
    # command block, which now creates it idempotently on every `docker
    # compose up`.
    wait_for_source = FileSensor.partial(
        task_id="wait_for_source",
        fs_conn_id="fs_default",
        deferrable=True,
        poke_interval=30,
        timeout=60 * 60,
    ).expand(filepath=source_filepaths())

    @task
    def silver_environments(params: dict) -> list[dict]:
        """One PIT_* env dict per quarter, in the order silver must process them."""
        lake_root = params["lake_root"]
        source_dir = params["source_dir"]
        envs: list[dict] = []
        for quarter in params["quarters"]:
            envs.append(
                {
                    "PIT_SOURCE_DIR": source_dir,
                    "PIT_BRONZE_ROOT": f"{lake_root}/bronze",
                    "PIT_SILVER_ROOT": f"{lake_root}/silver",
                    "PIT_GOLD_ROOT": f"{lake_root}/gold",
                    "PIT_QUARANTINE_ROOT": f"{lake_root}/quarantine",
                    "PIT_REGISTRY_PATH": f"{lake_root}/registry",
                    "PIT_QUARTERS": quarter,
                    # LOAD-BEARING: Delta SQL extensions/catalog are only
                    # registered inside cfg.spark.master.foreach in
                    # Pipeline.scala -- omitting this silently disables
                    # Delta support on the local/classic path.
                    "PIT_SPARK_MASTER": "local[*]",
                }
            )
        return envs

    # One DockerOperator map index per quarter (see deviation #1). Bounded
    # retries because container/Docker-daemon flakiness is a real transient
    # possibility here -- this is not a verdict task.
    silver = DockerOperator.partial(
        task_id="silver",
        image=VANTAGE_SPARK_IMAGE,
        command=SILVER_COMMAND,
        docker_url="unix://var/run/docker.sock",
        mounts=_sibling_mounts(),
        mount_tmp_dir=False,
        auto_remove="success",
        # INTEGRATION NOTE (2026-07-22): deliberately NOT setting `user=` here
        # to match the Airflow worker's uid (50000). Tried that first to fix
        # a downstream permission problem (see `audit` task's comment below)
        # and confirmed live it breaks Spark itself: apache/spark:3.5.3's
        # image has no /etc/passwd entry for an arbitrary uid, so Hadoop's
        # UserGroupInformation.createLoginUser() -> UnixLoginModule crashes
        # with `KerberosAuthException: ... NullPointerException: invalid
        # null input: name` building the JAAS UnixPrincipal for SparkContext
        # startup -- happens even with spark.jars.ivy/-Duser.name/HOME
        # workarounds (UnixLoginModule reads OS user info via a native call,
        # not the user.name/HOME JVM knobs). The image's own default user
        # (uid 185 "spark", which DOES have a passwd entry) has no such
        # problem, so silver/gold_rebuild run as that default instead, and
        # the write-permission problem is solved on the Airflow side (see
        # the `audit` task).
        max_active_tis_per_dagrun=1,  # exactly one silver container running at a time
        retries=2,
        retry_exponential_backoff=True,
    ).expand(environment=silver_environments())

    # Single rebuild after ALL silver map indices reach a terminal state
    # (Airflow's default behavior for a normal task downstream of a mapped
    # task -- no special trigger_rule needed). Bulk-idempotent rebuild, so a
    # bounded retry here is still legitimate (unlike audit/publish below).
    gold_rebuild = DockerOperator(
        task_id="gold_rebuild",
        image=VANTAGE_SPARK_IMAGE,
        command=GOLD_COMMAND,
        environment={
            # INTEGRATION FIX (2026-07-22): pit.config.AppConfig.PathsCfg
            # declares sourceDir as a required (non-Optional) String --
            # GoldRebuild.scala never reads it, but AppConfig.load() still
            # fails config parsing without it ("Key not found: 'source-dir'"
            # -- confirmed live). The contract's spike note ("identical env
            # vars... no PIT_QUARTERS needed") meant PIT_QUARTERS is the ONLY
            # var to drop; this was missed in the original task definition.
            "PIT_SOURCE_DIR": "{{ params.source_dir }}",
            "PIT_BRONZE_ROOT": "{{ params.lake_root }}/bronze",
            "PIT_SILVER_ROOT": "{{ params.lake_root }}/silver",
            "PIT_GOLD_ROOT": "{{ params.lake_root }}/gold",
            "PIT_QUARANTINE_ROOT": "{{ params.lake_root }}/quarantine",
            "PIT_REGISTRY_PATH": "{{ params.lake_root }}/registry",
            "PIT_SPARK_MASTER": "local[*]",  # same load-bearing note as above
        },
        docker_url="unix://var/run/docker.sock",
        mounts=_sibling_mounts(),
        mount_tmp_dir=False,
        auto_remove="success",
        # No `user=` override -- see the silver task's comment above.
        retries=2,
        retry_exponential_backoff=True,
        # INTEGRATION FIX (2026-07-22): the original comment here assumed
        # DockerOperator's default XCom push (last stdout line only) would
        # be GoldRebuild.scala's own summary println. Confirmed live that's
        # wrong -- Spark's shutdown INFO logging (SparkContext stopping,
        # ShutdownHookManager, ...) interleaves into the SAME captured
        # stream after that println, so the true last line is shutdown
        # noise and the regex match against it came back None (publish
        # record showed "gold_delta_version": null on an otherwise fully
        # GREEN run). xcom_all=True pushes every captured line as a list;
        # publish_task now searches all of them for the delta_version=...
        # pattern instead of assuming it's the last one.
        do_xcom_push=True,
        xcom_all=True,
    )

    # Negative control as a first-class DAG leg (rigor: an audit never seen
    # red on known-bad input is unevaluable evidence). It runs AFTER
    # gold_rebuild and BEFORE audit because gold is a pure rebuild -- any
    # defect planted earlier is healed by the rebuild itself (confirmed live
    # 2026-07-22: the first negative-control attempt pre-mutated a twin,
    # ran the full DAG, and audit came back GREEN on the rebuilt gold).
    # mode none = success no-op, so the green leg keeps default all_success
    # trigger rules everywhere downstream. retries=0: planting a defect is
    # deterministic; a failure here must halt the leg, not retry.
    negative_control = BashOperator(
        task_id="negative_control",
        retries=0,
        bash_command=(
            "set -e\n"
            'MODE="{{ params.negative_control }}"\n'
            'LAKE="{{ params.lake_root }}"\n'
            'case "$MODE" in\n'
            "  none)\n"
            '    echo "negative_control=none -- no mutation (green leg)" ;;\n'
            "  twin)\n"
            f"    python {AIRFLOW_REPO_MOUNT}/scripts/gate_b_mutate_twin.py "
            '--twin-lake "$LAKE" --manifest "$LAKE/mutation-manifest.json" ;;\n'
            "  uneval)\n"
            '    case "$LAKE" in *twin*) ;; *) echo "REFUSING: uneval mutation outside a twin lake"; exit 3 ;; esac\n'
            '    mv "$LAKE/gold/_delta_log" "$LAKE/gold/_delta_log.hidden"\n'
            '    echo "negative_control=uneval -- hid gold/_delta_log under $LAKE" ;;\n'
            '  *) echo "unknown negative_control mode: $MODE"; exit 4 ;;\n'
            "esac\n"
        ),
    )

    # A red OR unevaluable audit is a VERDICT, not a flake: retries=0.
    # gate_b_audit.py's exit codes are 0 (GREEN) / 1 (RED) / 2 (UNEVALUABLE);
    # BashOperator's default "any nonzero exit fails the task" already maps
    # BOTH 1 and 2 to task failure -- do not special-case exit 2, per the
    # contract and rigor's data-quality-fail-closed discipline.
    audit = BashOperator(
        task_id="audit",
        retries=0,
        bash_command=(
            f"python {GATE_B_AUDIT_SCRIPT} "
            "--lake {{ params.lake_root }} "
            "--json {{ params.lake_root }}/registry/audit-{{ run_id }}.json"
        ),
    )

    @task
    def publish_task(params: dict, run_id: str, ti=None) -> None:
        """Append one promotion record to {lake_root}/registry/publish-log.jsonl.

        Left at Airflow's default trigger_rule (all_success): structurally
        unreachable unless `audit` succeeded, i.e. unless gate_b_audit.py
        exited 0 (GREEN). retries=0 -- same verdict-not-flake reasoning as
        audit.
        """
        lake_root = params["lake_root"]
        audit_json_path = f"{lake_root}/registry/audit-{run_id}.json"

        # gold_rebuild pushes xcom_all=True -> a list of every captured
        # line (see that task's comment: the summary println is NOT
        # reliably the last line once Spark's shutdown logging interleaves
        # into the same stream). Search all lines, not just one.
        gold_stdout = ti.xcom_pull(task_ids="gold_rebuild") if ti else None
        gold_delta_version = None
        if gold_stdout:
            lines = gold_stdout if isinstance(gold_stdout, list) else [gold_stdout]
            for line in lines:
                m = GOLD_STDOUT_RE.search(str(line))
                if m:
                    gold_delta_version = int(m.group(1))
                    break

        record = {
            "published_at_utc": datetime.now(timezone.utc).isoformat(),
            "dag_run_id": run_id,
            "lake_root": lake_root,
            "quarters": params["quarters"],
            "audit_json_path": audit_json_path,
            "audit_outcome": "GREEN",  # fixed: this task is unreachable otherwise
            "gold_delta_version": gold_delta_version,
            # Matches the "unknown" fallback convention already documented
            # in docs/GATE-B-WAP-EVIDENCE.md's residual-gaps note.
            "code_sha": os.environ.get("VANTAGE_CODE_SHA", "unknown"),
        }

        registry_dir = f"{lake_root}/registry"
        os.makedirs(registry_dir, exist_ok=True)
        log_path = f"{registry_dir}/publish-log.jsonl"
        with open(log_path, "a", encoding="ascii") as f:
            f.write(json.dumps(record, ensure_ascii=True) + "\n")
        print(f"published lake_root={lake_root} run_id={run_id} path={log_path}")  # ASCII

    publish = publish_task()

    # ---- task dependency expression --------------------------------------
    wait_for_source >> silver >> gold_rebuild >> negative_control >> audit >> publish
