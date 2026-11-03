# WS1 -- Airflow orchestration (VANTAGE gap-closure sprint, 2026-07-22)

Runs the existing `pit.Pipeline` / `pit.gold.GoldRebuild` fat jar under
Airflow, on a **scratch** copy of the lake, gated by
`scripts/gate_b_audit.py`. Nothing here changes the Scala pipeline, the DQ
gate, or the audit script -- this directory is orchestration wrapped around
what already exists. Full design rationale lives in
`docs/adr/0001-airflow-orchestration.md`; the run evidence lives in
`docs/AIRFLOW-ORCHESTRATION-EVIDENCE.md`.

## What's here

| File | Owns |
|---|---|
| `docker-compose.yaml` | Airflow 3.0.2, LocalExecutor: postgres + apiserver + scheduler + dag-processor + triggerer. No Celery/Redis/worker/Flower. |
| `Dockerfile` | `apache/airflow:3.0.2` + `duckdb`, `apache-airflow-providers-docker`, `apache-airflow-providers-fab` (see the file's own header comment for why each, and what was deliberately left out). |
| `.env.example` | Copy to `.env` (gitignored) for local overrides. Dev-only defaults. |
| `dags/vantage_pipeline.py` | The one DAG: `wait_for_source_<quarter>` -> `silver_<quarter>` (sequential) -> `gold_rebuild` -> `audit` -> `publish`. |

## Sibling-container execution model

Every Spark task (`silver_<quarter>`, `gold_rebuild`) runs via
`DockerOperator`, which talks to the **host** Docker daemon over
`/var/run/docker.sock` (bind-mounted into every Airflow service) to launch a
sibling `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` container. This
is not Docker-in-Docker -- it's the same daemon your `docker ps` sees on the
host. Confirmed viable directly in the pre-build spike:

```
MSYS_NO_PATHCONV=1 docker run --rm -v //var/run/docker.sock:/var/run/docker.sock \
  docker:cli docker ps
```

returned the live host container list. See `docs/adr/0001-airflow-orchestration.md`
for why this route was chosen over baking Spark/JDK17 into the Airflow image
(short version: the fat jar excludes Spark -- `build.sbt` marks
`spark-core`/`spark-sql` `Provided` -- and the host JDK is 21, incompatible
with Spark 3.5; the sibling-container route sidesteps both without touching
the Airflow base image).

The `gate_b_audit.py` audit task runs **in-process inside the Airflow
worker** (not a sibling container) -- `duckdb` is installed directly in this
Dockerfile for that purpose.

## Bring-up

```bash
cd orchestration/airflow
cp .env.example .env        # edit VANTAGE_REPO_PATH / VANTAGE_DATA_PATH if this checkout lives elsewhere
docker compose build
docker compose up -d
```

No separate `docker compose run airflow-init` step is needed: `airflow-init`
(directory bootstrap, `airflow db migrate`, dev-admin user creation via
`_AIRFLOW_WWW_USER_USERNAME`/`_AIRFLOW_WWW_USER_PASSWORD`) is wired as a
`condition: service_completed_successfully` dependency of every long-running
service, so `docker compose up -d` runs it first automatically and the other
services wait for it to finish cleanly.

Check status:

```bash
docker compose ps
```

All five long-running services (`postgres`, `airflow-apiserver`,
`airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`) should
reach `healthy`. UI: `http://localhost:8080`, login `airflow` / `airflow`
(dev-only credentials -- see `.env.example`; do not expose this stack beyond
localhost with those defaults still set).

Verified locally (this session, 2026-07-22):
- `docker compose config -q` -> exit 0 (both before and after the final
  edits below).
- `docker compose build` -> all five images (`airflow-apiserver`,
  `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`,
  `airflow-init`, plus the debug-profile `airflow-cli`) built successfully.
- `docker compose run --rm --no-deps airflow-cli python -c "import duckdb,
  airflow.providers.docker, airflow.providers.fab"` -> succeeded (`duckdb
  1.5.5`, both providers import cleanly).
- Full stack `up`/health/DAG-trigger verification is Integration's job (WS1
  contract section 6), not re-done here -- this README only claims what was
  directly run in this session.

## Triggering the DAG -- three params legs

`dags/vantage_pipeline.py` exposes three Airflow Params: `quarters`,
`lake_root`, `source_dir`. Same DAG, three legs -- not three DAGs.

Via the UI: Trigger DAG w/ config, and paste one of the JSON blocks below.

Via the CLI (either `docker compose exec airflow-apiserver airflow ...`
against a running stack, or the ad hoc `airflow-cli` debug-profile service:
`docker compose run --rm airflow-cli airflow ...`):

**GREEN leg** (default params -- ordinary scratch lake):
```bash
docker compose exec airflow-apiserver airflow dags trigger vantage_pipeline \
  -c '{"quarters": ["2009q2"], "lake_root": "/vantage-data/scratch-airflow/lake", "source_dir": "/vantage-data/scratch-airflow/source"}'
```

**TWIN (RED) leg** -- against a lake `scripts/gate_b_mutate_twin.py` has
already corrupted (build the twin lake per
`docs/AIRFLOW-ORCHESTRATION-EVIDENCE.md`'s Integration steps first; the
script refuses any path without literal `twin` in it):
```bash
docker compose exec airflow-apiserver airflow dags trigger vantage_pipeline \
  -c '{"quarters": ["2009q2"], "lake_root": "/vantage-data/scratch-airflow/twin-lake", "source_dir": "/vantage-data/scratch-airflow/source"}'
```

**UNEVALUABLE leg** -- silver-only fixture lake, no `gold/_delta_log` yet
(re-point at whatever silver-only scratch copy Builder B's DAG produced or
documented; `gate_b_audit.py` exits 2 the moment `_delta_log` is missing):
```bash
docker compose exec airflow-apiserver airflow dags trigger vantage_pipeline \
  -c '{"quarters": ["2009q2"], "lake_root": "/vantage-data/scratch-airflow/uneval-lake", "source_dir": "/vantage-data/scratch-airflow/source"}'
```

Check task outcomes without trusting the UI color alone:
```bash
docker compose exec airflow-apiserver airflow tasks states-for-dag-run vantage_pipeline <dag_run_id>
```

## Backfill / idempotency exercise

Two runs over the same bounded window, per `dags/vantage_pipeline.py`'s own
module docstring (coordinate the literal command text there if it drifts
from this README):

```bash
docker compose exec airflow-apiserver airflow dags trigger vantage_pipeline \
  -c '{"quarters": ["2009q2", "2009q3"], "lake_root": "/vantage-data/scratch-airflow/lake", "source_dir": "/vantage-data/scratch-airflow/source"}'
# ... wait for completion, capture pre-rerun silver/gold Delta versions and
# registry/quarantine counts, then trigger the identical config again and
# diff. See docs/AIRFLOW-ORCHESTRATION-EVIDENCE.md for the captured output
# and which comparison tool (scripts/idempotency_diff.py or the simpler
# registry/batch-count check) actually applied.
```

## Where things live

- Scratch lake root (never `lake-backfill/` or `source/`):
  `$VANTAGE_DATA_PATH/scratch-airflow/` (the host data root from `.env`) -- see
  `docs/adr/0001-airflow-orchestration.md` and the DAG module docstring for
  the full `lake/` / `twin-lake/` / uneval-fixture layout.
- Evidence doc: `docs/AIRFLOW-ORCHESTRATION-EVIDENCE.md`.
- ADR: `docs/adr/0001-airflow-orchestration.md`.
- Interview writeup: `docs/interview/airflow.md`.

## Teardown

```bash
docker compose down          # keep the postgres volume (metadata persists)
docker compose down -v       # also drop the postgres volume (full reset)
```
