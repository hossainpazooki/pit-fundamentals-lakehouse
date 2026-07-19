# Development — build, test, run

State as of 2026-07-19.

## Build profiles

One source tree, two profiles:

| Profile | Toolchain | Purpose |
|---|---|---|
| **2.12** (default) | Scala 2.12 / Spark 3.5 / Delta 3.x | Local dev + the whole test suite (Spark local mode) |
| **2.13** | Scala 2.13 / Databricks Connect 17.3 (`Provided`) | The serverless Databricks jar (environment v4 accepts nothing else) |

```bash
sbt test                  # unit transforms + the §5 / §6 / §7 property tests (2.12 profile)
sbt assembly              # classic fat jar -> target/scala-2.12/vantage-assembly-*.jar
sbt "++2.13.16 assembly"  # serverless jar  -> target/scala-2.13/vantage-assembly-*.jar
```

The suite runs only on the 2.12 profile — the Connect profile has no local Spark —
so 2.12 green is the parity gate for code shared by both. That works because the
code sticks to the surface both runtimes provide: Delta operations go through SQL
(`MERGE INTO delta.`…``, `DESCRIBE HISTORY` — `pit.util.DeltaIO`), and the DQ gate
is plain DataFrame aggregations (no Deequ, no `io.delta.tables`, no
`SparkContext`).

Requires **JDK 17** (Temurin; a Spark-3.5-incompatible system JDK is a known trap)
and Spark 3.5's Hadoop runtime. On Windows, set `HADOOP_HOME` to a directory
containing `winutils.exe` + `hadoop.dll`, and prefix every sbt call (env is
per-invocation):

```powershell
$env:HADOOP_HOME='C:\path\to\hadoop'; $env:PATH="$env:HADOOP_HOME\bin;$env:PATH"; sbt test
```

CI (GitHub Actions) runs suite + scalafmt/scalafix + assembly on every push.

## Config contract

`PIT_SOURCE_DIR` is the parent of quarter directories (`<source>/<quarter>/num.txt`
+ `sub.txt`, the layout of the unzipped [SEC Financial Statement Data
Sets](https://www.sec.gov/dera/data/financial-statement-data-sets)); `PIT_QUARTERS`
is a comma-separated quarter list; `PIT_BRONZE_ROOT` / `PIT_SILVER_ROOT` /
`PIT_GOLD_ROOT` / `PIT_QUARANTINE_ROOT` / `PIT_REGISTRY_PATH` place the lake.

Config arrives two equivalent ways:

- **Environment variables** (local/classic runs).
- **`KEY=VALUE` program arguments** with the same PIT_* names
  (`pit.Pipeline PIT_SOURCE_DIR=... PIT_QUARTERS=...`) — the whole contract on
  serverless Databricks, which has no task environment variables
  (`AppConfig.loadFromArgs` turns them into system properties, which HOCON
  resolves exactly like env vars; args win over env).

## Running locally

The run is two steps: `pit.Pipeline` ingests quarters through the **silver stage**
(bronze, registry, DQ gate, silver + filing-index merges; one `run …` metrics line
per quarter, with `wall_clock_ms`), then one `pit.gold.GoldRebuild` pass rebuilds
gold from the whole lake. Rebuilding gold per quarter would be quadratic in lake
size — and gold is a pure function of silver + the filing index, so once at the end
is the correct shape, not a shortcut.

```bash
PIT_SOURCE_DIR=./data PIT_QUARTERS=2026q1 \
PIT_BRONZE_ROOT=./lake/bronze PIT_SILVER_ROOT=./lake/silver \
PIT_GOLD_ROOT=./lake/gold PIT_QUARANTINE_ROOT=./lake/quarantine \
PIT_REGISTRY_PATH=./lake/registry \
spark-submit --class pit.Pipeline target/scala-2.12/vantage-assembly-*.jar
# then, once per backfill:
spark-submit --class pit.gold.GoldRebuild target/scala-2.12/vantage-assembly-*.jar
```

## Running on Databricks (serverless)

The same two steps run as the `vantage-pipeline` job (`databricks.yml`): a
serverless environment-v4 Asset Bundle whose jar is installed from a Unity Catalog
volume (`java_dependencies` — the bundle does not rewrite local paths inside
environment specs, so the jar upload is an explicit step), with `run_gold_rebuild`
strictly after `run_pipeline`. Source data and the lake live under a UC volume
(public DBFS root is disabled on the target workspace).

```powershell
$env:DATABRICKS_HOST='https://<workspace-host>'
sbt "++2.13.16 assembly"
databricks fs cp --overwrite target/scala-2.13/vantage-assembly-0.1.0.jar dbfs:/Volumes/<catalog>/<schema>/vantage/jars/vantage-assembly-0.1.0.jar
databricks bundle deploy -t dev --var="source_dir=/Volumes/.../source" --var="lake_root=/Volumes/.../lake"
databricks bundle run vantage_pipeline -t dev --var="source_dir=..." --var="lake_root=..."
```

Auth note: `databricks auth login` run from a non-interactive shell stores the
OAuth token (host-keyed) but writes no profile — prefix commands with
`$env:DATABRICKS_HOST=...`.

Publish-credit discipline for any future publish (see
[GATE-B-WAP-EVIDENCE.md](GATE-B-WAP-EVIDENCE.md)): negative control against the
pre-publish state FIRST, then run, then audit + probe the published state, then
`check-effect-probe.mjs` must print `effect-probe: clean`.

## Tooling under `scripts/`

Full-history backfill: `fetch_sec_quarters.py` (polite, listing-verified SEC
downloads), `backfill_driver.py` (one quarter per invocation, JSONL ledger,
record-and-continue on halt), `make_uneval_fixture.py` (injected halt demo against
a scratch lake), `recompute_metrics.py` (per-quarter metrics recomputed from run
artifacts — the tool to re-derive any number in
[BACKFILL-METRICS.md](BACKFILL-METRICS.md); don't hand-recount).

Gate B publish verification: `gate_b_audit.py` (data-facing PIT audit of a lake,
three-outcome fail-closed — green / red / unevaluable-halts), `gate_b_mutate_twin.py`
(seeds the known-bad twin that proves the audit can go red), `gate_b_probe.py`
(consumer-path as-of probe with negative control), `gate_b_refute_workflow.mjs`
(the adversarial verification fan-out over the evidence).
