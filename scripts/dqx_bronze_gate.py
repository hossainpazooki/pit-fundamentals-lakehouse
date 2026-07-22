"""DQX generic row-checks over bronze/raw SEC quarter TSVs (sub.txt, num.txt).

Wrapped fail-closed, three-outcome, exactly like `gate_b_audit.py`:
  exit 0  every check outcome == "pass" or "warn" (warn = violation counted and
          printed, but the check's criticality is "warn": a measured, expected
          data characteristic -- it never gates)
  exit 1  no unevaluable checks, at least one check outcome == "fail"
  exit 2  at least one check outcome == "unevaluable" -- UNEVALUABLE ALWAYS WINS,
          even if other checks would otherwise be pass/fail. This includes:
          missing data files, missing required columns, zero rows, any DQX/Spark
          engine construction or execution error, and any skip-shaped DQX result
          (a check referencing an unresolvable column: DQX catches the Spark
          AnalysisException internally and marks the check `skipped=True` in
          `_errors` rather than raising -- see
          docs/superpowers/specs/2026-07-21-dqx-friction-log.md entry #3 -- which
          this gate must detect explicitly rather than trust good/bad counts alone).

Caveat (spike finding, load-bearing -- see the friction log entry #2): DQEngine
construction requires a live, reachable Databricks workspace even for pure
local-Spark checks. In an environment without one, construction can hang (SDK
retry/backoff) rather than fail fast. This gate does not work around that hang
(no manual timeout/threading) -- a hang here is a known, accepted environment
risk, out of scope for this gate to engineer around.

Usage:
  C:/Users/hossa/dev/vantage/.venv-dqx/Scripts/python.exe scripts/dqx_bronze_gate.py \
      --data <path> [--quarter <q>] [--json <out.json>]
"""

import argparse
import json
import os
import sys
from pathlib import Path

REQUIRED_SUB_COLS = {"adsh", "cik", "form"}
REQUIRED_NUM_COLS = {"tag", "value"}

CHECKS = [
    {"name": "sub_cik_not_null", "table": "sub", "check_func": "is_not_null",
     "column": "cik", "criticality": "error"},
    {"name": "sub_adsh_not_null", "table": "sub", "check_func": "is_not_null",
     "column": "adsh", "criticality": "error"},
    {"name": "sub_form_not_null_not_empty", "table": "sub",
     "check_func": "is_not_null_and_not_empty", "column": "form",
     "criticality": "error"},
    # criticality "warn": null `value` is a known SEC characteristic, not a defect
    # -- footnote-only rows are structural and the silver stage routes them to the
    # quarantine rows lane "rather than failing the whole batch on completeness"
    # (src/main/scala/pit/Pipeline.scala). A gating null-check here refuses every
    # real quarter (measured: 9/3999 in 2009q2, 139723/3690955 in 2026q1).
    {"name": "num_value_not_null", "table": "num", "check_func": "is_not_null",
     "column": "value", "criticality": "warn"},
    {"name": "num_tag_not_null", "table": "num", "check_func": "is_not_null",
     "column": "tag", "criticality": "error"},
]


def _precondition(name, table, column, check_func, outcome, detail):
    """Build a check-result dict; used for both precondition and per-check rows."""
    return {
        "name": name,
        "table": table,
        "column": column,
        "check_func": check_func,
        "outcome": outcome,
        "total": None,
        "good": None,
        "bad": None,
        "detail": detail,
    }


def run_checks(data: Path):
    """Yield check-result dicts (see _precondition for shape).

    Fail-closed: any precondition failure yields a single "precondition" entry
    with outcome "unevaluable" and stops (matches gate_b_audit.py's early-return
    shape). Only past all preconditions do we construct the DQX engine and run
    the declarative CHECKS list.
    """
    sub_path = data / "sub.txt"
    num_path = data / "num.txt"

    # 1.2(a) sub.txt and num.txt both exist under --data.
    missing_files = [p.name for p in (sub_path, num_path) if not p.exists()]
    if missing_files:
        yield _precondition(
            "precondition", None, None, None, "unevaluable",
            f"missing data file(s) under {data.as_posix()}: {sorted(missing_files)}",
        )
        return

    # Spark session setup (verified in spike) -- must happen before any read.
    os.environ.setdefault("HADOOP_HOME", r"C:\Users\hossa\hadoop")
    os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("dqx_bronze_gate").getOrCreate()
    except Exception as e:  # noqa: BLE001 - any Spark session failure is unevaluable
        yield _precondition(
            "precondition", None, None, None, "unevaluable",
            f"Spark session construction failed: {e}",
        )
        return

    # 1.2(b) both readable via the Spark read.
    try:
        sub_df = (
            spark.read.option("header", True).option("sep", "\t")
            .option("inferSchema", True).csv(f"{data.as_posix()}/sub.txt")
        )
        num_df = (
            spark.read.option("header", True).option("sep", "\t")
            .option("inferSchema", True).csv(f"{data.as_posix()}/num.txt")
        )
    except Exception as e:  # noqa: BLE001 - any read failure is unevaluable, not red
        yield _precondition(
            "precondition", None, None, None, "unevaluable",
            f"data unreadable: {e}",
        )
        return

    # 1.2(c) required columns present.
    missing_sub = sorted(REQUIRED_SUB_COLS - set(sub_df.columns))
    missing_num = sorted(REQUIRED_NUM_COLS - set(num_df.columns))
    if missing_sub or missing_num:
        detail_parts = []
        if missing_sub:
            detail_parts.append(f"sub.txt missing columns: {missing_sub}")
        if missing_num:
            detail_parts.append(f"num.txt missing columns: {missing_num}")
        yield _precondition(
            "precondition", None, None, None, "unevaluable", "; ".join(detail_parts),
        )
        return

    # 1.2(d) both tables have rows.
    sub_count = sub_df.count()
    num_count = num_df.count()
    if sub_count == 0:
        yield _precondition(
            "precondition", None, None, None, "unevaluable",
            "sub.txt has zero rows -- nothing to gate",
        )
        return
    if num_count == 0:
        yield _precondition(
            "precondition", None, None, None, "unevaluable",
            "num.txt has zero rows -- nothing to gate",
        )
        return

    yield _precondition(
        "precondition", None, None, None, "pass",
        f"sub.txt readable ({sub_count} rows), num.txt readable ({num_count} rows)",
    )

    # 1.3 DQX engine construction -- known friction: hangs without a reachable
    # Databricks workspace rather than failing fast. Not worked around here.
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.labs.dqx.engine import DQEngine
        dqx = DQEngine(WorkspaceClient())
    except Exception as e:  # noqa: BLE001 - engine construction failure is unevaluable
        yield _precondition(
            "precondition", None, None, None, "unevaluable",
            f"DQX engine construction failed: {e}",
        )
        return

    try:
        from databricks.labs.dqx.rule import DQRowRule
        from databricks.labs.dqx import check_funcs
    except Exception as e:  # noqa: BLE001 - import failure is unevaluable
        yield _precondition(
            "precondition", None, None, None, "unevaluable",
            f"DQX check API import failed: {e}",
        )
        return

    tables = {"sub": sub_df, "num": num_df}

    for check in CHECKS:
        name = check["name"]
        table = check["table"]
        column = check["column"]
        check_func_name = check["check_func"]
        df = tables[table]

        try:
            check_func = getattr(check_funcs, check_func_name)
            # Always split at DQX criticality "error" so violations land in bad_df
            # and stay countable; the CHECKS-level criticality is applied to the
            # OUTCOME below (warn = measured, never gates). DQX-native "warn"
            # criticality would route violations into _warnings inside good_df,
            # out of the split counts.
            rule = DQRowRule(
                name=name,
                check_func=check_func,
                column=column,
                criticality="error",
            )
        except Exception as e:  # noqa: BLE001 - rule build failure is unevaluable
            yield _precondition(
                name, table, column, check_func_name, "unevaluable",
                f"check rule construction failed: {e}",
            )
            continue

        try:
            good_df, bad_df = dqx.apply_checks_and_split(df, [rule])
        except Exception as e:  # noqa: BLE001 - engine execution failure is unevaluable
            yield _precondition(
                name, table, column, check_func_name, "unevaluable",
                f"check execution failed: {e}",
            )
            continue

        # 1.5 skip-shaped detection: inspect _errors for a skipped=True entry
        # naming this check, before trusting good/bad counts (a skip and a
        # genuine 100%-fail both look like good=0/bad=total).
        try:
            from pyspark.sql import functions as F

            skipped_rows = (
                bad_df.filter(F.col("_errors").isNotNull())
                .select(F.explode(F.col("_errors")).alias("e"))
                .filter((F.col("e.name") == name) & (F.col("e.skipped") == True))  # noqa: E712
                .select("e.message")
                .limit(1)
                .collect()
            )
        except Exception as e:  # noqa: BLE001 - inspection failure is unevaluable
            yield _precondition(
                name, table, column, check_func_name, "unevaluable",
                f"check execution failed: {e}",
            )
            continue

        if skipped_rows:
            yield _precondition(
                name, table, column, check_func_name, "unevaluable",
                skipped_rows[0]["message"],
            )
            continue

        try:
            good = good_df.count()
            bad = bad_df.count()
        except Exception as e:  # noqa: BLE001 - count failure is unevaluable
            yield _precondition(
                name, table, column, check_func_name, "unevaluable",
                f"check execution failed: {e}",
            )
            continue

        total = good + bad
        if bad > 0:
            outcome = "warn" if check["criticality"] == "warn" else "fail"
        else:
            outcome = "pass"
        detail = f"{bad} bad rows out of {total}"
        result = _precondition(name, table, column, check_func_name, outcome, detail)
        result["total"] = total
        result["good"] = good
        result["bad"] = bad
        yield result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True,
                     help="Directory containing sub.txt and num.txt")
    ap.add_argument("--quarter", default=None,
                     help="Label only; derived from --data if omitted")
    ap.add_argument("--json", default=None,
                     help="Also write machine-readable results here")
    args = ap.parse_args()
    data = Path(args.data)
    quarter = args.quarter or data.name

    results = list(run_checks(data))
    for r in results:
        print(f"[{r['outcome'].upper():11s}] {r['name']}: {r['detail']}")

    outcomes = {r["outcome"] for r in results}
    if "unevaluable" in outcomes:
        outcome, code = "UNEVALUABLE (halt -- not coerced into pass or fail)", 2
    elif "fail" in outcomes:
        outcome, code = "FAIL", 1
    elif "warn" in outcomes:
        outcome, code = "PASS (with warn-level findings -- counted, not gating)", 0
    else:
        outcome, code = "PASS", 0
    print(f"dqx_bronze_gate data={data.as_posix()} quarter={quarter} "
          f"outcome={outcome} exit={code}")

    if args.json:
        Path(args.json).write_text(
            json.dumps(
                {
                    "data": data.as_posix(),
                    "quarter": quarter,
                    "outcome": outcome.split()[0].lower(),
                    "exit": code,
                    "checks": results,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    return code


if __name__ == "__main__":
    # Serverless spark_python_task runs this under an IPython-style harness that
    # treats ANY SystemExit -- including SystemExit(0) -- as a workload failure
    # (observed 2026-07-22: green gate, task FAILED with "SystemExit: 0").
    # So: return normally on 0; raise only for red (1) / unevaluable (2), where
    # a failed task IS the fail-closed contract. Local CLI behavior unchanged.
    _code = main()
    if _code != 0:
        sys.exit(_code)
