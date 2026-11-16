"""Stream/file equivalence oracle (WS3): is a Kafka-fed lake the same lake as a file-fed one?

Compares two lake roots built from the SAME quarter by the two ingest routes:
  E1 bronze-row-hash  bronze/num `_src_row_hash`: row count, distinct count and a
                      whole-table hash sum must be identical. The row hash covers the
                      declared source columns only, so it is route-independent; batch
                      ids and ingest timestamps legitimately differ and are not compared.
  E2 gold-content     gold: identical column list, row count and whole-table content
                      hash sum over every column.

Three-outcome, fail-closed (same contract as gate_b_audit.py):
  exit 0  every check PASS
  exit 1  at least one check FAIL
  exit 2  UNEVALUABLE: a table is missing or empty, the gold column lists differ, or a
          query errors. Unevaluable halts; it is never coerced into pass or fail.

Output names tables and counts only -- never a filesystem path -- so it can be pasted
into a tracked evidence doc as is.

Usage: python scripts/stream_equivalence.py --stream-lake <root> --file-lake <root>
Requires: pip install duckdb (delta extension auto-installed on first use).
"""

import argparse
import sys
from pathlib import Path

import duckdb


def delta(path: Path) -> str:
    return f"delta_scan('{path.as_posix()}')"


def columns(con, path: Path):
    return sorted(r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {delta(path)}").fetchall())


def run_checks(stream: Path, file: Path):
    """Yield (check, verdict, detail) with verdict in PASS | FAIL | UNEVALUABLE."""
    con = duckdb.connect()
    for table in ("bronze/num", "gold"):
        for label, lake in (("stream", stream), ("file", file)):
            if not (lake / table / "_delta_log").exists():
                yield ("precondition", "UNEVALUABLE", f"no Delta table: {label} {table}")
                return

    sql = "SELECT COUNT(*), COUNT(DISTINCT _src_row_hash), SUM(hash(_src_row_hash)) FROM {}"
    try:
        s = con.execute(sql.format(delta(stream / "bronze" / "num"))).fetchone()
        f = con.execute(sql.format(delta(file / "bronze" / "num"))).fetchone()
    except Exception as exc:  # fail closed: a query error is not a pass and not a fail
        yield ("E1 bronze-row-hash", "UNEVALUABLE", f"query error: {type(exc).__name__}")
        return
    if s[0] == 0 or f[0] == 0:
        yield ("E1 bronze-row-hash", "UNEVALUABLE", f"zero rows: stream={s[0]} file={f[0]}")
        return
    detail = (f"stream rows={s[0]} distinct={s[1]} sum={s[2]} | "
              f"file rows={f[0]} distinct={f[1]} sum={f[2]}")
    yield ("E1 bronze-row-hash", "PASS" if tuple(s) == tuple(f) else "FAIL", detail)

    try:
        s_cols = columns(con, stream / "gold")
        f_cols = columns(con, file / "gold")
        if s_cols != f_cols:
            yield ("E2 gold-content", "UNEVALUABLE",
                   f"column lists differ: stream={s_cols} file={f_cols}")
            return
        expr = ", ".join(f'"{c}"' for c in s_cols)
        gsql = f"SELECT COUNT(*), SUM(hash({expr})) FROM {{}}"
        gs = con.execute(gsql.format(delta(stream / "gold"))).fetchone()
        gf = con.execute(gsql.format(delta(file / "gold"))).fetchone()
    except Exception as exc:
        yield ("E2 gold-content", "UNEVALUABLE", f"query error: {type(exc).__name__}")
        return
    if gs[0] == 0 or gf[0] == 0:
        yield ("E2 gold-content", "UNEVALUABLE", f"zero rows: stream={gs[0]} file={gf[0]}")
        return
    detail = f"stream rows={gs[0]} sum={gs[1]} | file rows={gf[0]} sum={gf[1]}"
    yield ("E2 gold-content", "PASS" if tuple(gs) == tuple(gf) else "FAIL", detail)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream-lake", required=True, help="lake root fed by the Kafka route")
    ap.add_argument("--file-lake", required=True, help="lake root fed by the file route")
    args = ap.parse_args()

    verdicts = []
    for check, verdict, detail in run_checks(Path(args.stream_lake), Path(args.file_lake)):
        print(f"{verdict:<11} {check}: {detail}")
        verdicts.append(verdict)
    if "UNEVALUABLE" in verdicts or not verdicts:
        print("stream-equivalence: UNEVALUABLE")
        return 2
    if "FAIL" in verdicts:
        print("stream-equivalence: RED")
        return 1
    print("stream-equivalence: GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
