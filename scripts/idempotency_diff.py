"""Idempotency refute: after rerunning ONE quarter + a second GoldRebuild, diff the lake
against its pre-rerun Delta versions (time-travel — no snapshot copies).

Must be IDENTICAL: bronze count (rerun MERGE inserts 0); registry count (MERGE update, not
insert); silver count AND full-table content fingerprint EXCLUDING the `_ingest_ts` audit
column (the rerun's whenMatched legitimately refreshes it); gold count AND full fingerprint
(gold carries no audit columns). Content fingerprint = COUNT + SUM(hash(row)) over a lazy
scan of both versions — a whole-table check, stronger than a batch-scoped anti-join.
Legitimately DIFFERENT, asserted exactly: quarantine `/rows` grows by exactly the rerun
quarter's quarantined count (append-mode audit lane, by design).

Usage: python scripts/idempotency_diff.py --pre-silver 62 --pre-gold 0 \
           --pre-bronze-count 181351169 --pre-registry-count 69 \
           --pre-quarantine-count 2926646 --rerun-quarantined 41517
"""

import argparse
import json
import sys
from pathlib import Path

import duckdb
from deltalake import DeltaTable

FAILS = []


def check(name, got, want):
    ok = got == want
    print(f"{'OK ' if ok else 'FAIL'} {name}: got={got} want={want}")
    if not ok:
        FAILS.append(name)


def fingerprint(con, source, cols):
    expr = ", ".join(cols)
    n, h = con.execute(
        f"SELECT COUNT(*), SUM(hash({expr})) FROM {source}"
    ).fetchone()
    return n, h


def versioned(con, path, version, name):
    import pyarrow.dataset as pads

    # Spark writes INT96 timestamps; gold's 9999-12-31 MAX_SENTINEL overflows an ns read.
    ds = DeltaTable(path, version=version).to_pyarrow_dataset(
        parquet_read_options=pads.ParquetReadOptions(coerce_int96_timestamp_unit="us")
    )
    con.register(name, ds)
    return name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="C:/Users/hossa/dev/vantage-data")
    ap.add_argument("--lake", default="lake-backfill")
    ap.add_argument("--pre-silver", type=int, required=True)
    ap.add_argument("--pre-gold", type=int, required=True)
    ap.add_argument("--pre-bronze-count", type=int, required=True)
    ap.add_argument("--pre-registry-count", type=int, required=True)
    ap.add_argument("--pre-quarantine-count", type=int, required=True)
    ap.add_argument("--rerun-quarantined", type=int, required=True)
    args = ap.parse_args()
    L = f"{Path(args.data_root).as_posix()}/{args.lake}"

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    check("bronze count unchanged",
          con.execute(f"SELECT COUNT(*) FROM delta_scan('{L}/bronze/num')").fetchone()[0],
          args.pre_bronze_count)
    check("registry count unchanged",
          con.execute(f"SELECT COUNT(*) FROM delta_scan('{L}/registry')").fetchone()[0],
          args.pre_registry_count)
    check("quarantine /rows grew by EXACTLY the rerun quarter's quarantined count",
          con.execute(f"SELECT COUNT(*) FROM delta_scan('{L}/quarantine/rows')").fetchone()[0],
          args.pre_quarantine_count + args.rerun_quarantined)

    silver_cols = [c for c, in con.execute(
        f"SELECT column_name FROM (DESCRIBE SELECT * FROM delta_scan('{L}/silver'))").fetchall()
        if c != "_ingest_ts"]
    pre = fingerprint(con, versioned(con, f"{L}/silver", args.pre_silver, "silver_pre"), silver_cols)
    cur = fingerprint(con, f"delta_scan('{L}/silver')", silver_cols)
    check("silver count (ex-rerun) unchanged", cur[0], pre[0])
    check("silver content fingerprint (ALL columns except _ingest_ts) unchanged", cur[1], pre[1])

    gold_cols = [c for c, in con.execute(
        f"SELECT column_name FROM (DESCRIBE SELECT * FROM delta_scan('{L}/gold'))").fetchall()]
    # BOTH sides through the same reader: duckdb delta_scan and the pyarrow versioned path
    # represent timestamps differently, and gold hashes three timestamp columns — an
    # asymmetric comparison flags a false diff (found live on the 2026-07-08 run).
    pre_g = fingerprint(con, versioned(con, f"{L}/gold", args.pre_gold, "gold_pre"), gold_cols)
    cur_version = DeltaTable(f"{L}/gold").version()
    cur_g = fingerprint(con, versioned(con, f"{L}/gold", cur_version, "gold_cur"), gold_cols)
    check("gold count unchanged across rebuild", cur_g[0], pre_g[0])
    check("gold content fingerprint (all columns) unchanged", cur_g[1], pre_g[1])

    # The rerun's silver MERGE commit, from the _delta_log artifact: inserted 0, updated all.
    log_dir = Path(args.data_root) / args.lake / "silver" / "_delta_log"
    commit = json.loads([line for line in
                         (log_dir / f"{args.pre_silver + 1:020d}.json").read_text().splitlines()
                         if '"commitInfo"' in line][0])["commitInfo"]
    m = commit.get("operationMetrics", {})
    check("rerun silver MERGE inserted rows", int(m.get("numTargetRowsInserted", -1)), 0)
    print(f"     rerun silver MERGE matched-updated rows: {m.get('numTargetRowsMatchedUpdated')} "
          f"(the _ingest_ts refresh — legitimate)")

    if FAILS:
        print(f"\nIDEMPOTENCY REFUTE FAILED: {FAILS}")
        sys.exit(1)
    print("\nIDEMPOTENCY: rerun changed nothing but the audit column, quarantine lane growth "
          "exact, gold rebuild reproduced identical content")


if __name__ == "__main__":
    main()
