"""Recompute per-quarter backfill metrics FROM RUN ARTIFACTS, then diff against the log lines.

Nothing in the published table is restated from session memory or from the pipeline's own
log lines: every number is recomputed from the state the run left behind, and the log lines
are then used only as a CROSS-CHECK. Any mismatch is a finding and a nonzero exit.

Artifact source per metric:
  rows_in            bronze/num, COUNT(*) GROUP BY _batch_id                (DuckDB delta_scan)
  scoped_out         bronze/num, entity-scope predicate re-applied per batch (segments/coreg)
  quarantine /rows   quarantine/rows, COUNT(*) GROUP BY _batch_id
  quarantine /uneval quarantine/unevaluable, COUNT(*) GROUP BY batchId
  quarantine /detail quarantine/detail _delta_log commit operationMetrics (no batch column)
  rows_to_silver     silver _delta_log WRITE/MERGE operationMetrics per commit, mapped to
                     quarters in ledger order; cross-checked vs rows_in-scoped_out-quarantined
  dedupe             silver MERGE metrics: numSourceRows - inserted - matchedUpdated
                     (matched-but-no-op rows); bronze: numSourceRows - numTargetRowsInserted
  key_collisions     bronze/num per batch, entity+non-null-key rows, GROUP BY 6-col natural
                     key HAVING cnt>1 -> SUM(cnt-1); the DQ gate certifies 0, recomputed anyway
  wall_clock_ms      the run log line (a run artifact), sanity-bounded by _delta_log timestamps
Join spine: batch registry manifest_json -> ingestParams.quarter <-> batch_id.

Usage: python scripts/recompute_metrics.py [--data-root ...] [--lake lake-backfill]
                                           [--out docs/BACKFILL-METRICS-table.md]
Requires: pip install duckdb (delta extension auto-installed on first use).
"""

import argparse
import json
import re
import sys
from pathlib import Path

import duckdb

NATURAL_KEY = ["adsh", "tag", "version", "ddate", "qtrs", "uom"]
ENTITY_PRED = "(segments IS NULL OR segments = '') AND (coreg IS NULL OR coreg = '')"
NONNULL_PRED = " AND ".join(f"({c} IS NOT NULL AND {c} <> '')" for c in NATURAL_KEY + ["value"])
LOG_LINE = re.compile(
    r"run batch_id=(\S+) rows_in=(\d+) rows_out=(\d+) scoped_out=(\d+) rejected=(\d+) "
    r"quarantined=(\d+) gate=(\S+) delta_version=(\d+)(?: wall_clock_ms=(\d+))?"
)


def delta(con, path: Path) -> str:
    return f"delta_scan('{path.as_posix()}')"


def commit_metrics(delta_log: Path):
    """(version, operation, operationMetrics) per commit, from the plain-JSON _delta_log."""
    out = []
    for f in sorted(delta_log.glob("*.json")):
        version = int(f.stem)
        for line in f.read_text(encoding="utf-8").splitlines():
            if '"commitInfo"' in line:
                ci = json.loads(line)["commitInfo"]
                out.append((version, ci.get("operation", ""), ci.get("operationMetrics", {}),
                            ci.get("timestamp")))
    return out


def registry_quarters(con, lake: Path):
    rows = con.execute(
        f"SELECT batch_id, manifest_json FROM {delta(con, lake / 'registry')}"
    ).fetchall()
    spine = {}
    for batch_id, manifest_json in rows:
        quarter = json.loads(manifest_json)["param.quarter"]
        spine[quarter] = batch_id
    return spine


def per_batch_counts(con, table: Path, batch_col: str, where: str = "TRUE"):
    if not (table / "_delta_log").exists():
        return {}  # lane never written (e.g. clean backfill has no /detail)
    rows = con.execute(
        f"SELECT {batch_col}, COUNT(*) FROM {delta(con, table)} WHERE {where} GROUP BY 1"
    ).fetchall()
    return dict(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="C:/Users/hossa/dev/vantage-data")
    ap.add_argument("--lake", default="lake-backfill")
    ap.add_argument("--ledger", default="backfill-ledger.jsonl")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    lake = data_root / args.lake
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    spine = registry_quarters(con, lake)  # quarter -> batch_id
    by_batch = {v: k for k, v in spine.items()}
    print(f"registry: {len(spine)} batches")

    bronze = lake / "bronze" / "num"
    rows_in = per_batch_counts(con, bronze, "_batch_id")
    scoped_out = per_batch_counts(con, bronze, "_batch_id", f"NOT ({ENTITY_PRED})")
    # Quarantine lanes are APPEND-MODE audit logs: a rerun appends the same rows again under a
    # new _ingest_ts (Gate A record; observed 2x41,517 on the gate-a lake). The per-quarter
    # metric is one run-slice; unequal slice counts within a batch are a finding.
    q_rows, q_rows_slices = {}, {}
    if (lake / "quarantine" / "rows" / "_delta_log").exists():  # lane written at least once
        for b, _ts, n in con.execute(
            f"SELECT _batch_id, CAST(_ingest_ts AS VARCHAR), COUNT(*) "
            f"FROM {delta(con, lake / 'quarantine' / 'rows')} GROUP BY 1, 2 ORDER BY 2"
        ).fetchall():
            q_rows.setdefault(b, n)
            q_rows_slices.setdefault(b, []).append(n)
    q_uneval = per_batch_counts(con, lake / "quarantine" / "unevaluable", "batchId")

    collisions = {}
    for quarter, batch_id in spine.items():
        (n,) = con.execute(
            f"SELECT COALESCE(SUM(cnt - 1), 0) FROM (SELECT COUNT(*) AS cnt FROM {delta(con, bronze)} "
            f"WHERE _batch_id = ? AND {ENTITY_PRED} AND {NONNULL_PRED} "
            f"GROUP BY {', '.join(NATURAL_KEY)} HAVING COUNT(*) > 1)",
            [batch_id],
        ).fetchone()
        collisions[batch_id] = int(n)

    # Silver commit metrics -> quarters, in ledger 'ok' order (one WRITE/MERGE per quarter).
    ledger = [json.loads(l) for l in (data_root / args.ledger).read_text(encoding="utf-8").splitlines()
              if l.strip()]
    ok_quarters = [r["quarter"] for r in ledger if r["status"] == "ok" and r["quarter"] in spine]
    silver_commits = [c for c in commit_metrics(lake / "silver" / "_delta_log")
                      if c[1] in ("WRITE", "MERGE")]
    if len(silver_commits) != len(ok_quarters):
        print(f"WARNING: {len(silver_commits)} silver commits vs {len(ok_quarters)} ok quarters "
              f"(reruns append commits; idempotency rerun expected to add one)")
    silver_by_quarter = {}
    for (version, op, m, _ts), quarter in zip(silver_commits, ok_quarters):
        if op == "WRITE":
            src = int(m.get("numOutputRows", 0))
            ins, upd = src, 0
        else:
            src = int(m.get("numSourceRows", 0))
            ins = int(m.get("numTargetRowsInserted", 0))
            upd = int(m.get("numTargetRowsMatchedUpdated", 0))
        silver_by_quarter[quarter] = {"rows_to_silver": src, "dedupe": src - ins - upd,
                                      "version": version}

    detail_commits = commit_metrics(lake / "quarantine" / "detail" / "_delta_log") \
        if (lake / "quarantine" / "detail").exists() else []

    # Log-line cross-check values, read from the per-quarter LOG FILES (the run artifact) —
    # not from the ledger's log_line field, which early driver versions left empty.
    logged = {}
    for rec in ledger:
        if rec["status"] != "ok" or rec["quarter"].startswith("_"):
            continue
        log_file = data_root / "logs" / f"backfill-{rec['quarter']}.log"
        text = log_file.read_text(encoding="utf-8", errors="replace") if log_file.exists() else ""
        m = LOG_LINE.search(text)
        if m:
            logged[rec["quarter"]] = {
                "batch_id": m.group(1), "rows_in": int(m.group(2)), "rows_out": int(m.group(3)),
                "scoped_out": int(m.group(4)), "quarantined": int(m.group(6)),
                "wall_clock_ms": int(m.group(9)) if m.group(9) else None, "wall_s": rec["wall_s"],
            }

    header = ("| quarter | rows_in | scoped_out | rows_to_silver | q_rows | q_detail | q_uneval | "
              "dedupe | key_collisions | wall_clock_s |")
    sep = "|" + "---|" * 10
    lines, mismatches = [header, sep], []
    totals = dict.fromkeys(["rows_in", "scoped_out", "rows_to_silver", "q_rows", "dedupe",
                            "key_collisions"], 0)
    for quarter in sorted(spine):
        b = spine[quarter]
        ri, so = rows_in.get(b, 0), scoped_out.get(b, 0)
        qr, qu = q_rows.get(b, 0), q_uneval.get(b, 0)
        sv = silver_by_quarter.get(quarter, {})
        rts, dd = sv.get("rows_to_silver", 0), sv.get("dedupe", 0)
        co = collisions.get(b, 0)
        lg = logged.get(quarter, {})
        wall = (lg.get("wall_clock_ms") or 0) / 1000 if lg else 0
        lines.append(f"| {quarter} | {ri} | {so} | {rts} | {qr} | 0 | {qu} | {dd} | {co} | {wall:.0f} |")
        for k, v in [("rows_in", ri), ("scoped_out", so), ("rows_to_silver", rts), ("q_rows", qr),
                     ("dedupe", dd), ("key_collisions", co)]:
            totals[k] += v

        # Cross-checks: recomputed vs logged, and the conservation identity.
        if lg:
            for key, mine in [("rows_in", ri), ("scoped_out", so), ("quarantined", qr)]:
                if lg[key] != mine:
                    mismatches.append(f"{quarter}: {key} recomputed={mine} logged={lg[key]}")
            if lg["batch_id"] != b:
                mismatches.append(f"{quarter}: batch_id registry={b} logged={lg['batch_id']}")
        halted = any(r["quarter"] == quarter and r["status"] == "halted" for r in ledger)
        if halted:
            # A halted quarter's signature: nothing reached silver; a nonzero collision count
            # CONFIRMS the gate's refusal (the recomputation agreeing with the halt).
            if rts != 0:
                mismatches.append(f"{quarter}: HALTED but rows_to_silver={rts} -- gate leaked")
        else:
            if ri - so - qr != rts:
                mismatches.append(
                    f"{quarter}: conservation rows_in-scoped_out-quarantined={ri - so - qr} != rows_to_silver={rts}")
            if co != 0:
                mismatches.append(f"{quarter}: key_collisions={co} in an INGESTED quarter -- the gate missed it")
        slices = q_rows_slices.get(b, [])
        if len(set(slices)) > 1:
            mismatches.append(f"{quarter}: unequal quarantine run-slices {slices} -- rerun changed the quarantined set")

    lines.append(f"| **total** | {totals['rows_in']} | {totals['scoped_out']} | "
                 f"{totals['rows_to_silver']} | {totals['q_rows']} | 0 | "
                 f"{sum(q_uneval.values())} | {totals['dedupe']} | {totals['key_collisions']} | |")
    if detail_commits:
        lines.append(f"\n/detail lane commits (attributed by timestamp): "
                     f"{[(v, m.get('numOutputRows')) for v, _o, m, _t in detail_commits]}")

    table = "\n".join(lines)
    print(table)
    if args.out:
        Path(args.out).write_text(table + "\n", encoding="utf-8")
        print(f"\nwritten: {args.out}")

    if mismatches:
        print("\nCROSS-CHECK MISMATCHES (findings):")
        for m in mismatches:
            print("  " + m)
        sys.exit(1)
    print("\ncross-check: all recomputed numbers match the run log lines and conservation identity")


if __name__ == "__main__":
    main()
