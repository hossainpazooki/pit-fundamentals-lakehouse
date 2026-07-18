"""Gate B consumer-path probe -- verify a publish by its EFFECT, not its exit log.

Queries a gold table exactly as a consumer would (as-of semantics over the Delta
snapshot: valid_from <= D < valid_to) and passes only if the expected fact comes
back with the expected value. Paired with a negative control run against the
effect-ABSENT state (`verify-the-effect`): a probe that cannot fail when the
effect is absent is vacuous and credits nothing (`check-effect-probe.mjs` is the
mechanization; use --record to emit its record shape).

The expected value must be derived INDEPENDENTLY of the gold being probed (for
the Databricks firing: from the local known-good lake), or the probe is circular.

Exit: 0 probe passed - 1 probe failed (fact absent / wrong value) - 2 unevaluable
(malformed expectation, not a presence/absence signal).

Usage:
  python scripts/gate_b_probe.py --gold <gold-path> --cik 1750 \
      --tag AccruedLiabilitiesCurrent --ddate 20250228 --qtrs 0 --uom USD \
      --as-of "2025-06-01 00:00:00" --expect 236700000.0 \
      [--record probe-records.json --claim "..." --role probe|control]
"""

import argparse
import json
import sys
from pathlib import Path

import duckdb


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold", required=True)
    ap.add_argument("--cik", type=int, required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--ddate", type=int, required=True)
    ap.add_argument("--qtrs", type=int, required=True)
    ap.add_argument("--uom", required=True)
    ap.add_argument("--as-of", dest="as_of", required=True)
    ap.add_argument("--expect", type=float, required=True)
    ap.add_argument("--record", help="append a check-effect-probe record to this JSON file")
    ap.add_argument("--claim", default="gate-b publish visible on consumer path")
    ap.add_argument("--role", choices=["probe", "control"], default="probe")
    args = ap.parse_args()

    gold = Path(args.gold)
    passed = False
    detail = ""
    try:
        if not (gold / "_delta_log").exists():
            raise FileNotFoundError(f"no Delta table at {gold} (effect-absent state?)")
        con = duckdb.connect()
        rows = con.execute(
            f"""
            SELECT value, CAST(valid_from AS VARCHAR), CAST(valid_to AS VARCHAR), adsh
            FROM delta_scan('{gold.as_posix()}')
            WHERE cik = ? AND tag = ? AND ddate = ? AND qtrs = ? AND uom = ?
              AND valid_from <= TIMESTAMP '{args.as_of}'
              AND valid_to   >  TIMESTAMP '{args.as_of}'
            """,
            [args.cik, args.tag, args.ddate, args.qtrs, args.uom],
        ).fetchall()
        if len(rows) == 1 and float(rows[0][0]) == args.expect:
            passed = True
            detail = (f"as_of({args.as_of}) -> value={rows[0][0]} adsh={rows[0][3]} "
                      f"valid=[{rows[0][1]}, {rows[0][2]})")
        else:
            detail = (f"as_of({args.as_of}) -> {len(rows)} row(s), "
                      f"values={[str(r[0]) for r in rows]}, expected exactly one == {args.expect}")
    except Exception as e:  # noqa: BLE001 - absent table IS the control's expected shape
        detail = f"consumer path unreadable: {e}"

    print(f"[{'PASS' if passed else 'FAIL'}] {args.role} gold={gold.as_posix()}")
    print(f"  {detail}")

    if args.record:
        rec_path = Path(args.record)
        records = json.loads(rec_path.read_text(encoding="utf-8")) if rec_path.exists() else []
        entry = next((r for r in records if r["claim"] == args.claim), None)
        if entry is None:
            entry = {"claim": args.claim, "probePassed": None,
                     "controlRan": False, "controlPassed": None}
            records.append(entry)
        if args.role == "probe":
            entry["probePassed"] = passed
            entry["probeDetail"] = detail
        else:
            entry["controlRan"] = True
            entry["controlPassed"] = passed
            entry["controlDetail"] = detail
        rec_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
