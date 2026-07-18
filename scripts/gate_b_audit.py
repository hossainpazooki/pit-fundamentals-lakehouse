"""Gate B audit — data-facing PIT invariants over a lake's gold table (Delta snapshot).

The WAP audit component that runs against DATA (the code-facing component is `sbt test`).
Parameterized by --lake so the SAME audit runs on the staged candidate and on a mutated
twin: an audit that has never been seen red on known-bad input is unevaluable evidence
(ADR-0005 res. 3 shape; `data-quality-fail-closed`).

Three-outcome, fail-closed:
  exit 0  every check PASS
  exit 1  at least one check FAIL (audit red)
  exit 2  at least one check UNEVALUABLE (missing table/column, zero rows, query error)
          -- unevaluable HALTS; it is never coerced into pass or fail.

Checks (gold: cik,tag,ddate,qtrs,uom natural key; valid_from/valid_to intervals):
  A1 no-lookahead      no row with valid_from < accepted (a value visible before its
                       filing was accepted is lookahead -- the core PIT property)
  A2 interval-shape    no INVERTED interval (valid_to < valid_from). Empty intervals
                       (valid_to = valid_from) are legitimate: a filing superseded at
                       its own accepted instant (accepted tie loser / same-instant
                       restatement) is never visible to any as-of query.
  A2b empty-superseded every empty interval has a same-instant NON-empty sibling (the
                       winner); an orphaned empty interval is an anomaly.
  A3 no-overlap        per key, non-empty intervals never overlap (zero-measure
                       intervals overlap nothing and are excluded)
  A4 one-open-interval per key, exactly one open (sentinel valid_to) interval
  A5 no-dup-fact       no duplicate (key, valid_from, adsh) row
  A6 registry-sane     registry batch_ids unique, one batch per quarter, count > 0

Usage: python scripts/gate_b_audit.py --lake <lake-root> [--json <out.json>]
Requires: pip install duckdb (delta extension auto-installed on first use).
"""

import argparse
import json
import sys
from pathlib import Path

import duckdb

KEY = "cik, tag, ddate, qtrs, uom"
OPEN_SENTINEL = "TIMESTAMP '9999-01-01 00:00:00'"  # open intervals use year-9999 valid_to
REQUIRED_GOLD_COLS = {
    "cik", "tag", "ddate", "qtrs", "uom", "value",
    "accepted", "adsh", "version", "valid_from", "valid_to",
}


def delta(path: Path) -> str:
    return f"delta_scan('{path.as_posix()}')"


def run_checks(lake: Path):
    """Yield (check, verdict, detail) with verdict in PASS | FAIL | UNEVALUABLE."""
    con = duckdb.connect()
    gold = lake / "gold"
    registry = lake / "registry"

    # Evaluability preconditions -- fail closed, never coerce into pass/fail.
    if not (gold / "_delta_log").exists():
        yield ("precondition", "UNEVALUABLE", f"no Delta table at {gold}")
        return
    try:
        cols = {
            r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {delta(gold)}").fetchall()
        }
    except Exception as e:  # noqa: BLE001 - any read failure is unevaluable, not red
        yield ("precondition", "UNEVALUABLE", f"gold unreadable: {e}")
        return
    missing = REQUIRED_GOLD_COLS - cols
    if missing:
        yield ("precondition", "UNEVALUABLE", f"gold missing columns: {sorted(missing)}")
        return
    n = con.execute(f"SELECT count(*) FROM {delta(gold)}").fetchone()[0]
    if n == 0:
        yield ("precondition", "UNEVALUABLE", "gold has zero rows -- nothing to audit")
        return
    yield ("precondition", "PASS", f"gold readable, {n} active rows")

    def count_check(name, sql, describe):
        try:
            bad = con.execute(sql).fetchone()[0]
        except Exception as e:  # noqa: BLE001
            yield (name, "UNEVALUABLE", f"query failed: {e}")
            return
        yield (name, "PASS" if bad == 0 else "FAIL", f"{describe}: {bad}")

    yield from count_check(
        "A1 no-lookahead",
        f"SELECT count(*) FROM {delta(gold)} WHERE valid_from < accepted",
        "rows visible before their filing was accepted",
    )
    yield from count_check(
        "A2 interval-shape",
        f"SELECT count(*) FROM {delta(gold)} WHERE valid_to < valid_from",
        "rows with inverted validity interval",
    )
    yield from count_check(
        "A2b empty-superseded",
        f"""
        WITH empty AS (
          SELECT {KEY}, valid_from, adsh FROM {delta(gold)} WHERE valid_to = valid_from
        )
        SELECT count(*) FROM empty e WHERE NOT EXISTS (
          SELECT 1 FROM {delta(gold)} w
          WHERE w.cik = e.cik AND w.tag = e.tag AND w.ddate = e.ddate
            AND w.qtrs = e.qtrs AND w.uom = e.uom
            AND w.valid_from = e.valid_from AND w.adsh <> e.adsh
            AND w.valid_to > w.valid_from
        )
        """,
        "empty intervals with no same-instant winning sibling",
    )
    yield from count_check(
        "A3 no-overlap",
        f"""
        WITH ordered AS (
          SELECT {KEY}, valid_from, valid_to,
                 lead(valid_from) OVER (PARTITION BY {KEY} ORDER BY valid_from) AS nxt
          FROM {delta(gold)} WHERE valid_to > valid_from
        )
        SELECT count(*) FROM ordered WHERE nxt IS NOT NULL AND valid_to > nxt
        """,
        "adjacent non-empty intervals that overlap within a key",
    )
    yield from count_check(
        "A4 one-open-interval",
        f"""
        WITH per_key AS (
          SELECT {KEY},
                 sum(CASE WHEN valid_to >= {OPEN_SENTINEL} THEN 1 ELSE 0 END) AS open_n
          FROM {delta(gold)} GROUP BY {KEY}
        )
        SELECT count(*) FROM per_key WHERE open_n <> 1
        """,
        "keys without exactly one open interval",
    )
    yield from count_check(
        "A5 no-dup-fact",
        f"""
        SELECT coalesce(sum(c - 1), 0) FROM (
          SELECT count(*) AS c FROM {delta(gold)}
          GROUP BY {KEY}, valid_from, adsh HAVING count(*) > 1
        )
        """,
        "duplicate (key, valid_from, adsh) rows",
    )

    if not (registry / "_delta_log").exists():
        yield ("A6 registry-sane", "UNEVALUABLE", f"no Delta table at {registry}")
        return
    try:
        batches, uniq_ids, uniq_q = con.execute(
            f"""
            SELECT count(*), count(DISTINCT batch_id),
                   count(DISTINCT json_extract_string(manifest_json, '$."param.quarter"'))
            FROM {delta(registry)}
            """
        ).fetchone()
    except Exception as e:  # noqa: BLE001
        yield ("A6 registry-sane", "UNEVALUABLE", f"query failed: {e}")
        return
    ok = batches > 0 and uniq_ids == batches and uniq_q == batches
    yield (
        "A6 registry-sane",
        "PASS" if ok else "FAIL",
        f"batches={batches} distinct_ids={uniq_ids} distinct_quarters={uniq_q}",
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", required=True, help="lake root containing gold/ and registry/")
    ap.add_argument("--json", help="also write machine-readable results here")
    args = ap.parse_args()
    lake = Path(args.lake)

    results = list(run_checks(lake))
    for check, verdict, detail in results:
        print(f"[{verdict:11s}] {check}: {detail}")

    verdicts = {v for _, v, _ in results}
    if "UNEVALUABLE" in verdicts:
        outcome, code = "UNEVALUABLE (halt -- not coerced into pass or fail)", 2
    elif "FAIL" in verdicts:
        outcome, code = "RED", 1
    else:
        outcome, code = "GREEN", 0
    print(f"gate_b_audit lake={lake.as_posix()} outcome={outcome} exit={code}")

    if args.json:
        Path(args.json).write_text(
            json.dumps(
                {
                    "lake": lake.as_posix(),
                    "outcome": outcome.split()[0],
                    "exit": code,
                    "checks": [
                        {"check": c, "verdict": v, "detail": d} for c, v, d in results
                    ],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    return code


if __name__ == "__main__":
    sys.exit(main())
