"""Mutate a TWIN copy of the gold table so the Gate B audit can be seen red.

ADR-0005 res.-3 shape: an audit never seen red on known-bad input is unevaluable
evidence. This script corrupts the as-of seam in a twin lake (never the real one):
it picks one ACTIVE gold parquet file and shifts `accepted` forward by N years while
leaving `valid_from` alone -- after which those rows claim visibility (valid_from)
BEFORE their filing was accepted, i.e. lookahead, and audit check A1 must go red.

The twin's _delta_log add entry is patched with the rewritten file's size so the
twin stays a well-formed Delta table; the mutation is in the DATA, not the log.

Safety: refuses to run unless the lake path contains 'twin'.

Usage: python scripts/gate_b_mutate_twin.py --twin-lake <twin-root> \
           [--shift-years 5] [--manifest <out.json>]
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def active_files(delta_log: Path):
    """Replay add/remove actions across commit JSONs -> the live snapshot's files."""
    live = {}
    for f in sorted(delta_log.glob("*.json")):
        for line in f.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            action = json.loads(line)
            if "add" in action:
                live[action["add"]["path"]] = f
            elif "remove" in action:
                live.pop(action["remove"]["path"], None)
    return live  # path -> commit file that added it


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--twin-lake", required=True)
    ap.add_argument("--shift-years", type=int, default=5)
    ap.add_argument("--manifest", help="write a mutation manifest here")
    args = ap.parse_args()
    twin = Path(args.twin_lake)
    if "twin" not in twin.as_posix().lower():
        print(f"REFUSING: {twin} does not look like a twin copy (no 'twin' in path)")
        return 3
    gold = twin / "gold"
    log_dir = gold / "_delta_log"

    live = active_files(log_dir)
    if not live:
        print("REFUSING: no active files found in twin gold _delta_log")
        return 3
    # Smallest active file: cheapest rewrite, same evidentiary force.
    target_rel = min(live, key=lambda p: (gold / p).stat().st_size)
    target = gold / target_rel
    commit_file = live[target_rel]

    # int96 MUST be coerced to us: the open-interval sentinel (year 9999) overflows
    # timestamp[ns] and would corrupt valid_to wholesale -- the twin must differ from
    # the candidate ONLY at the designed seam. Timestamps are then tagged as UTC
    # instants (Delta semantics) so unmutated columns decode to identical instants.
    table = pq.read_table(target, coerce_int96_timestamp_unit="us")
    for col in ("accepted", "valid_from", "valid_to"):
        i = table.schema.get_field_index(col)
        naive = table.column(col)
        utc = naive.cast(pa.timestamp("us", tz="UTC"))
        table = table.set_column(i, col, utc)
    n = table.num_rows
    accepted = table.column("accepted")
    shift_us = args.shift_years * 365 * 24 * 3600 * 1_000_000
    shifted = pc.add(accepted, pa.scalar(shift_us, type=pa.duration("us")))
    before = accepted[0].as_py()
    idx = table.schema.get_field_index("accepted")
    mutated = table.set_column(idx, "accepted", shifted.cast(accepted.type))
    pq.write_table(mutated, target)
    after_size = target.stat().st_size

    # Patch the add entry's size so the twin stays a well-formed Delta table.
    lines = commit_file.read_text(encoding="utf-8").splitlines()
    patched = False
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        action = json.loads(line)
        if "add" in action and action["add"]["path"] == target_rel:
            action["add"]["size"] = after_size
            lines[i] = json.dumps(action, separators=(",", ":"))
            patched = True
    if not patched:
        print(f"ERROR: add entry for {target_rel} not found in {commit_file.name}")
        return 3
    commit_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    record = {
        "mutated_at_utc": datetime.now(timezone.utc).isoformat(),
        "twin_lake": twin.as_posix(),
        "file": target_rel,
        "rows_mutated": n,
        "mutation": f"accepted += {args.shift_years} years; valid_from untouched "
                    "-> valid_from < accepted (lookahead) on every row of this file",
        "accepted_sample_before": str(before),
        "accepted_sample_after": str(mutated.column("accepted")[0].as_py()),
        "expected_audit_outcome": f"A1 no-lookahead FAIL with count == {n}",
    }
    print(json.dumps(record, indent=2))
    if args.manifest:
        Path(args.manifest).write_text(json.dumps(record, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
