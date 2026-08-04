"""Mutate a TWIN copy of the gold table so assert_as_of_key_unique can be seen red.

Same res.-3 shape as scripts/gate_b_mutate_twin.py (an audit/test never seen red
on known-bad input is unevaluable evidence) but a DIFFERENT seam: this script
plants a DOUBLE-VISIBILITY violation instead of lookahead.

It finds natural keys (cik, tag, ddate, qtrs, uom) that have more than one
physical version in the twin's active gold snapshot -- i.e. restatement
history -- and for N of them, overwrites the SUPERSEDED row's `valid_to` with
the currently-live row's `valid_to` value for that same key (in this lake that
live value is the open sentinel, 9999-12-31, however it happens to be encoded
physically -- see note below). After the mutation, both the live row and the
reopened superseded row are visible at the same as-of instant, which is
precisely what assert_as_of_key_unique (grain: the 5-column natural key, at
one as-of slice) forbids.

Value-copy, not reconstruction: rather than recomputing the literal sentinel
'9999-12-31 05:00:00-05' by hand -- which risks a timezone/encoding mismatch
against however Spark actually wrote it (int96, session-timezone-dependent) --
this script copies the EXACT raw `valid_to` value already sitting in the
live/most-current row of the same key group into the superseded row's slot.
That is guaranteed to decode identically to "still open" because it IS an
open row's real value, byte for byte.

int96 MUST be coerced to us on read: the open-interval sentinel (year 9999)
overflows timestamp[ns] and would crash the read outright.

The twin's _delta_log add entries are patched with each rewritten file's new
size so the twin stays a well-formed Delta table; the mutation is in the
DATA, not the log.

Safety: refuses to run unless the twin-lake path contains 'twin'. Never reads
from or writes to anything under --source-lake except to seed a missing twin
via copytree.

Usage: python scripts/dbt_mutate_twin.py --source-lake <src-root> \
           --twin-lake <twin-root> [--violations 5] [--manifest <out.json>]
"""

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
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
    ap.add_argument("--source-lake", required=True)
    ap.add_argument("--twin-lake", required=True)
    ap.add_argument("--violations", type=int, default=5)
    ap.add_argument("--manifest", help="write a mutation manifest here")
    args = ap.parse_args()

    twin = Path(args.twin_lake)
    if "twin" not in twin.as_posix().lower():
        print(f"REFUSING: {twin} does not look like a twin copy (no 'twin' in path)")
        return 3

    source = Path(args.source_lake)
    if not twin.exists():
        src_gold = source / "gold"
        twin_gold = twin / "gold"
        if not src_gold.exists():
            print(f"REFUSING: source gold table not found at {src_gold}")
            return 3
        print(f"twin-lake {twin} does not exist -- seeding from {src_gold}")
        shutil.copytree(src_gold, twin_gold)

    gold = twin / "gold"
    log_dir = gold / "_delta_log"
    live = active_files(log_dir)
    if not live:
        print("REFUSING: no active files found in twin gold _delta_log")
        return 3

    # Load every active file once. Keep the pyarrow Table per file so a file
    # touched by more than one selected violation is only read/written once.
    tables = {}
    for rel in live:
        tables[rel] = pq.read_table(gold / rel, coerce_int96_timestamp_unit="us")

    # Build the natural-key index across ALL active files (a key's versions
    # need not live in the same physical file).
    key_groups = {}
    for rel, table in tables.items():
        cik = table.column("cik")
        tag = table.column("tag")
        ddate = table.column("ddate")
        qtrs = table.column("qtrs")
        uom = table.column("uom")
        valid_to = table.column("valid_to")
        for i in range(table.num_rows):
            key = (
                cik[i].as_py(),
                tag[i].as_py(),
                ddate[i].as_py(),
                qtrs[i].as_py(),
                uom[i].as_py(),
            )
            key_groups.setdefault(key, []).append((rel, i, valid_to[i].as_py()))

    multi_version = {k: v for k, v in key_groups.items() if len(v) > 1}
    if not multi_version:
        print(
            "REFUSING: no natural key in the twin's active gold snapshot has more "
            "than one version -- there is no restatement history to plant a "
            "double-visibility violation on. Negative control cannot be established."
        )
        return 3

    n_requested = args.violations
    selected_keys = sorted(multi_version.keys())[:n_requested]
    if len(selected_keys) < n_requested:
        print(
            f"NOTE: only {len(selected_keys)} natural key(s) with >1 version exist "
            f"in the twin (requested {n_requested}); planting {len(selected_keys)}."
        )

    # For each selected key: keep = the row with the max valid_to (the
    # currently-live version), victim = a different row in the same key group
    # (the most recently superseded one, i.e. second-largest valid_to). Copy
    # keep's raw valid_to value onto victim -- no timestamp arithmetic.
    # victims_by_file: rel -> list of (row_idx, new_valid_to_value)
    victims_by_file = {}
    planted = []
    for key in selected_keys:
        rows = key_groups[key]
        rows_sorted = sorted(rows, key=lambda r: r[2], reverse=True)
        keep_rel, keep_idx, keep_valid_to = rows_sorted[0]
        victim_rel, victim_idx, victim_valid_to_before = rows_sorted[1]
        victims_by_file.setdefault(victim_rel, []).append((victim_idx, keep_valid_to))
        planted.append(
            {
                "cik": key[0],
                "tag": key[1],
                "ddate": key[2],
                "qtrs": key[3],
                "uom": key[4],
                "version_count": len(rows),
                "victim_file": victim_rel,
                "victim_row": victim_idx,
                "victim_valid_to_before": str(victim_valid_to_before),
                "victim_valid_to_after": str(keep_valid_to),
                "keep_file": keep_rel,
                "keep_row": keep_idx,
            }
        )

    files_rewritten = []
    for rel, victims in victims_by_file.items():
        table = tables[rel]
        idx = table.schema.get_field_index("valid_to")
        col_type = table.column("valid_to").type
        values = table.column("valid_to").to_pylist()
        for row_idx, new_value in victims:
            values[row_idx] = new_value
        new_col = pa.array(values, type=col_type)
        mutated = table.set_column(idx, "valid_to", new_col)
        target = gold / rel
        pq.write_table(mutated, target)
        after_size = target.stat().st_size

        commit_file = live[rel]
        lines = commit_file.read_text(encoding="utf-8").splitlines()
        patched = False
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            action = json.loads(line)
            if "add" in action and action["add"]["path"] == rel:
                action["add"]["size"] = after_size
                lines[i] = json.dumps(action, separators=(",", ":"))
                patched = True
        if not patched:
            print(f"ERROR: add entry for {rel} not found in {commit_file.name}")
            return 3
        commit_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        files_rewritten.append(rel)

    n_planted = len(planted)
    record = {
        "mutated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_lake": source.as_posix(),
        "twin_lake": twin.as_posix(),
        "violations_requested": n_requested,
        "keys_with_multiple_versions_available": len(multi_version),
        "natural_keys_made_doubly_visible": n_planted,
        "files_rewritten": files_rewritten,
        "mutation": (
            "for each planted key, the superseded row's valid_to was overwritten "
            "with the currently-live row's valid_to for that same natural key "
            "(raw value copy, not a computed sentinel) -> two versions of the "
            "same (cik, tag, ddate, qtrs, uom) are simultaneously visible"
        ),
        "planted": planted,
        "expected_test_outcome": (
            f"assert_as_of_key_unique FAIL with row count == {n_planted}"
        ),
    }
    print(json.dumps(record, indent=2))
    if args.manifest:
        Path(args.manifest).write_text(json.dumps(record, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
