"""Twin-red harness for `dqx_bronze_gate.py` -- plants a KNOWN count of malformed
rows in a twin copy of sub.txt so the operator can confirm the gate exits 1
reporting exactly the planted count.

Stdlib-only (no pyspark/DQX dependency): copies the real quarter TSVs to a twin
path, then nulls out --count values in one sub.txt column (cik | adsh | form) at
--count reproducibly chosen row indices (seeded RNG). num.txt is copied
unmutated. Restricted to sub.txt because the spike measured sub.txt's three
checks at bad=0/22 on real 2009q2 data, so "exactly N planted" is provable
there -- num.txt carries a real nonzero null-`value` baseline on every quarter
(9/3999 in 2009q2), which is why the gate holds that check at warn criticality
(a known SEC characteristic, see dqx_bronze_gate.py CHECKS) rather than a
plantable gating check.

Exit codes:
  0  mutation planted successfully, manifest written/printed
  3  REFUSING -- twin-dest path lacks "twin" substring, OR --count exceeds
     available data rows, OR --data is missing sub.txt / has zero data rows

Operator follow-up (not executed by this script):
  C:/Users/hossa/dev/vantage/.venv-dqx/Scripts/python.exe scripts/dqx_bronze_gate.py \
      --data <twin-dest> --json <out.json>
  Expected: exit 1; JSON check named <expected_gate_check> has outcome "fail" and
  bad == count_planted; the other sub checks have outcome "pass";
  num_value_not_null reports its real baseline at outcome "warn" (non-gating).

Usage:
  C:/Users/hossa/dev/vantage/.venv-dqx/Scripts/python.exe scripts/dqx_gate_mutate_twin.py \
      --data <path> --twin-dest <path> [--column cik] [--count 5] [--seed 42] \
      [--manifest <out.json>]
"""

import argparse
import json
import random
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

CHECK_NAME_BY_COLUMN = {
    "cik": "sub_cik_not_null",
    "adsh": "sub_adsh_not_null",
    "form": "sub_form_not_null_not_empty",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True,
                     help="Source quarter directory to copy from (real data only)")
    ap.add_argument("--twin-dest", required=True,
                     help="Destination directory; must contain 'twin' in its path")
    ap.add_argument("--column", choices=["cik", "adsh", "form"], default="cik",
                     help="sub.txt column to null out")
    ap.add_argument("--count", type=int, default=5,
                     help="Number of data rows (excluding header) to malform")
    ap.add_argument("--seed", type=int, default=42,
                     help="RNG seed for reproducible row selection")
    ap.add_argument("--manifest", default=None,
                     help="Write the mutation manifest JSON here")
    args = ap.parse_args()
    data = Path(args.data)
    twin_dest = Path(args.twin_dest)

    # Safety refusal -- identical shape to gate_b_mutate_twin.py line 51-53.
    if "twin" not in twin_dest.as_posix().lower():
        print(f"REFUSING: {twin_dest} does not look like a twin copy (no 'twin' in path)")
        return 3

    src_sub = data / "sub.txt"
    src_num = data / "num.txt"
    if not src_sub.exists():
        print(f"REFUSING: {src_sub} does not exist -- --data must contain sub.txt")
        return 3

    src_lines = src_sub.read_text(encoding="utf-8").splitlines()
    if len(src_lines) < 1:
        print(f"REFUSING: {src_sub} has zero rows (not even a header)")
        return 3
    header = src_lines[0]
    data_rows = src_lines[1:]
    n_data_rows = len(data_rows)
    if n_data_rows == 0:
        print(f"REFUSING: {src_sub} has zero data rows -- nothing to mutate")
        return 3

    if args.count < 1:
        print(f"REFUSING: --count {args.count} must be >= 1")
        return 3
    if args.count > n_data_rows:
        print(f"REFUSING: --count {args.count} exceeds {n_data_rows} available "
              f"data rows in sub.txt")
        return 3

    twin_dest.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_sub, twin_dest / "sub.txt")
    if src_num.exists():
        shutil.copy2(src_num, twin_dest / "num.txt")

    header_cols = header.split("\t")
    try:
        col_idx = header_cols.index(args.column)
    except ValueError:
        print(f"REFUSING: column '{args.column}' not found in sub.txt header")
        return 3

    row_indices = sorted(random.Random(args.seed).sample(range(n_data_rows), args.count))

    mutated_rows = list(data_rows)
    for idx in row_indices:
        fields = mutated_rows[idx].split("\t")
        fields[col_idx] = ""
        mutated_rows[idx] = "\t".join(fields)

    twin_sub = twin_dest / "sub.txt"
    twin_sub.write_text("\n".join([header] + mutated_rows) + "\n", encoding="utf-8")

    expected_check = CHECK_NAME_BY_COLUMN[args.column]
    manifest = {
        "mutated_at_utc": datetime.now(timezone.utc).isoformat(),
        "data": data.as_posix(),
        "twin_dest": twin_dest.as_posix(),
        "table": "sub",
        "column": args.column,
        "count_planted": args.count,
        "seed": args.seed,
        "row_indices_mutated": row_indices,
        "expected_gate_check": expected_check,
        "expected_gate_outcome": {
            "outcome": "fail",
            "exit": 1,
            "bad": args.count,
        },
    }
    print(json.dumps(manifest, indent=2))
    print(f"dqx_gate_mutate_twin data={data.as_posix()} twin_dest={twin_dest.as_posix()} "
          f"column={args.column} planted={args.count} outcome=MUTATED exit=0")
    print("Operator follow-up: "
          "C:/Users/hossa/dev/vantage/.venv-dqx/Scripts/python.exe "
          f"scripts/dqx_bronze_gate.py --data {twin_dest.as_posix()} --json <out.json>")
    if args.manifest:
        Path(args.manifest).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
