"""Build the injected-unevaluable fixture: a real quarter with the `value` column REMOVED.

Halt-on-unevaluable must be demonstrated, not assumed (a backfill where /unevaluable stays
empty and unexercised is a vacuous probe). Dropping `value` from num.txt trips the source
precondition (`DataQualityGate.requiredColumnsPresent`, checked before castNum can mask the
absence as typed nulls) -- the exact fail-closed path. Run it against a SCRATCH lake so the
real backfill lake is never polluted:

    python scripts/make_uneval_fixture.py --source-quarter 2009q1
    # then run pit.Pipeline with -Dconfig.file=.../uneval-demo.conf and PIT_QUARTERS=2009q1
    # and probe: /unevaluable has exactly one (batchId, reason) row; silver ABSENT;
    # bronze + registry PRESENT (ingest precedes the gate, by design).
"""

import argparse
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="C:/Users/hossa/dev/vantage-data")
    ap.add_argument("--source-quarter", default="2009q1")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    q = args.source_quarter
    src = data_root / "source" / q
    dst = data_root / "source-uneval" / q
    dst.mkdir(parents=True, exist_ok=True)

    # sub.txt verbatim; num.txt with the `value` column dropped from header and every row.
    (dst / "sub.txt").write_bytes((src / "sub.txt").read_bytes())
    with open(src / "num.txt", encoding="utf-8", errors="replace") as fin, \
         open(dst / "num.txt", "w", encoding="utf-8", newline="") as fout:
        header = fin.readline().rstrip("\r\n").split("\t")
        if "value" not in header:
            raise SystemExit(f"FATAL: no `value` column in {src / 'num.txt'} header: {header}")
        drop = header.index("value")
        fout.write("\t".join(c for i, c in enumerate(header) if i != drop) + "\n")
        for line in fin:
            cells = line.rstrip("\r\n").split("\t")
            fout.write("\t".join(c for i, c in enumerate(cells) if i != drop) + "\n")

    conf = data_root / "uneval-demo.conf"
    conf.write_text(
        "// Injected-unevaluable demo (2026-07-07). SCRATCH lake -- never the backfill lake.\n"
        "pit {\n"
        '  spark { app-name = "vantage-uneval-demo", master = "local[*]" }\n'
        "  paths {\n"
        f'    source-dir      = "{(data_root / "source-uneval").as_posix()}"\n'
        f'    bronze-root     = "{(data_root / "lake-uneval-demo/bronze").as_posix()}"\n'
        f'    silver-root     = "{(data_root / "lake-uneval-demo/silver").as_posix()}"\n'
        f'    gold-root       = "{(data_root / "lake-uneval-demo/gold").as_posix()}"\n'
        f'    quarantine-root = "{(data_root / "lake-uneval-demo/quarantine").as_posix()}"\n'
        f'    registry-path   = "{(data_root / "lake-uneval-demo/registry").as_posix()}"\n'
        "  }\n"
        f'  ingest {{ quarters = ["{q}"] }}\n'
        "}\n",
        encoding="utf-8",
    )
    print(f"fixture ready: {dst} (value column dropped); conf: {conf}")


if __name__ == "__main__":
    main()
