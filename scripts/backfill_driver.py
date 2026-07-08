"""Chronological full-history backfill driver: ONE quarter per Pipeline invocation.

One-quarter-per-invocation buys halt isolation (a DQ halt costs exactly that quarter),
trivial resumability (the JSONL ledger is the checkpoint), a per-quarter log file, and a
fresh JVM per quarter. A halted or errored quarter is RECORDED AND SKIPPED -- later
quarters are independent at silver -- and surfaces as an explicit hole in the report;
nothing is silently dropped. Gold is NOT built here: run --gold-rebuild once at the end
(gold is a pure rebuild over the whole lake; see the gold sub-scoping finding 2026-07-07).

Usage:
    python scripts/backfill_driver.py [--data-root ...] [--repo ...] [--quarters 2009q1,...]
                                      [--dry-run] [--gold-rebuild]
"""

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ADD_OPENS = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED"
)
HADOOP_HOME = "C:\\Users\\hossa\\hadoop"
RUN_LINE = re.compile(r"run batch_id=")  # sbt prefixes '[info] ' — do not anchor


def build_env(repo: Path, data_root: Path, quarter: str, code_sha: str):
    import os

    env = dict(os.environ)
    env["HADOOP_HOME"] = HADOOP_HOME
    env["PATH"] = f"{HADOOP_HOME}\\bin;" + env.get("PATH", "")
    env["JAVA_TOOL_OPTIONS"] = (
        f"-Dconfig.file={data_root.as_posix()}/backfill.conf -Dspark.master=local[*] {ADD_OPENS}"
    )
    env["PIT_QUARTERS"] = quarter
    env["VANTAGE_CODE_SHA"] = code_sha
    return env


def git_head_sha(repo: Path) -> str:
    """HEAD sha, suffixed -dirty when the working tree differs -- the batch manifest must not
    claim a clean commit that was not actually what ran."""
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    return f"{sha}-dirty" if dirty else sha


def ledger_load(ledger_path: Path):
    done = {}
    if ledger_path.exists():
        for line in ledger_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rec = json.loads(line)
                done[rec["quarter"]] = rec
    return done


def ledger_append(ledger_path: Path, rec: dict):
    with open(ledger_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")


def run_sbt(repo: Path, main_class: str, env, log_path: Path) -> int:
    with open(log_path, "w", encoding="utf-8", errors="replace") as log:
        proc = subprocess.run(
            f'sbt "Test/runMain {main_class}"',
            cwd=repo,
            env=env,
            shell=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    return proc.returncode


def classify(exit_code: int, log_text: str) -> str:
    if exit_code == 0:
        return "ok"
    if "DataQualityException" in log_text or "DQ gate" in log_text:
        return "halted"
    return "error"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="C:/Users/hossa/dev/vantage-data")
    ap.add_argument("--repo", default="C:/Users/hossa/dev/vantage")
    ap.add_argument("--quarters", default="", help="comma list; default = every extracted source quarter")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--gold-rebuild", action="store_true", help="run the single GoldRebuild pass and exit")
    args = ap.parse_args()

    data_root, repo = Path(args.data_root), Path(args.repo)
    logs_dir = data_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    ledger_path = data_root / "backfill-ledger.jsonl"
    code_sha = git_head_sha(repo)

    if args.gold_rebuild:
        env = build_env(repo, data_root, "", code_sha)
        t0 = time.monotonic()
        rc = run_sbt(repo, "pit.gold.GoldRebuild", env, logs_dir / "gold-rebuild.log")
        wall = round(time.monotonic() - t0, 1)
        log_text = (logs_dir / "gold-rebuild.log").read_text(encoding="utf-8", errors="replace")
        line = next((l for l in log_text.splitlines() if l.startswith("gold_rebuild ")), "")
        ledger_append(
            ledger_path,
            {"quarter": "_gold_rebuild", "status": "ok" if rc == 0 else "error", "exit": rc,
             "wall_s": wall, "log_line": line, "code_sha": code_sha,
             "ts": datetime.now(timezone.utc).isoformat()},
        )
        print(f"gold rebuild: exit={rc} wall={wall}s {line}")
        sys.exit(rc)

    if args.quarters:
        quarters = [q.strip() for q in args.quarters.split(",") if q.strip()]
    else:
        quarters = sorted(
            p.name for p in (data_root / "source").iterdir()
            if p.is_dir() and re.fullmatch(r"\d{4}q[1-4]", p.name)
            and (p / "num.txt").exists() and (p / "sub.txt").exists()
        )
    done = ledger_load(ledger_path)
    todo = [q for q in quarters if done.get(q, {}).get("status") != "ok"]
    print(f"{len(quarters)} quarters total, {len(quarters) - len(todo)} already ok, {len(todo)} to run")
    print(f"code_sha pinned for this backfill: {code_sha}")
    if args.dry_run:
        print("dry run:", " ".join(todo))
        return

    walls = []
    for i, q in enumerate(todo, 1):
        env = build_env(repo, data_root, q, code_sha)
        log_path = logs_dir / f"backfill-{q}.log"
        t0 = time.monotonic()
        rc = run_sbt(repo, "pit.Pipeline", env, log_path)
        wall = round(time.monotonic() - t0, 1)
        log_text = log_path.read_text(encoding="utf-8", errors="replace")
        status = classify(rc, log_text)
        line = next((l for l in log_text.splitlines() if RUN_LINE.search(l)), "")
        ledger_append(
            ledger_path,
            {"quarter": q, "status": status, "exit": rc, "wall_s": wall, "log_line": line,
             "code_sha": code_sha, "ts": datetime.now(timezone.utc).isoformat()},
        )
        walls.append(wall)
        recent = walls[-5:]
        projected_min = sum(recent) / len(recent) * (len(todo) - i) / 60
        print(f"[{i}/{len(todo)}] {q}: {status} exit={rc} wall={wall}s "
              f"(projected remaining ~{projected_min:.0f} min)")
        if status != "ok":
            print(f"    {status.upper()}: see {log_path.name}; continuing with later quarters")

    bad = [q for q in todo if ledger_load(ledger_path).get(q, {}).get("status") != "ok"]
    print(f"backfill pass complete; not-ok quarters: {bad if bad else 'none'}")


if __name__ == "__main__":
    main()
