ts: 2026-07-22T17:30:04Z
commit: 1354b8d
session: 0e090857-9de3-4fc5-9b6c-b44b7a2b8ba3 (DQX bronze pre-gate build + serverless deploy)
status: verified

fact: Git Bash (MSYS) rewrites POSIX-looking path values inside `--var=name=/Volumes/...`
arguments to `C:/Program Files/Git/Volumes/...` before the databricks CLI ever sees them —
so a bundle run launched from Git Bash bakes the mangled path into job parameters and the
Linux driver crashes on it (observed 2026-07-22: JSON write failed with FileNotFoundError on
the mangled path; two legs lost). Values coming from `databricks.yml` defaults are immune
(no shell involved). Fix: `MSYS_NO_PATHCONV=1` in the environment, or run bundle commands
from PowerShell. Local tooling footgun, noted in CLAUDE.md; not upstreamable.

basis:
```
# ts anchor: failing run task-output mtime (bbprga24r.output) -- the mangled-path FileNotFoundError legs
# job run 2026-07-22 (launched from Git Bash without MSYS_NO_PATHCONV):
FileNotFoundError: [Errno 2] No such file or directory:
  'C:/Program Files/Git/Volumes/workspace/default/vantage/dqx-verify/results/gate_2009q2.json'
# identical command with MSYS_NO_PATHCONV=1: TERMINATED SUCCESS, JSON written to the volume
```

re-verify: from Git Bash, `cmd //c echo --var=x=/Volumes/test` prints the mangled `C:/Program Files/Git/...` form; `MSYS_NO_PATHCONV=1 cmd //c echo --var=x=/Volumes/test` prints it verbatim.
