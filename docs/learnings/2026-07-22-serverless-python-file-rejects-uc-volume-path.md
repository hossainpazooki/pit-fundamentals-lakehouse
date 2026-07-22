ts: 2026-07-22T17:21:15Z
commit: 1354b8d
session: 0e090857-9de3-4fc5-9b6c-b44b7a2b8ba3 (DQX bronze pre-gate build + serverless deploy)
status: verified

fact: A serverless `spark_python_task` with `python_file:` pointing at a UC-volume path
fails in ~40s with "Cannot read the python file ... Either it does not exist, or the
identity ... lacks the required permissions" — while `databricks fs cat` reads the very same
path with the very same identity. Bundle-relative `python_file: scripts/dqx_bronze_gate.py`
(DABs rewrites to the synced workspace file) works. Inverted from serverless JAR tasks,
whose `java_dependencies` REQUIRE the absolute volume path (GATE-B port record). Platform
docs gap, recorded in the `databricks.yml` comment.

basis:
```
# ts anchor: failing run task-output mtime (bg9slb6wv.output), matching the CLI line "2026-07-22 13:21:15" (UTC-4 local)
# run 2026-07-22 13:21Z, job [dev hossain] vantage-dqx-bronze-gate:
INTERNAL_ERROR FAILED Task run_dqx_gate failed with message: Cannot read the python file
/Volumes/workspace/default/vantage/scripts/dqx_bronze_gate.py. Either it does not exist, or
the identity used to run this job, hossain@pazooki.com, lacks the required permissions.
$ databricks fs cat dbfs:/Volumes/workspace/default/vantage/scripts/dqx_bronze_gate.py | head -1
"""DQX generic row-checks over bronze/raw SEC quarter TSVs (sub.txt, num.txt).
```

re-verify: set `python_file:` in `databricks.yml` to an fs-cat-readable `/Volumes/...` copy of the script, `bundle deploy` + `bundle run vantage_dqx_bronze_gate` — the task must fail with the cannot-read message; restore the bundle-relative path and the same run must start.
