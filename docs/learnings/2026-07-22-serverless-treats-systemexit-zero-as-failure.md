ts: 2026-07-22T17:24:37Z
commit: 1354b8d
session: 0e090857-9de3-4fc5-9b6c-b44b7a2b8ba3 (DQX bronze pre-gate build + serverless deploy)
status: verified

fact: The serverless `spark_python_task` harness (IPython-style) treats ANY `SystemExit` as
a workload failure — including `SystemExit(0)`, a clean exit. Observed 2026-07-22: the gate
ran fully green on 2026q1 (JSON artifact on the volume: outcome pass, exit 0, warn 139,723)
yet the task FAILED with "SystemExit: 0" — a green-effect/failed-status split, the
self-report-vs-effect gap inverted. Fix is gate-side and preserves the fail-closed contract:
return normally on 0, `sys.exit(code)` only for nonzero (red exit 1 / unevaluable exit 2,
where a failed task IS the contract). Platform bug-report candidate.

basis:
```
# ts anchor: failing run task-output mtime (bxrpkoazu.output) -- the SystemExit: 0 observation
# run 2026-07-22 13:2xZ: task FAILED --
An exception has occurred, use %tb to see the full traceback.
SystemExit: 0
# same run's artifact, fetched from the volume:
$ databricks fs cat dbfs:/Volumes/workspace/default/vantage/dqx-verify/results/gate.json
{ "quarter": "2026q1", "outcome": "pass", "exit": 0, ... "num_value_not_null" warn 139723/3690955 }
# post-fix (commit 1354b8d) leg C, 2026-07-22: same data -> TERMINATED SUCCESS
```

re-verify: `grep -n "sys.exit" scripts/dqx_bronze_gate.py` — the entrypoint must raise SystemExit only for nonzero codes; reverting to `sys.exit(main())` and rerunning `vantage_dqx_bronze_gate` on a passing dataset reproduces the SystemExit:0 task failure.
