ts: 2026-07-21T17:58:53Z
commit: 1354b8d
session: 0e090857-9de3-4fc5-9b6c-b44b7a2b8ba3 (DQX bronze pre-gate build + serverless deploy)
status: verified

fact: `DQEngine(WorkspaceClient())` requires a live, reachable Databricks workspace even
when the only checks being run are generic local-Spark row checks with no workspace-side
dependency — with no reachable workspace it hangs >100s in SDK retry/backoff rather than
failing fast or working offline. Observed 2026-07-21 (spike); every green local run of
`scripts/dqx_bronze_gate.py` since has visibly authed first ("loading DEFAULT profile ...
Using Databricks CLI authentication"). Accepted as a known environment risk — the gate does
NOT engineer around it (no timeout/threading). Upstream feature-request candidate: an
offline/local-only construction path for pure local-Spark usage.

basis:
```
# ts anchor: spike agent completion (epoch 1784655885077 + 848076 ms) -- the finding is reported in its result
# spike, 2026-07-21: DQEngine construction with no reachable workspace hung >100s (SDK retry)
# every subsequent local gate run, e.g. orchestrator re-run 2026-07-21 14:30Z:
INFO [databricks.sdk] loading DEFAULT profile from ~/.databrickscfg: host, auth_type
INFO [databricks.sdk] Using Databricks CLI authentication
[PASS] precondition: sub.txt readable (22 rows), num.txt readable (3999 rows) ...
```

re-verify: `DATABRICKS_CONFIG_FILE=/dev/null DATABRICKS_HOST= .venv-dqx/Scripts/python.exe scripts/dqx_bronze_gate.py --data <any-quarter>` — DQX engine construction must not complete promptly without a reachable workspace (hang or auth error, never a silent offline pass).
