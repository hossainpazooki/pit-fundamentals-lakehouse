ts: 2026-07-21T18:28:46Z
commit: 1354b8d
session: 0e090857-9de3-4fc5-9b6c-b44b7a2b8ba3 (DQX bronze pre-gate build + serverless deploy)
status: verified

fact: A DQX check referencing a column absent from the DataFrame is SKIPPED, not raised:
DQX catches the Spark AnalysisException internally, logs a manager-level warning, and writes
`skipped=True` entries into every row's `_errors` — so with `criticality="error"` the split
becomes good=0/bad=ALL, indistinguishable from a genuine 100% failure unless the consumer
inspects `_errors[].skipped`. This is DQX issue-#609-family behavior observed live
(2026-07-21 spike; independently re-driven by a skeptic against real 2009q2 with an injected
bogus-column check). `dqx_bronze_gate.py` therefore detects skip-shape explicitly and maps
it to UNEVALUABLE (exit 2), never trusting good/bad counts alone. Upstream docs-PR
candidate: document `_errors[].skipped` and the good=0/bad=all collapse.

basis:
```
# ts anchor: skeptic agent completion (epoch 1784658294464 + 232286 ms) -- the injected-check probe that re-drove the skip live
WARNING [d.l.dqx.manager] Skipping check evaluation 'sub_bogus_col_not_null' due to invalid check columns
# split effect on sub.txt (22 rows): good=0, bad=22 -- every row, though nothing was evaluated
# gate mapping (skeptic re-run 2026-07-21): that check -> outcome="unevaluable",
#   detail="Check evaluation skipped due to invalid check columns: [...]" -> exit 2
```

re-verify: drive `run_checks()` from `scripts/dqx_bronze_gate.py` with an injected `DQRowRule(check_func=is_not_null, column="no_such_column")` (outside REQUIRED_* so preconditions pass) against any real quarter — DQX must log the skip warning and the gate must yield that check as `unevaluable`, not pass/fail.
