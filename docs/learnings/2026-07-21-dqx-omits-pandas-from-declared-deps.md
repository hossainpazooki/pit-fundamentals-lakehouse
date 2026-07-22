ts: 2026-07-21T17:44:45Z
commit: 1354b8d
session: 0e090857-9de3-4fc5-9b6c-b44b7a2b8ba3 (DQX bronze pre-gate build + serverless deploy)
status: verified

fact: `databricks-labs-dqx` 0.15.0 does not declare `pandas` among its dependencies, but
`check_funcs.py` does an unconditional top-level `import pandas as pd` — so a venv holding
exactly DQX's declared deps fails at import time. Observed 2026-07-21 during the local spike
(`.venv-dqx`, Python 3.14.2); `pip install pandas` (resolved 3.0.3) fixed it. Upstream
bug-report candidate (packaging gap: pandas belongs in install_requires).

basis:
```
# ts anchor: spike agent start (workflow wf_0b3e158e-773, startedAt epoch 1784655885077) -- the venv/install phase where the import failure surfaced
$ pip show databricks-labs-dqx
Requires: databricks-labs-blueprint, databricks-sdk, pyyaml, sqlalchemy      # no pandas
$ python -c "from databricks.labs.dqx.engine import DQEngineCore"
ModuleNotFoundError: No module named 'pandas'
# (spike record, 2026-07-21; venv contained only DQX's declared dependency closure)
```

re-verify: `python -m venv t && t/Scripts/pip install --no-deps databricks-labs-dqx databricks-labs-blueprint databricks-sdk pyyaml sqlalchemy && t/Scripts/python -c "from databricks.labs.dqx.engine import DQEngineCore"` — must raise ModuleNotFoundError for pandas while 0.15.0's metadata still omits it.
