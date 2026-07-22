# Learnings ledger (index)

Anchored, re-executable records of non-obvious facts learned about **this
repo and its integrations** — what survived refutation and what got killed.
Entries live beside this file as dated immutable markdown files,
`YYYY-MM-DD-<topic>.md`, so a plain listing sorts chronologically. This index
holds **pointers only, never evidence**: every claim here must trace to an
entry file, which traces to its quoted basis.

Each entry is a record with required fields (rigor's learnings format, gated
by `node ../rigor/scripts/check-learnings.mjs docs/learnings`):

- `ts:` — RFC 3339 UTC, captured from the system clock when the entry landed
- `commit:` — this repo's HEAD at capture
- `session:` — provenance pointer into the harness transcript
- `status:` — `verified` | `refuted-assumption` | `suspected`
- `fact:` — the one-line non-obvious finding
- `basis:` — the command run and its output, quoted at capture
- `re-verify:` — one executable line that re-establishes the fact

Entries are immutable once written; a wrong entry is never edited in place —
a dated superseding entry with a `kills:` reference is appended instead.

## Entries

- [2026-07-21 — DQX omits pandas from its declared deps](2026-07-21-dqx-omits-pandas-from-declared-deps.md)
  — import fails in a clean venv; upstream bug-report candidate.
- [2026-07-21 — DQEngine requires a live workspace even for local-Spark checks](2026-07-21-dqengine-requires-live-workspace-for-local-checks.md)
  — hangs (SDK retry) without one; accepted environment risk, upstream feature-request candidate.
- [2026-07-21 — DQX missing-column skip collapses into the bad count](2026-07-21-dqx-missing-column-skip-collapses-into-bad-count.md)
  — good=0/bad=all is indistinguishable from 100%-fail without `_errors[].skipped`; upstream docs-PR candidate.
- [2026-07-22 — serverless spark_python_task rejects a UC-volume python_file](2026-07-22-serverless-python-file-rejects-uc-volume-path.md)
  — while the files API reads the same path; bundle-relative path works; platform docs gap.
- [2026-07-22 — serverless python harness treats SystemExit(0) as workload failure](2026-07-22-serverless-treats-systemexit-zero-as-failure.md)
  — a clean exit fails the task; gate returns normally on 0.
- [2026-07-22 — Git Bash MSYS mangles /Volumes paths in --var args](2026-07-22-msys-mangles-volume-paths-in-var-args.md)
  — rewritten to `C:/Program Files/Git/Volumes/...`; `MSYS_NO_PATHCONV=1` or PowerShell.
