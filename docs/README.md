# docs/ index

What is authoritative now and what is a point-in-time record. (Split out of the
root README 2026-07-19, in the ATLAS docs convention.)

## Authoritative, current-state (must track the code)

| Doc | What it binds |
|---|---|
| [`SYSTEM.md`](SYSTEM.md) | The PIT property and how it is enforced: medallion architecture, the two guards, provenance posture, the properties-under-test table, limits. |
| [`STATUS.md`](STATUS.md) | Gates, runs, deployment — the dated state of record (moved out of the README 2026-07-19). |
| [`DEVELOPMENT.md`](DEVELOPMENT.md) | Build profiles (2.12 classic / 2.13 Connect), config contract, local + serverless run commands, `scripts/` tooling. |

## Append-only ledgers (immutable entries; corrections are new dated entries)

- [`learnings/`](learnings/LEARNINGS.md) — anchored, re-executable facts about
  this repo and its integrations (rigor's learnings format:
  `ts:`/`commit:`/`basis:`/`re-verify:` per entry, pointer-only index, gated
  by rigor's `check-learnings.mjs`).

## Point-in-time records (historical; do not "fix" retroactively)

- [`BACKFILL-METRICS.md`](BACKFILL-METRICS.md) — per-quarter metrics of the
  2009→present backfill (2026-07-08), every number recomputed from run artifacts
  (`scripts/recompute_metrics.py`, exit 0 = match).
- [`GATE-B-WAP-EVIDENCE.md`](GATE-B-WAP-EVIDENCE.md) — the Gate B publish record
  (2026-07-18/19): three-part write-audit-publish credit rule with verbatim
  outputs, the serverless port record, and the honest defect history.
