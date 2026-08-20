# docs/ index

What is authoritative now and what is a point-in-time record. (Split out of the
root README 2026-07-19, in the ATLAS docs convention.)

## Authoritative, current-state (must track the code)

| Doc | What it binds |
|---|---|
| [`SYSTEM.md`](SYSTEM.md) | The PIT property and how it is enforced: medallion architecture, the two guards, provenance posture, the properties-under-test table, limits. |
| [`STATUS.md`](STATUS.md) | Gates, runs, deployment — the dated state of record (moved out of the README 2026-07-19). |
| [`DEVELOPMENT.md`](DEVELOPMENT.md) | Build profiles (2.12 classic / 2.13 Connect), config contract, local + serverless run commands, `scripts/` tooling. |
| [`adr/`](adr/README.md) | Accepted decisions and why the alternative was refused: [0001](adr/0001-airflow-orchestration.md) orchestration, [0002](adr/0002-dbt-transform-layer.md) the dbt transform layer. |

## Append-only ledgers (immutable entries; corrections are new dated entries)

- [`learnings/`](learnings/LEARNINGS.md) — anchored, re-executable facts about
  this repo and its integrations (rigor's learnings format:
  `ts:`/`commit:`/`basis:`/`re-verify:` per entry, pointer-only index, gated
  by rigor's `check-learnings.mjs`).

## Point-in-time records (historical; do not "fix" retroactively)

- [`AIRFLOW-ORCHESTRATION-EVIDENCE.md`](AIRFLOW-ORCHESTRATION-EVIDENCE.md) — the WS1
  orchestration record (2026-07-22): green / twin-red / unevaluable legs run end to
  end, with the corrected negative control and the finding that the first one was
  healed by the pure rebuild.
- [`DBT-EVIDENCE.md`](DBT-EVIDENCE.md) — the WS2 serving-layer record (2026-08-03):
  the as-of serving view and marts, the PIT invariant tests, and the mutated twin
  that drove the as-of key-uniqueness test red.

- [`BACKFILL-METRICS.md`](BACKFILL-METRICS.md) — per-quarter metrics of the
  2009→present backfill (2026-07-08), every number recomputed from run artifacts
  (`scripts/recompute_metrics.py`, exit 0 = match).
- [`GATE-B-WAP-EVIDENCE.md`](GATE-B-WAP-EVIDENCE.md) — the Gate B publish record
  (2026-07-18/19): three-part write-audit-publish credit rule with verbatim
  outputs, the serverless port record, and the honest defect history.

## Written for a reader, not a runner

- [`interview/`](interview/) — short notes explaining single design decisions in
  prose, for someone reading the repo rather than running it.
