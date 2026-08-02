# Architecture Decision Records

This directory is new as of the 2026-07-22 gap-closure sprint (WS1). VANTAGE
previously recorded decisions in evidence docs and session-state files under
`docs/superpowers/plans/` -- see `CLAUDE.md`. ADRs formalize the load-bearing
subset of those decisions going forward.

## Convention

- Filename: `NNNN-title-with-dashes.md`, zero-padded 4-digit sequence
  (`0001`, `0002`, ...).
- Required sections: `Status` (`Proposed` / `Accepted` / `Superseded`),
  `Context`, `Decision`, `Consequences`.
- `0001-airflow-orchestration.md` is the worked example.
- `Status` starts `Proposed`. A record only moves to `Accepted` once the
  decision has landed and its claims are backed by evidence (for `0001`,
  that means `docs/AIRFLOW-ORCHESTRATION-EVIDENCE.md`'s green/red/
  unevaluable/backfill legs, not the DAG merely existing). `Superseded`
  records stay in place and gain a one-line pointer to whichever ADR
  replaced them -- history is not deleted.
- Write the `Decision` section as decisions, not description: each
  numbered/headed decision point should be followed by the reasoning that
  ruled out the alternative, not just a restatement of what was built.
  `Consequences` must name real residual gaps and limits honestly (e.g. "not
  yet enforced by a technical guard") rather than implying more coverage
  than what actually landed.

## Adding a new ADR

Copy `0001-airflow-orchestration.md`'s section shape, pick the next
zero-padded sequence number, and list it below.

## Index

- [`0001-airflow-orchestration.md`](0001-airflow-orchestration.md) -- local
  Airflow orchestration for the silver -> gold -> audit -> publish pipeline
  (sibling-container `spark-submit`, sequential silver, single gold rebuild,
  fail-closed audit gate). Status: Accepted (2026-07-22, evidence:
  `docs/AIRFLOW-ORCHESTRATION-EVIDENCE.md`).
