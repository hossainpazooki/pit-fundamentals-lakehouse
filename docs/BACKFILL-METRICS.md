# Full-history backfill — per-quarter run metrics (2026-07-08)

Every published SEC FSDS quarter, 2009q1 → 2026q1 (69 quarters; the list was taken from SEC's
own listing page, not assumed). **63 quarters ingested; 6 refused by the DQ gate, each with a
recorded, recomputed cause** — refusals are explicit holes, never silent dedupe.

## Provenance: how each number was produced

Every number below was **recomputed from run artifacts** after the fact — none is restated
from the pipeline's own console output; the per-quarter `run …` log lines were used only as a
cross-check, which passes exactly (`scripts/recompute_metrics.py`, exit 0).

| metric | artifact source |
|---|---|
| rows_in | `bronze/num` count per `_batch_id` (DuckDB over Delta) |
| scoped_out | entity-scope predicate re-applied to bronze per batch |
| rows_to_silver | silver `_delta_log` WRITE/MERGE `operationMetrics` per commit |
| q_rows / q_uneval | quarantine lanes counted per `(_batch_id, _ingest_ts)` run-slice (append-mode audit lanes grow on rerun, by design) |
| q_detail | `/detail` `_delta_log` commit metrics (Deequ check-result rows at backfill time; runs after the 2026-07-19 serverless port write the native gate's detail schema instead) |
| dedupe | MERGE metrics: `numSourceRows − inserted − matchedUpdated` |
| key_collisions | 6-col natural key `GROUP BY … HAVING count>1` over bronze per batch |
| wall_clock_s | `wall_clock_ms` in the run log line (silver stage; excludes ~30–45 s sbt/JVM startup per invocation; halted quarters show 0 — no completed stage) |

Join spine: batch registry `manifest_json → param.quarter ↔ batch_id`. One registry row per
published quarter: 69/69 present, including refused quarters (ingest and registration precede
the gate, by design — refused batches are inspectable in bronze but absent from silver/gold).

## Per-quarter table

| quarter | rows_in | scoped_out | rows_to_silver | q_rows | q_detail | q_uneval | dedupe | key_collisions | wall_clock_s |
|---|---|---|---|---|---|---|---|---|---|
| 2009q1 | 0 | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 0 |
| 2009q2 | 3999 | 269 | 3721 | 9 | 0 | 0 | 0 | 0 | 3 |
| 2009q3 | 126650 | 17096 | 109507 | 47 | 0 | 0 | 0 | 0 | 6 |
| 2009q4 | 146335 | 23965 | 122357 | 13 | 0 | 0 | 0 | 0 | 7 |
| 2010q1 | 194741 | 51812 | 142902 | 27 | 0 | 0 | 0 | 0 | 7 |
| 2010q2 | 136044 | 26292 | 109459 | 293 | 0 | 0 | 0 | 0 | 8 |
| 2010q3 | 480612 | 129732 | 348906 | 1974 | 0 | 0 | 0 | 0 | 10 |
| 2010q4 | 511508 | 143408 | 365733 | 2367 | 0 | 0 | 0 | 0 | 14 |
| 2011q1 | 690483 | 266189 | 0 | 2305 | 0 | 0 | 0 | **1** | 0 |
| 2011q2 | 498885 | 148031 | 348948 | 1906 | 0 | 0 | 0 | 0 | 13 |
| 2011q3 | 2025154 | 535956 | 1462455 | 26743 | 0 | 0 | 0 | 0 | 21 |
| 2011q4 | 2227859 | 617022 | 0 | 32244 | 0 | 0 | 0 | **2** | 0 |
| 2012q1 | 2699630 | 984926 | 0 | 35429 | 0 | 0 | 0 | **5** | 0 |
| 2012q2 | 2407915 | 726133 | 1630964 | 50818 | 0 | 0 | 0 | 0 | 25 |
| 2012q3 | 2816079 | 1066242 | 0 | 52861 | 0 | 0 | 0 | **4** | 0 |
| 2012q4 | 2999818 | 1175904 | 0 | 59465 | 0 | 0 | 0 | **2** | 0 |
| 2013q1 | 3156166 | 1407324 | 1704593 | 44249 | 0 | 0 | 0 | 0 | 27 |
| 2013q2 | 2943353 | 1203540 | 1674243 | 65570 | 0 | 0 | 0 | 0 | 75 |
| 2013q3 | 3081119 | 1262847 | 1753609 | 64663 | 0 | 0 | 0 | 0 | 101 |
| 2013q4 | 3097400 | 1288567 | 1747131 | 61702 | 0 | 0 | 0 | 0 | 93 |
| 2014q1 | 3483361 | 1615043 | 1814300 | 54018 | 0 | 0 | 0 | 0 | 33 |
| 2014q2 | 2814446 | 1154071 | 1595103 | 65272 | 0 | 0 | 0 | 0 | 41 |
| 2014q3 | 2936590 | 1199556 | 1684898 | 52136 | 0 | 0 | 0 | 0 | 36 |
| 2014q4 | 2940236 | 1216745 | 1673783 | 49708 | 0 | 0 | 0 | 0 | 32 |
| 2015q1 | 3322479 | 1520467 | 1763006 | 39006 | 0 | 0 | 0 | 0 | 44 |
| 2015q2 | 2588598 | 1066226 | 1475205 | 47167 | 0 | 0 | 0 | 0 | 49 |
| 2015q3 | 2780346 | 1141825 | 1594002 | 44519 | 0 | 0 | 0 | 0 | 56 |
| 2015q4 | 2780517 | 1159675 | 1577608 | 43234 | 0 | 0 | 0 | 0 | 38 |
| 2016q1 | 3174261 | 1462502 | 1676756 | 35003 | 0 | 0 | 0 | 0 | 35 |
| 2016q2 | 2394508 | 997633 | 1354064 | 42811 | 0 | 0 | 0 | 0 | 52 |
| 2016q3 | 2370332 | 1002779 | 1333628 | 33925 | 0 | 0 | 0 | 0 | 46 |
| 2016q4 | 2627253 | 1110774 | 1476481 | 39998 | 0 | 0 | 0 | 0 | 53 |
| 2017q1 | 3095513 | 1438333 | 1619162 | 38018 | 0 | 0 | 0 | 0 | 51 |
| 2017q2 | 2321869 | 985546 | 1291259 | 45064 | 0 | 0 | 0 | 0 | 56 |
| 2017q3 | 2475254 | 1038533 | 1396830 | 39891 | 0 | 0 | 0 | 0 | 55 |
| 2017q4 | 2518948 | 1052702 | 1422833 | 43413 | 0 | 0 | 0 | 0 | 61 |
| 2018q1 | 3033024 | 1437880 | 1558582 | 36562 | 0 | 0 | 0 | 0 | 68 |
| 2018q2 | 2663988 | 1226177 | 1388760 | 49051 | 0 | 0 | 0 | 0 | 63 |
| 2018q3 | 2629893 | 1185285 | 1403603 | 41005 | 0 | 0 | 0 | 0 | 57 |
| 2018q4 | 2681097 | 1233218 | 1404541 | 43338 | 0 | 0 | 0 | 0 | 64 |
| 2019q1 | 3072555 | 1549924 | 1488753 | 33878 | 0 | 0 | 0 | 0 | 50 |
| 2019q2 | 2816279 | 1385635 | 1378772 | 51872 | 0 | 0 | 0 | 0 | 57 |
| 2019q3 | 2957329 | 1477814 | 1434683 | 44832 | 0 | 0 | 0 | 0 | 74 |
| 2019q4 | 3094866 | 1571321 | 1475918 | 47627 | 0 | 0 | 0 | 0 | 74 |
| 2020q1 | 3099352 | 1540973 | 1516414 | 41965 | 0 | 0 | 0 | 0 | 68 |
| 2020q2 | 2562710 | 1221933 | 1297715 | 43062 | 0 | 0 | 0 | 0 | 62 |
| 2020q3 | 2868109 | 1359717 | 1468151 | 40241 | 0 | 0 | 0 | 0 | 67 |
| 2020q4 | 2963091 | 1437725 | 1484603 | 40763 | 0 | 0 | 0 | 0 | 68 |
| 2021q1 | 3072093 | 1470877 | 1563882 | 37334 | 0 | 0 | 0 | 0 | 87 |
| 2021q2 | 2833915 | 1333074 | 1452835 | 48006 | 0 | 0 | 0 | 0 | 81 |
| 2021q3 | 3172372 | 1490946 | 1631355 | 50071 | 0 | 0 | 0 | 0 | 77 |
| 2021q4 | 3389370 | 1649021 | 1687016 | 53333 | 0 | 0 | 0 | 0 | 85 |
| 2022q1 | 3264632 | 1572095 | 1645648 | 46889 | 0 | 0 | 0 | 0 | 83 |
| 2022q2 | 3047158 | 1402477 | 1585863 | 58818 | 0 | 0 | 0 | 0 | 84 |
| 2022q3 | 3229151 | 1496902 | 1680560 | 51689 | 0 | 0 | 0 | 0 | 90 |
| 2022q4 | 3543392 | 1793509 | 1695186 | 54697 | 0 | 0 | 0 | 0 | 91 |
| 2023q1 | 3428670 | 1757173 | 1627407 | 44090 | 0 | 0 | 0 | 0 | 79 |
| 2023q2 | 3393990 | 1779712 | 1560858 | 53420 | 0 | 0 | 0 | 0 | 80 |
| 2023q3 | 3572434 | 1876390 | 1646312 | 49732 | 0 | 0 | 0 | 0 | 78 |
| 2023q4 | 3699124 | 1999387 | 1647933 | 51804 | 0 | 0 | 0 | 0 | 91 |
| 2024q1 | 3428694 | 1888203 | 1502701 | 37790 | 0 | 0 | 0 | 0 | 84 |
| 2024q2 | 3426170 | 1861866 | 1510111 | 54193 | 0 | 0 | 0 | 0 | 81 |
| 2024q3 | 3521878 | 1909423 | 1567509 | 44946 | 0 | 0 | 0 | 0 | 90 |
| 2024q4 | 3705078 | 2072775 | 1584999 | 47304 | 0 | 0 | 0 | 0 | 93 |
| 2025q1 | 3658551 | 2082242 | 1534768 | 41541 | 0 | 0 | 0 | 0 | 104 |
| 2025q2 | 3409930 | 1968783 | 1390589 | 50558 | 0 | 0 | 0 | 0 | 112 |
| 2025q3 | 3720081 | 2138383 | 1534357 | 47341 | 0 | 0 | 0 | 0 | 108 |
| 2025q4 | 3832977 | 2255614 | 1528158 | 49205 | 0 | 0 | 0 | 0 | 110 |
| 2026q1 | 3690955 | 2185031 | 1464407 | 41517 | 0 | 0 | 0 | 0 | 84 |
| **total** | **181,351,169** | **84,849,150** | **86,616,395** | **2,744,342** | 0 | 2 | 0 | **14** | |

Totals reconcile against the tables themselves: bronze holds exactly 181,351,169 rows; silver
exactly 86,616,395; gold 86,615,392 (see the 2013q1 note below).

## The six refused quarters (explicit holes, with recomputed causes)

- **2009q1 — SEC publishes it empty.** Its zip's `num.txt` is 62 bytes: a header and zero
  rows (verified inside SEC's own zip). The gate fail-closed as *unevaluable* ("empty batch
  where rows expected"); the first data-bearing quarter is 2009q2. `q_uneval = 2` is one
  provenance row per halted attempt (the quarter was attempted twice).
- **2011q1, 2011q4, 2012q1, 2012q3, 2012q4 — natural-key collisions with CONFLICTING
  values.** 1 / 2 / 5 / 4 / 2 colliding keys respectively (14 keys, 28 rows, out of ~11.4M
  rows in those quarters). Inspected directly: the same filing asserts the same
  `(adsh, tag, version, ddate, qtrs, uom)` with two different values — e.g. dividends
  declared of 0.4667 vs 0.4700, and in one case −115,587,000 as a per-share dividend. No
  correct silent resolution exists, so the gate refuses the quarter whole rather than picking
  a winner. Row-level quarantine of colliding keys is future work — a DQ-contract change
  needing its own test round. Retries re-halt identically (deterministic refusal).

## Other measured facts

- **2013q1 orphans:** 1,003 silver rows (all in 2013q1) reference an `adsh` with no filing
  record in that quarter's `sub.txt` — a wart in SEC's published files. Gold's inner join
  excludes them (no filing record → no `cik`/`accepted`); they remain in silver. This accounts
  for the entire silver−gold difference: 86,616,395 − 86,615,392 = 1,003 exactly.
- **Gold:** 86,615,392 point-in-time facts, 16,667 distinct CIKs, `accepted` spanning
  2009-04-15 → 2026-03-31. Built once from full silver + the accumulated filing index
  (393,384 filings), 153–165 s per rebuild.
- **Idempotency, re-executed not asserted** (`scripts/idempotency_diff.py`, exit 0): 2026q1
  rerun end-to-end + a second gold rebuild — bronze count unchanged (MERGE inserted 0),
  registry count unchanged, silver count and whole-table content fingerprint (all columns
  except the `_ingest_ts` audit column) unchanged, gold count and whole-table fingerprint
  identical across rebuilds (both versions read through the same reader — an asymmetric
  read path false-flags timestamp columns), quarantine `/rows` grew by **exactly** 41,517
  (append-mode audit lane, by design).
- **Time-travel, executed not asserted:** 2015q3 reconstructed from the registry's recorded
  `delta_version=26` via versioned read, filtered to its `_batch_id`: 2,780,346 rows; two-way
  anti-join on `_src_row_hash` against current bronze is empty in both directions. Retrieval,
  not byte-for-byte replay — the rung stays where it was.
- **Unevaluable-halt demonstrated twice:** naturally (2009q1 above) and via an injected
  fixture (`value` column removed from a copy of a real quarter, scratch lake): nonzero exit,
  one `(batchId, reason)` row per attempt in `/unevaluable`, silver absent, bronze + registry
  present (ingest precedes the gate, by design).
- **Dedupe = 0 everywhere:** no quarter resends another's filings (`adsh` is quarter-unique),
  and the idempotency rerun's MERGE inserted 0 — the dedupe machinery's effect shows up as
  the rerun's no-op, not as first-pass row drops.
- Wall-clock is the silver-stage `wall_clock_ms` from each run's log line and excludes
  ~30–45 s of sbt/JVM startup per invocation; total pipeline time for the 63 ingested
  quarters ≈ 66 min of stage time (~2.4 h including startup overhead and the refused
  quarters' attempts).
