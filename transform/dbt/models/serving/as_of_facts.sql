-- as_of_facts: the point-in-time serving view.
--
-- Returns exactly the rows of stg_gold that were VISIBLE at the as-of instant
-- (var("as_of_date") at 23:59:59 America/New_York), across all restatements.
-- One row per natural key (cik, tag, ddate, qtrs, uom): whichever version of
-- that key was open (valid_from <= T < valid_to) at instant T.
--
-- The window is HALF-OPEN by construction: valid_from <= T AND T < valid_to.
-- This is deliberate, not incidental -- a row's last instant of visibility and
-- its successor row's first instant of visibility must never overlap. If the
-- window were closed on both ends (<=, <=) a restatement landing exactly at T
-- could make two versions of the same natural key visible simultaneously,
-- which would break the "exactly one row per natural key" guarantee this
-- model exists to provide. Closed-open (this model) is the only shape that
-- keeps the timeline a true partition.

select
    cik,
    tag,
    ddate,
    qtrs,
    uom,
    value,
    accepted,
    adsh,
    version,
    valid_from,
    valid_to
from {{ ref('stg_gold') }}
where valid_from <= {{ as_of_ts() }}
  and {{ as_of_ts() }} < valid_to
