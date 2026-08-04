-- INVARIANT: as-of key uniqueness. At a single as-of instant, exactly one row
-- may be visible per natural key (cik, tag, ddate, qtrs, uom). If this query
-- returns any rows, the as-of slice contains more than one visible version of
-- the same fact at the same instant -- the valid_from/valid_to intervals for
-- that key overlap at the as-of point. A non-empty result means the LAKE
-- violated a PIT guarantee, not that this test needs relaxing.

select cik, tag, ddate, qtrs, uom, count(*) as version_count
from {{ ref('as_of_facts') }}
group by cik, tag, ddate, qtrs, uom
having count(*) > 1
