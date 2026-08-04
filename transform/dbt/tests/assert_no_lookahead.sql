-- INVARIANT: no-lookahead. A fact visible at the as-of instant must have been
-- accepted by the SEC at or before that instant. If this query returns any
-- rows, a row visible at instant T was accepted after T -- the model used
-- information that did not exist yet at the as-of point in time. A non-empty
-- result means the LAKE violated a PIT guarantee, not that this test needs
-- relaxing.

select *
from {{ ref('as_of_facts') }}
where accepted > {{ as_of_ts() }}
