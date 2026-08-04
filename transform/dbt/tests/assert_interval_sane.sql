-- INVARIANT: interval sanity. Every row's visibility interval must be
-- well-formed: it must start strictly before it ends (valid_from < valid_to).
-- If this query returns any rows, the lake contains a row whose interval is
-- empty or inverted -- a row that was never visible for any positive span of
-- time, or one recorded backwards. A non-empty result means the LAKE
-- violated a PIT guarantee, not that this test needs relaxing.

select *
from {{ ref('stg_gold') }}
where valid_from >= valid_to
