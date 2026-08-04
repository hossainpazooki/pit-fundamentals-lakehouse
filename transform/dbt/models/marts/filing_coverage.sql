-- filing_coverage: as-of coverage mart.
-- Answers "what did we know about which filer-periods, as of a date" by
-- aggregating the as-of slice (as_of_facts) up to the (cik, ddate, qtrs)
-- grain. This is an as-of slice, not full history: restated facts that were
-- superseded before the as-of instant are invisible here by construction
-- (see restatement_freq for the full-history view).
{{ config(materialized='table') }}

select
    cik,
    ddate,
    qtrs,
    count(*)             as fact_count,
    count(distinct tag)  as distinct_tags,
    min(accepted)        as first_accepted,
    max(accepted)        as last_accepted
from {{ ref('as_of_facts') }}
group by
    cik,
    ddate,
    qtrs
