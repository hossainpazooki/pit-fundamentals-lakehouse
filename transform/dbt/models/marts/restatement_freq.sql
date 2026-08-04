-- restatement_freq: full-history restatement-frequency mart.
--
-- Built FROM ref('stg_gold') -- deliberately NOT from as_of_facts. An as-of
-- slice shows exactly one visible version per natural key (that is the whole
-- point of as_of_facts), so restatement history is structurally invisible
-- there: you cannot count how many versions a key has ever had by looking at
-- a single point-in-time snapshot. This mart needs the full, unfiltered
-- version history of stg_gold to count restatements at all.
--
-- A restatement is a NEW filing (distinct adsh, later accepted) for the same
-- natural key (cik, tag, ddate, qtrs, uom). version_count is the number of
-- rows -- i.e. the number of distinct versions -- ever recorded for that key;
-- distinct_adsh is the number of distinct accession numbers among them. Only
-- keys that were restated at least once (version_count > 1) are emitted.

select
    cik,
    tag,
    ddate,
    qtrs,
    uom,
    count(*) as version_count,
    count(distinct adsh) as distinct_adsh,
    min(accepted) as first_accepted,
    max(accepted) as last_accepted
from {{ ref('stg_gold') }}
group by cik, tag, ddate, qtrs, uom
having count(*) > 1
