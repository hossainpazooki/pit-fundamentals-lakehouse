-- Materialized as a view via the project default (dbt_project.yml sets
-- models.vantage.staging.+materialized: view); no local config block needed.
--
-- This is the ONLY model that may touch delta_scan(). It is a pure
-- passthrough of the PIT gold table: the 11 contract columns, selected
-- explicitly BY NAME (never SELECT *) so an upstream schema change fails
-- loudly here instead of silently widening every downstream model.

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
from delta_scan('{{ var("gold_path") }}')
