with intermediate as (
    select * from {{ ref('int_energy_by_country') }}
)
select
    country,
    iso_code,
    year,
    avg_fossil_share,
    avg_renewables_share,
    total_ghg_emissions,
    avg_energy_per_capita,
    avg_carbon_intensity
from intermediate
where iso_code is not null
order by country, year