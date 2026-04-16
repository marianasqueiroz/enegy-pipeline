with staging as (
    select * from {{ ref('stg_energy') }}
)
select
    country,
    iso_code,
    year,
    round(avg(fossil_share_energy), 2)      as avg_fossil_share,
    round(avg(renewables_share_energy), 2)  as avg_renewables_share,
    round(sum(greenhouse_gas_emissions), 2) as total_ghg_emissions,
    round(avg(energy_per_capita), 2)        as avg_energy_per_capita,
    round(avg(carbon_intensity_elec), 2)    as avg_carbon_intensity
from staging
group by country, iso_code, year