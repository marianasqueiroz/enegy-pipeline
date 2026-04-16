with staging as (
    select * from {{ ref('stg_energy') }}
)
select
    country,
    iso_code,
    year,
    round(solar_consumption, 2)       as solar_twh,
    round(wind_consumption, 2)        as wind_twh,
    round(hydro_consumption, 2)       as hydro_twh,
    round(nuclear_consumption, 2)     as nuclear_twh,
    round(renewables_consumption, 2)  as total_renewables_twh,
    round(coal_consumption, 2)        as coal_twh,
    round(gas_consumption, 2)         as gas_twh,
    round(oil_consumption, 2)         as oil_twh,
    round(fossil_fuel_consumption, 2) as total_fossil_twh,
    round(greenhouse_gas_emissions, 2) as ghg_emissions
from staging
where iso_code is not null
  and year >= 2000
order by country, year