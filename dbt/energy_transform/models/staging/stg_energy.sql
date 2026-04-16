with source as (
    select * from {{ source('energy_data', 'energy_staging') }}
)
select
    country,
    year,
    iso_code,
    population,
    gdp,
    primary_energy_consumption,
    energy_per_capita,
    fossil_fuel_consumption,
    fossil_share_energy,
    renewables_consumption,
    renewables_share_energy,
    coal_consumption,
    gas_consumption,
    oil_consumption,
    solar_consumption,
    wind_consumption,
    hydro_consumption,
    nuclear_consumption,
    greenhouse_gas_emissions,
    carbon_intensity_elec,
    electricity_generation,
    ingestion_date
from source
where country is not null
  and year is not null