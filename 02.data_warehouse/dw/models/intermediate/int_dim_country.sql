-- models/intermediate/int_dim_country.sql
-- Dimensão de países: 1 linha por país, com os atributos do retrato mais recente

with ranked as (
    select
        country,
        population,
        snapshot_date,
        -- 1 = retrato mais recente de cada país
        row_number() over (partition by country order by snapshot_date desc) as rn
    from {{ ref('stg_covid_countries') }}
)

select
    country       as country_id,    -- Nome do país na API (ex: Brazil, USA)
    population,                     -- População no retrato mais recente
    snapshot_date as last_snapshot_date
from ranked
where rn = 1
