-- models/intermediate/int_dim_airport.sql

with base as (
    select
        airport,
        airport_name
    from {{ ref('stg_airline_delay_cause') }}
    -- Referencia o staging — o dbt garante que o staging roda primeiro
)

select
    airport           as airport_id,   -- Código IATA (ex: ATL, LAX)
    max(airport_name) as airport_name  -- max() desambigua linhas duplicadas com nomes ligeiramente diferentes
from base
group by airport  -- Cada aeroporto aparece uma única vez