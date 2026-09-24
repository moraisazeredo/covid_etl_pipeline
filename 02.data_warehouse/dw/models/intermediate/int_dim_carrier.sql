-- models/intermediate/int_dim_carrier.sql

with base as (
    select
        carrier,
        carrier_name
    from {{ ref('stg_airline_delay_cause') }}
)

select
    carrier           as carrier_id,   -- Código da companhia (ex: AA, DL, UA)
    max(carrier_name) as carrier_name  -- Nome completo (ex: American Airlines)
from base
group by carrier