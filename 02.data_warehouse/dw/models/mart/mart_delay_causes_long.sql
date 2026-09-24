-- models/mart/mart_delay_causes_long.sql

with fct as (
    select * from {{ ref('int_fct_flight_delays') }}
),

-- Técnica de unpivot manual via UNION ALL:
-- cada bloco SELECT transforma uma coluna em uma linha com label "cause"
unpivoted as (
    select month_id, carrier_id, airport_id, 'carrier'       as cause, carrier_delay       as delay_minutes from fct
    union all
    select month_id, carrier_id, airport_id, 'weather'       as cause, weather_delay       as delay_minutes from fct
    union all
    select month_id, carrier_id, airport_id, 'nas'           as cause, nas_delay           as delay_minutes from fct
    union all
    select month_id, carrier_id, airport_id, 'security'      as cause, security_delay      as delay_minutes from fct
    union all
    select month_id, carrier_id, airport_id, 'late_aircraft' as cause, late_aircraft_delay as delay_minutes from fct
)

select * from unpivoted
-- Resultado: 5x mais linhas que a fato, mas com coluna "cause" filtrável