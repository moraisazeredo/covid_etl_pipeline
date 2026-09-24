-- models/intermediate/int_fct_flight_delays.sql

with stg as (
    select * from {{ ref('stg_airline_delay_cause') }}
),

fct as (
    select
        -- Chaves estrangeiras que ligam às dimensões
        stg.year_month_key as month_id,    -- FK → int_dim_month
        stg.carrier        as carrier_id,  -- FK → int_dim_carrier
        stg.airport        as airport_id,  -- FK → int_dim_airport

        -- Métricas de volume de voos
        stg.arr_flights,    -- Total de voos que chegaram
        stg.arr_del15,      -- Voos atrasados 15+ minutos
        stg.arr_cancelled,  -- Voos cancelados
        stg.arr_diverted,   -- Voos desviados

        -- Minutos totais de atraso por causa
        stg.arr_delay,             -- Atraso total
        stg.carrier_delay,         -- Culpa da companhia (manutenção, tripulação)
        stg.weather_delay,         -- Condições climáticas
        stg.nas_delay,             -- Sistema aéreo nacional (controle de tráfego)
        stg.security_delay,        -- Segurança/triagem
        stg.late_aircraft_delay,   -- Aeronave atrasada de voo anterior

        -- Contagem de ocorrências por causa (fracionais)
        stg.carrier_ct,
        stg.weather_ct,
        stg.nas_ct,
        stg.security_ct,
        stg.late_aircraft_ct

    from stg
)

select * from fct