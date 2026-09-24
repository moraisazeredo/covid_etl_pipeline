-- models/intermediate/int_dim_month.sql

with base as (
    select distinct          -- distinct: cada combinação única de ano+mês
        year,
        month,
        year_month_key       -- Chave criada no staging (ex: 202205)
    from {{ ref('stg_airline_delay_cause') }}
)

select
    year_month_key as month_id,  -- Chave primária da dimensão de tempo
    year,
    month
from base