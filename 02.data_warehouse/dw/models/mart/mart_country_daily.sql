-- models/mart/mart_country_daily.sql
-- Evolução diária por país, com média móvel de 7 retratos
-- Responde: "Os casos estão subindo ou caindo em cada país?"

with fct as (
    select * from {{ ref('int_fct_covid_daily') }}
),
dim_date as (
    select * from {{ ref('int_dim_date') }}
)

select
    f.date_id    as snapshot_date,
    d.year,
    d.month,
    d.year_month,
    f.country_id,

    f.cases,
    f.deaths,
    f.new_cases,
    f.new_deaths,
    f.new_tests,

    -- Média móvel suaviza oscilações de um dia (ex: fins de semana sem atualização)
    round(avg(f.new_cases)  over w7, 2) as new_cases_7d_avg,
    round(avg(f.new_deaths) over w7, 2) as new_deaths_7d_avg

from fct f
join dim_date d on f.date_id = d.date_id
window w7 as (
    partition by f.country_id
    order by f.date_id
    rows between 6 preceding and current row  -- Retrato atual + 6 anteriores
)
