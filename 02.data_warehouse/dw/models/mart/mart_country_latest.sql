-- models/mart/mart_country_latest.sql
-- Situação mais recente de cada país, com taxas e rankings
-- Responde: "Qual país teve mais mortes por milhão? Qual a letalidade em cada país?"

with fct as (
    select * from {{ ref('int_fct_covid_daily') }}
),
dim_country as (
    select * from {{ ref('int_dim_country') }}
),

latest as (
    select fct.*
    from fct
    join dim_country c
      on fct.country_id = c.country_id
     and fct.date_id    = c.last_snapshot_date  -- Só o retrato mais recente de cada país
)

select
    l.country_id,
    l.date_id as snapshot_date,
    c.population,

    l.cases,
    l.deaths,
    l.recovered,
    l.active,
    l.critical,
    l.tests,

    -- Letalidade: % de casos que resultaram em morte (nullif evita divisão por zero)
    round(1.0 * l.deaths    / nullif(l.cases, 0), 4) as case_fatality_rate,
    -- % de casos recuperados
    round(1.0 * l.recovered / nullif(l.cases, 0), 4) as recovery_rate,
    -- Positividade: % de testes que deram positivo
    round(1.0 * l.cases     / nullif(l.tests, 0), 4) as test_positivity_rate,
    -- % da população que teve COVID
    round(1.0 * l.cases     / nullif(c.population, 0), 4) as pct_population_infected,

    l.cases_per_one_million,
    l.deaths_per_one_million,
    l.tests_per_one_million,

    -- Rankings entre os países monitorados (1 = maior valor)
    rank() over (order by l.cases desc)                  as rank_cases,
    rank() over (order by l.deaths_per_one_million desc) as rank_deaths_per_million

from latest l
join dim_country c on l.country_id = c.country_id
