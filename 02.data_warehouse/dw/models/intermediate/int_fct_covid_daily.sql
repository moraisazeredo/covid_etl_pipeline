-- models/intermediate/int_fct_covid_daily.sql
-- Fato diária: 1 linha por país × dia de retrato
-- A API devolve totais ACUMULADOS; as colunas new_* calculam a variação desde o retrato anterior

with stg as (
    select * from {{ ref('stg_covid_countries') }}
),

with_previous as (
    select
        stg.*,
        -- Valor do retrato anterior do mesmo país (null no primeiro dia de carga)
        lag(cases)     over w as prev_cases,
        lag(deaths)    over w as prev_deaths,
        lag(recovered) over w as prev_recovered,
        lag(tests)     over w as prev_tests
    from stg
    window w as (partition by country order by snapshot_date)
)

select
    -- Chaves estrangeiras
    snapshot_date as date_id,     -- FK → int_dim_date
    country       as country_id,  -- FK → int_dim_country

    -- Totais acumulados
    cases,
    deaths,
    recovered,
    active,
    critical,
    tests,

    -- Variação desde o retrato anterior (null no primeiro dia; pode ser negativa se a fonte revisar números)
    cases     - prev_cases     as new_cases,
    deaths    - prev_deaths    as new_deaths,
    recovered - prev_recovered as new_recovered,
    tests     - prev_tests     as new_tests,

    -- Taxas por milhão (calculadas pela própria API)
    cases_per_one_million,
    deaths_per_one_million,
    tests_per_one_million,
    active_per_one_million,
    recovered_per_one_million

from with_previous
