with src as (

    select
        snapshot_date,
        country,
        cases,
        deaths,
        recovered,
        active,
        critical,
        cases_per_one_million,
        deaths_per_one_million,
        tests,
        tests_per_one_million,
        population,
        one_case_per_people,
        one_death_per_people,
        one_test_per_people,
        active_per_one_million,
        recovered_per_one_million
    from {{ source('raw', 'covid_countries') }}  -- Tabela carregada pela task load_raw do Airflow

),

typed as (

    select
        cast(snapshot_date as date)    as snapshot_date,  -- Dia do retrato (a API devolve totais acumulados)
        cast(country as varchar)       as country,

        -- Contagens: bigint porque população e testes passam de 1 bilhão em alguns países
        cast(cases as bigint)          as cases,
        cast(deaths as bigint)         as deaths,
        cast(recovered as bigint)      as recovered,
        cast(active as bigint)         as active,
        cast(critical as bigint)       as critical,
        cast(tests as bigint)          as tests,
        cast(population as bigint)     as population,

        -- Taxas: numeric para manter as casas decimais
        cast(cases_per_one_million as numeric)     as cases_per_one_million,
        cast(deaths_per_one_million as numeric)    as deaths_per_one_million,
        cast(tests_per_one_million as numeric)     as tests_per_one_million,
        cast(active_per_one_million as numeric)    as active_per_one_million,
        cast(recovered_per_one_million as numeric) as recovered_per_one_million,

        -- "1 caso a cada N pessoas"
        cast(one_case_per_people as integer)  as one_case_per_people,
        cast(one_death_per_people as integer) as one_death_per_people,
        cast(one_test_per_people as integer)  as one_test_per_people

    from src
)

select *
from typed
