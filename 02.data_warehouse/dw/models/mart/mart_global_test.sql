-- models/mart/mart_global_daily_kpis.sql
-- KPIs consolidados dos países monitorados, 1 linha por dia de retrato
-- Responde: "Como está o total do grupo de países hoje e como mudou desde ontem?"

with fct as (
    select * from {{ ref('int_fct_covid_daily') }}
)

select
    date_id                    as snapshot_date,
    count(distinct country_id) as countries_reporting,  -- Quantos países vieram no retrato

    sum(cases)     as total_cases,
    sum(deaths)    as total_deaths,
    sum(recovered) as total_recovered,
    sum(active)    as total_active,
    sum(tests)     as total_tests,

    sum(new_cases)  as new_cases,   -- Null no primeiro dia de carga (não há retrato anterior)
    sum(new_deaths) as new_deaths,

    round(1.0 * sum(deaths) / nullif(sum(cases), 0), 4) as case_fatality_rate,
    round(1.0 * sum(cases)  / nullif(sum(tests), 0), 4) as test_positivity_rate

from fct
group by date_id
