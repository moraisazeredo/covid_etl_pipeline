-- models/intermediate/int_dim_date.sql
-- Dimensão de datas: 1 linha por dia em que houve retrato (snapshot)

with dates as (
    select distinct snapshot_date
    from {{ ref('stg_covid_countries') }}
)

select
    snapshot_date                            as date_id,
    extract(year  from snapshot_date)::int   as year,
    extract(month from snapshot_date)::int   as month,
    extract(day   from snapshot_date)::int   as day,
    extract(isodow from snapshot_date)::int  as day_of_week,  -- 1 = segunda ... 7 = domingo
    to_char(snapshot_date, 'YYYY-MM')        as year_month
from dates
