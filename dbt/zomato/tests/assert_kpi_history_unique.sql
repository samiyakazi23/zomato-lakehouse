-- The incremental model must never duplicate a city-day
select order_date, city, count(*) from {{ ref('mart_kpi_history') }} group by 1, 2 having count(*) > 1
