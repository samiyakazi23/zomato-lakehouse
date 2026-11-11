-- Average delivery should sit between 10 and 120 minutes for every city-day
select * from {{ ref('mart_city_kpis') }}
where avg_delivery_minutes is not null and (avg_delivery_minutes < 10 or avg_delivery_minutes > 120)
