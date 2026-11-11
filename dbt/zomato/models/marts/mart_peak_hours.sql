-- Demand index by hour of day (IST): 1.0 = the city's average hour
with h as (
    select city, hour_of_day, sum(orders) as orders, round(avg(avg_order_value), 2) as avg_order_value
    from {{ ref('stg_hourly_demand') }}
    group by 1, 2
)
select
    city,
    hour_of_day,
    orders,
    avg_order_value,
    round(orders::numeric / nullif(avg(orders) over (partition by city), 0), 2) as demand_index
from h
