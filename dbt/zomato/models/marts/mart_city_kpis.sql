-- Daily KPIs per city with 7-day rolling metrics and week-over-week growth
with daily as (
    select * from {{ ref('stg_daily_city_metrics') }}
)
select
    order_date,
    city,
    orders,
    delivered_orders,
    cancelled_orders,
    gmv,
    avg_order_value,
    cancel_rate,
    avg_delivery_minutes,
    p90_delivery_minutes,
    avg_rating,
    unique_customers,
    gold_share,
    sum(gmv)    over w7 as gmv_7d,
    round(avg(orders) over w7, 1) as orders_7d_avg,
    lag(gmv, 7) over w as gmv_same_day_last_week,
    case when lag(gmv, 7) over w > 0
         then round((gmv / lag(gmv, 7) over w - 1) * 100, 2)
    end as wow_growth_pct
from daily
window
    w  as (partition by city order by order_date),
    w7 as (partition by city order by order_date rows between 6 preceding and current row)
