-- Cuisine popularity and GMV share within each city
with perf as (
    select * from {{ ref('stg_restaurant_performance') }}
),
agg as (
    select city, cuisine, count(*) as restaurants, sum(orders) as orders, sum(gmv) as gmv,
           round(avg(avg_rating), 2) as avg_rating
    from perf
    group by 1, 2
)
select
    *,
    round(gmv / nullif(sum(gmv) over (partition by city), 0) * 100, 2) as gmv_share_pct,
    rank() over (partition by city order by gmv desc)                   as cuisine_rank
from agg
