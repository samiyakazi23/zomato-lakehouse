-- Restaurant leaderboard (last 90 days) enriched with listing attributes
with perf as (
    select * from {{ ref('stg_restaurant_performance') }}
),
listing as (
    select restaurant_id, locality, avg_cost_for_two, is_pure_veg, rating as listed_rating
    from {{ source('dims', 'restaurants') }}
)
select
    perf.*,
    listing.locality,
    listing.avg_cost_for_two,
    listing.is_pure_veg,
    listing.listed_rating,
    rank() over (partition by perf.city order by perf.gmv desc)   as city_gmv_rank,
    rank() over (order by perf.gmv desc)                          as national_gmv_rank,
    case
        when perf.avg_rating >= 4.2 and perf.cancel_rate < 0.04 then 'Star'
        when perf.avg_rating < 3.0 or perf.cancel_rate > 0.10   then 'At risk'
        else 'Steady'
    end as health_segment
from perf
left join listing using (restaurant_id)
