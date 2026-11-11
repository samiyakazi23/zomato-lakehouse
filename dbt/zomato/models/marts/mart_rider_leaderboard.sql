-- Rider performance over the last 7 days of data
with recent as (
    select *
    from {{ ref('stg_rider_daily') }}
    where order_date > (select max(order_date) - 7 from {{ ref('stg_rider_daily') }})
),
agg as (
    select
        r.rider_id,
        d.rider_name,
        coalesce(d.city, max(r.city)) as city,
        d.vehicle_type,
        sum(r.deliveries)                                                       as deliveries,
        round(sum(r.avg_delivery_minutes * r.deliveries) / sum(r.deliveries), 2) as avg_delivery_minutes,
        sum(r.distance_km)                                                       as distance_km,
        round(avg(r.avg_rating), 2)                                              as avg_rating
    from recent r
    left join {{ source('dims', 'riders') }} d using (rider_id)
    group by r.rider_id, d.rider_name, d.city, d.vehicle_type
)
select *, rank() over (partition by city order by deliveries desc, avg_delivery_minutes) as city_rank
from agg
