select
    order_date::date                    as order_date,
    rider_id,
    city,
    deliveries::bigint                  as deliveries,
    avg_delivery_minutes::numeric(8, 2) as avg_delivery_minutes,
    distance_km::numeric(10, 2)         as distance_km,
    avg_rating::numeric(4, 2)           as avg_rating
from {{ source('gold', 'rider_daily') }}
