select
    restaurant_id,
    restaurant_name,
    city,
    cuisine,
    orders::bigint                      as orders,
    gmv::numeric(16, 2)                 as gmv,
    avg_order_value::numeric(10, 2)     as avg_order_value,
    avg_rating::numeric(4, 2)           as avg_rating,
    ratings_count::bigint               as ratings_count,
    cancel_rate::numeric(6, 4)          as cancel_rate,
    avg_delivery_minutes::numeric(8, 2) as avg_delivery_minutes
from {{ source('gold', 'restaurant_performance') }}
