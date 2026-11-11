select
    order_date::date                         as order_date,
    city,
    orders::bigint                           as orders,
    delivered_orders::bigint                 as delivered_orders,
    cancelled_orders::bigint                 as cancelled_orders,
    gmv::numeric(16, 2)                      as gmv,
    avg_order_value::numeric(10, 2)          as avg_order_value,
    cancel_rate::numeric(6, 4)               as cancel_rate,
    avg_delivery_minutes::numeric(8, 2)      as avg_delivery_minutes,
    p90_delivery_minutes::numeric(8, 2)      as p90_delivery_minutes,
    avg_rating::numeric(4, 2)                as avg_rating,
    unique_customers::bigint                 as unique_customers,
    gold_share::numeric(6, 4)                as gold_share,
    total_discount::numeric(16, 2)           as total_discount,
    late_rate::numeric(6, 4)                 as late_rate
from {{ source('gold', 'daily_city_metrics') }}
