select
    city,
    day_of_week,
    hour_of_day::int                as hour_of_day,
    orders::bigint                  as orders,
    avg_order_value::numeric(10, 2) as avg_order_value
from {{ source('gold', 'hourly_demand') }}
