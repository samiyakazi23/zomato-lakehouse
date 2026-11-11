-- Incremental model: each run only reprocesses the last 3 days (late streaming data can still
-- change them) and leaves older days untouched, instead of rebuilding the whole table.
{{ config(
    materialized='incremental',
    unique_key=['order_date', 'city'],
    incremental_strategy='delete+insert',
) }}
select
    order_date,
    city,
    orders,
    gmv,
    avg_order_value,
    cancel_rate,
    avg_delivery_minutes,
    avg_rating,
    late_rate,
    now() as loaded_at
from {{ ref('stg_daily_city_metrics') }}
{% if is_incremental() %}
where order_date >= (select max(order_date) - 3 from {{ this }})
{% endif %}
