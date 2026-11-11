{% snapshot snap_restaurants %}
{#
  SCD Type 2 history of restaurant listings. Each time a restaurant's price, rating, veg flag or
  prep time changes, the old row is closed (dbt_valid_to) and a new row opened (dbt_valid_from),
  so we can answer "what did this restaurant cost in March?" or "who raised prices this month?".
#}
{{ config(
    target_schema='analytics',
    unique_key='restaurant_id',
    strategy='check',
    check_cols=['rating', 'avg_cost_for_two', 'is_pure_veg', 'prep_time_min'],
) }}
select restaurant_id, restaurant_name, city, locality, cuisine, rating, avg_cost_for_two, is_pure_veg, prep_time_min
from {{ source('dims', 'restaurants') }}
{% endsnapshot %}
