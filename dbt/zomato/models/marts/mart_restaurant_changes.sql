-- Every price or rating change captured by the SCD2 snapshot, newest first
with versions as (
    select
        *,
        row_number() over w                as version,
        lag(avg_cost_for_two) over w       as previous_cost,
        lag(rating) over w                 as previous_rating
    from {{ ref('snap_restaurants') }}
    window w as (partition by restaurant_id order by dbt_valid_from)
)
select
    restaurant_id,
    restaurant_name,
    city,
    cuisine,
    dbt_valid_from                         as changed_at,
    previous_cost,
    avg_cost_for_two                       as new_cost,
    previous_rating,
    rating                                 as new_rating,
    case
        when previous_cost is distinct from avg_cost_for_two and previous_rating is distinct from rating then 'Price and rating'
        when previous_cost is distinct from avg_cost_for_two then 'Price'
        else 'Rating'
    end                                    as change_type,
    dbt_valid_to is null                   as is_current
from versions
where version > 1
