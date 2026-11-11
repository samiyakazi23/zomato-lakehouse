select * from {{ ref('mart_city_kpis') }} where cancel_rate < 0 or cancel_rate > 1
