-- Fails if any city-day reports negative GMV
select * from {{ ref('mart_city_kpis') }} where gmv < 0
