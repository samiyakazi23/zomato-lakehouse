select order_date, city, count(*) as n
from {{ ref('mart_city_kpis') }}
group by 1, 2
having count(*) > 1
