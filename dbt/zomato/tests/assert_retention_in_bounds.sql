-- Retention can never exceed 100%, and month 0 is always exactly 100%
select * from {{ ref('mart_customer_retention') }}
where retention < 0 or retention > 1 or (month_number = 0 and retention <> 1)
