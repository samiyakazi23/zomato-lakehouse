-- Cohort retention: share of each monthly sign-up cohort still ordering N months later
select
    cohort,
    month_number,
    customers,
    cohort_size,
    round(customers::numeric / nullif(cohort_size, 0), 4) as retention
from {{ source('gold', 'customer_cohorts') }}
