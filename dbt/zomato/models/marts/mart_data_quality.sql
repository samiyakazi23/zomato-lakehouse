-- Rejected (quarantined) events per day and reason
select quarantine_date::date as quarantine_date, dq_reason, rejected_rows::bigint as rejected_rows
from {{ source('gold', 'dq_quarantine_daily') }}
