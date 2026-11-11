-- How lateness and cuisine move ratings
select
    late_bucket,
    cuisine,
    sum(reviews)                                             as reviews,
    round(sum(rating * reviews)::numeric / nullif(sum(reviews), 0), 2) as avg_rating,
    round(sum(case when rating <= 2 then reviews else 0 end)::numeric / nullif(sum(reviews), 0), 4) as low_rating_share
from {{ source('gold', 'rating_matrix') }}
group by 1, 2
