select *
from {{ ref('dim_security') }}
where review_materiality_score < 0
