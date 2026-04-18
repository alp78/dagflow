-- depends_on: {{ ref('dim_security') }}
{{ config(tags=['security_master']) }}

select
  run_id,
  security_id,
  count(*) as row_count
from {{ ref('dim_security') }}
group by 1, 2
having count(*) > 1
