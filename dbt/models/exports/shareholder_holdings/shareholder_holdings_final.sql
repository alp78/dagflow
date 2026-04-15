{{ config(materialized='table', schema='export', tags=['shareholder_holdings', 'exports']) }}

select
  review_row_id as origin_review_row_id,
  current_row_hash as origin_review_row_hash,
  run_id,
  business_date,
  filer_name,
  security_name,
  coalesce(shares_held_override, shares_held_raw) as shares_held,
  holding_pct_of_outstanding,
  gen_random_uuid() as export_batch_id,
  concat('shareholder_holdings_', business_date::text, '.csv') as file_id
from {{ source('review', 'shareholder_holdings_daily') }}
where approval_state in ('approved', 'exported')
