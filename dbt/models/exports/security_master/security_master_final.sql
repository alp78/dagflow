{{ config(materialized='table', schema='export', alias='security_master_preview', tags=['security_master', 'exports']) }}

select
  review_row_id as origin_review_row_id,
  current_row_hash as origin_review_row_hash,
  run_id,
  business_date,
  coalesce(ticker, 'UNKNOWN') as ticker,
  issuer_name,
  investable_shares,
  review_materiality_score,
  gen_random_uuid() as export_batch_id,
  concat('security_master_', business_date::text, '.csv') as file_id
from {{ source('review', 'security_master_daily') }}
where run_id = {{ dagflow_pipeline_run_id('security_master') }}
  and approval_state in ('approved', 'exported')
