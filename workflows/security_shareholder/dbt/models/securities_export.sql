{{ config(materialized='table', schema='export', alias='security_master_final', tags=['security_shareholder', 'exports']) }}

select
    run_id,
    review_row_id,
    current_row_hash as review_row_hash,
    business_date,
    coalesce(ticker, 'UNKNOWN') as ticker,
    issuer_name,
    investable_shares,
    review_materiality_score,
    md5(run_id::text || '|' || review_row_id::text) as _row_export_key
from {{ source('review', 'security_master_daily') }}
where approval_state in ('approved', 'exported')
  and run_id = {{ dagflow_pipeline_run_id("security_shareholder") }}
