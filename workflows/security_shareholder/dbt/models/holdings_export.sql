{{ config(materialized='table', schema='export', alias='shareholder_holdings_final', tags=['security_shareholder', 'exports']) }}

select
    run_id,
    review_row_id,
    current_row_hash as review_row_hash,
    business_date,
    filer_name,
    security_name,
    coalesce(shares_held_override, shares_held_raw, 0) as shares_held,
    holding_pct_of_outstanding,
    md5(run_id::text || '|' || review_row_id::text) as _row_export_key
from {{ source('review', 'shareholder_holdings_daily') }}
where approval_state in ('approved', 'exported')
  and run_id = {{ dagflow_pipeline_run_id("security_shareholder") }}
