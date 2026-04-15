{{ config(materialized='table', schema='marts', tags=['security_master', 'marts']) }}

select
  security_id,
  pipeline_code,
  dataset_code,
  run_id,
  business_date,
  source_record_id,
  cik,
  ticker,
  issuer_name,
  exchange,
  shares_outstanding_raw,
  free_float_pct_raw,
  investability_factor_raw,
  free_float_shares,
  investable_shares,
  review_materiality_score,
  {{ dagflow_row_hash(["security_id", "run_id", "ticker", "investable_shares"]) }} as row_hash
from {{ ref('int_security_attributes') }}
