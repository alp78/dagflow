{{ config(materialized='table', schema='intermediate', tags=['security_master', 'intermediate']) }}

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
  round(shares_outstanding_raw * free_float_pct_raw, 6) as free_float_shares,
  round(shares_outstanding_raw * free_float_pct_raw * investability_factor_raw, 6) as investable_shares,
  round((shares_outstanding_raw * free_float_pct_raw * investability_factor_raw) / 1000000.0, 6) as review_materiality_score,
  row_hash
from {{ ref('int_security_base') }}
