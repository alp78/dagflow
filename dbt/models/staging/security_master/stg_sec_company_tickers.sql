{{ config(materialized='table', schema='staging', tags=['security_master', 'staging']) }}

with current_run as (
  select {{ dagflow_pipeline_run_id('security_master') }} as run_id
)

select
  raw_id,
  source_file_id,
  pipeline_code,
  dataset_code,
  run_id,
  business_date,
  source_record_id,
  source_payload,
  source_payload_hash,
  cik,
  ticker,
  company_name as issuer_name,
  exchange,
  row_hash,
  loaded_at
from {{ source('raw', 'sec_company_tickers') }}
where run_id = (select run_id from current_run)
