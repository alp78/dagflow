{{ config(materialized='table', schema='staging', tags=['security_master', 'staging']) }}

with current_run as (
  select {{ dagflow_pipeline_run_id('security_master') }} as run_id
)

select
  raw_id,
  pipeline_code,
  dataset_code,
  run_id,
  business_date,
  source_record_id,
  source_payload_hash,
  cik,
  fact_name,
  fact_value,
  unit,
  row_hash,
  loaded_at
from {{ source('raw', 'sec_company_facts') }}
where fact_name = 'shares_outstanding'
  and run_id = (select run_id from current_run)
