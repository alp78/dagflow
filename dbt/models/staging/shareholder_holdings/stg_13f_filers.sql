{{ config(materialized='table', schema='staging', tags=['shareholder_holdings', 'staging']) }}

with current_run as (
  select {{ dagflow_pipeline_run_id('shareholder_holdings') }} as run_id
)

select
  raw_id,
  pipeline_code,
  dataset_code,
  run_id,
  business_date,
  source_record_id,
  accession_number,
  filer_cik,
  filer_name,
  report_period,
  source_payload_hash,
  row_hash,
  loaded_at
from {{ source('raw', 'holdings_13f_filers') }}
where run_id = (select run_id from current_run)
