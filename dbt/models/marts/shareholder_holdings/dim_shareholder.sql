{{ config(materialized='table', schema='marts', tags=['shareholder_holdings', 'marts']) }}

select
  shareholder_id,
  pipeline_code,
  dataset_code,
  run_id,
  business_date,
  accession_number,
  filer_cik,
  filer_name,
  report_period,
  row_hash
from {{ ref('int_shareholder_base') }}
