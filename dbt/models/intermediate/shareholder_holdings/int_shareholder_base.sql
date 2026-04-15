{{ config(materialized='table', schema='intermediate', tags=['shareholder_holdings', 'intermediate']) }}

select
  {{ dagflow_bigint_key(["run_id", "filer_cik"]) }} as shareholder_id,
  pipeline_code,
  dataset_code,
  run_id,
  business_date,
  accession_number,
  filer_cik,
  filer_name,
  report_period,
  {{ dagflow_row_hash(["run_id", "filer_cik", "accession_number"]) }} as row_hash
from {{ ref('stg_13f_filers') }}
