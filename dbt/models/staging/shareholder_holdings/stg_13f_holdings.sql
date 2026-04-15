{{ config(materialized='table', schema='staging', tags=['shareholder_holdings', 'staging']) }}

select
  raw_id,
  pipeline_code,
  dataset_code,
  run_id,
  business_date,
  source_record_id,
  accession_number,
  filer_cik,
  security_identifier,
  cusip,
  shares_held,
  market_value,
  source_payload_hash,
  row_hash,
  loaded_at
from {{ source('raw', 'holdings_13f') }}
