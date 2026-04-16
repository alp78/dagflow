-- depends_on: {{ ref('stg_13f_filers') }}
{{ config(materialized='table', schema='intermediate', tags=['shareholder_holdings', 'intermediate']) }}

with ranked_filers as (
  select
    *,
    row_number() over (
      partition by run_id, filer_cik
      order by report_period desc nulls last, accession_number desc
    ) as filer_rank
  from {{ ref('stg_13f_filers') }}
)

select
  {{ dagflow_bigint_key(["filer_cik"]) }} as shareholder_id,
  pipeline_code,
  dataset_code,
  run_id,
  business_date,
  accession_number,
  filer_cik,
  filer_name,
  report_period,
  {{ dagflow_row_hash(["filer_cik", "filer_name", "report_period", "accession_number"]) }} as row_hash
from ranked_filers
where filer_rank = 1
