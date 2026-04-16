-- depends_on: {{ ref('stg_13f_holdings') }}
-- depends_on: {{ ref('int_shareholder_base') }}
-- depends_on: {{ ref('dim_security') }}
{{ config(materialized='table', schema='intermediate', tags=['shareholder_holdings', 'intermediate']) }}

{% set security_run_id = var('dagflow_security_run_id', none) %}

with holdings as (
  select * from {{ ref('stg_13f_holdings') }}
),
filers as (
  select * from {{ ref('int_shareholder_base') }}
),
security as (
  select *
  from {{ ref('dim_security') }}
  {% if security_run_id %}
  where run_id = '{{ security_run_id }}'::uuid
  {% endif %}
)
select
  {{ dagflow_bigint_key(["h.run_id", "h.accession_number", "h.filer_cik", "h.security_identifier"]) }} as holding_id,
  f.shareholder_id,
  s.security_id,
  h.pipeline_code,
  h.dataset_code,
  h.run_id,
  h.business_date,
  h.source_record_id,
  h.accession_number,
  h.filer_cik,
  f.filer_name,
  h.security_identifier,
  s.issuer_name as security_name,
  h.shares_held as shares_held_raw,
  h.market_value as reviewed_market_value_raw,
  0.95::numeric(12, 6) as source_confidence_raw,
  s.shares_outstanding_raw,
  {{ dagflow_row_hash(["h.run_id", "h.accession_number", "h.filer_cik", "h.security_identifier", "h.shares_held", "h.market_value"]) }} as row_hash
from holdings h
join filers f
  on f.run_id = h.run_id
 and f.filer_cik = h.filer_cik
 and f.accession_number = h.accession_number
left join security s
  on s.business_date = h.business_date
 and s.ticker = h.security_identifier
