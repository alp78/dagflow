-- depends_on: {{ ref('int_holding_with_security') }}
{{ config(materialized='table', schema='marts', tags=['shareholder_holdings', 'marts']) }}

with holdings as (
  select * from {{ ref('int_holding_with_security') }}
),
portfolio_totals as (
  select
    run_id,
    filer_cik,
    sum(reviewed_market_value_raw) as total_market_value
  from holdings
  group by 1, 2
)
select
  h.holding_id,
  h.shareholder_id,
  h.security_id,
  h.pipeline_code,
  h.dataset_code,
  h.run_id,
  h.business_date,
  h.source_record_id,
  h.accession_number,
  h.filer_cik,
  h.filer_name,
  h.security_identifier,
  h.security_name,
  h.shares_held_raw,
  h.reviewed_market_value_raw,
  h.source_confidence_raw,
  round(h.reviewed_market_value_raw / nullif(h.shares_held_raw, 0), 6) as derived_price_per_share,
  round(h.shares_held_raw / nullif(h.shares_outstanding_raw, 0), 8) as holding_pct_of_outstanding,
  round(h.reviewed_market_value_raw / nullif(p.total_market_value, 0), 6) as portfolio_weight,
  {{ dagflow_row_hash(["h.holding_id", "h.run_id", "h.security_identifier", "h.shares_held_raw", "h.reviewed_market_value_raw"]) }} as row_hash
from holdings h
left join portfolio_totals p
  on p.run_id = h.run_id
 and p.filer_cik = h.filer_cik
