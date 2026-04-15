{{ config(materialized='table', schema='intermediate', tags=['security_master', 'intermediate']) }}

with tickers as (
  select * from {{ ref('stg_sec_company_tickers') }}
),
facts as (
  select * from {{ ref('stg_sec_company_facts') }}
),
joined as (
  select
    {{ dagflow_bigint_key(["t.cik", "t.ticker"]) }} as security_id,
    t.pipeline_code,
    t.dataset_code,
    t.run_id,
    t.business_date,
    t.source_record_id,
    t.cik,
    t.ticker,
    t.issuer_name,
    t.exchange,
    f.fact_value as shares_outstanding_raw,
    0.90::numeric(12, 6) as free_float_pct_raw,
    0.98::numeric(12, 6) as investability_factor_raw,
    t.source_payload_hash,
    {{ dagflow_row_hash(["t.run_id", "t.cik", "t.ticker", "f.fact_value"]) }} as row_hash
  from tickers t
  left join facts f
    on t.run_id = f.run_id
   and t.business_date = f.business_date
   and t.cik = f.cik
)
select * from joined
