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
    coalesce(
      nullif((t.source_payload ->> 'holder_count_recent_window')::numeric, 0),
      0
    ) as holder_count_recent_window,
    nullif((t.source_payload ->> 'reported_shares_recent_window')::numeric, 0) as reported_shares_recent_window,
    nullif((t.source_payload ->> 'top_holder_shares_recent_window')::numeric, 0) as top_holder_shares_recent_window,
    t.source_payload_hash,
    {{ dagflow_row_hash([
      "t.run_id",
      "t.cik",
      "t.ticker",
      "f.fact_value",
      "t.source_payload ->> 'reported_shares_recent_window'",
      "t.source_payload ->> 'top_holder_shares_recent_window'",
      "t.source_payload ->> 'holder_count_recent_window'",
    ]) }} as row_hash
  from tickers t
  left join facts f
    on t.run_id = f.run_id
   and t.business_date = f.business_date
   and t.cik = f.cik
)
select
  security_id,
  pipeline_code,
  dataset_code,
  run_id,
  business_date,
  source_record_id,
  cik,
  ticker,
  issuer_name,
  exchange,
  shares_outstanding_raw,
  case
    when shares_outstanding_raw is null or shares_outstanding_raw <= 0 then null
    when reported_shares_recent_window is null then null
    else round(
      greatest(
        0,
        least(
          1,
          1 - least(reported_shares_recent_window / nullif(shares_outstanding_raw, 0), 1)
        )
      ),
      6
    )
  end as free_float_pct_raw,
  case
    when reported_shares_recent_window is null or reported_shares_recent_window <= 0 then null
    when top_holder_shares_recent_window is null then null
    else round(
      greatest(
        0,
        least(
          1,
          1 - least(
            top_holder_shares_recent_window / nullif(reported_shares_recent_window, 0),
            1
          )
        )
      ),
      6
    )
  end as investability_factor_raw,
  source_payload_hash,
  row_hash
from joined
