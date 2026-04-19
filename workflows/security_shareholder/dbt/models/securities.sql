{{ config(materialized='table', schema='marts', tags=['security_shareholder']) }}

with tickers as (
    select raw_id, source_file_id, run_id, business_date,
           cik, ticker, company_name as issuer_name, exchange,
           source_payload_hash, row_hash,
           (source_payload::jsonb ->> 'holder_count_recent_window')::numeric as holder_count_recent_window,
           (source_payload::jsonb ->> 'reported_shares_recent_window')::numeric as reported_shares_recent_window,
           (source_payload::jsonb ->> 'top_holder_shares_recent_window')::numeric as top_holder_shares_recent_window
    from {{ source('raw', 'sec_company_tickers') }}
    where run_id = {{ dagflow_pipeline_run_id("security_shareholder") }}
),

facts as (
    select run_id, business_date, cik, fact_value::numeric as shares_outstanding_raw
    from {{ source('raw', 'sec_company_facts') }}
    where run_id = {{ dagflow_pipeline_run_id("security_shareholder") }}
      and fact_name = 'shares_outstanding'
),

base as (
    select
        {{ dagflow_bigint_key(["t.cik", "t.ticker"]) }} as security_id,
        t.run_id, t.business_date, t.cik, t.ticker, t.issuer_name, t.exchange,
        t.source_file_id, t.source_payload_hash, t.row_hash,
        coalesce(f.shares_outstanding_raw, 0) as shares_outstanding_raw,
        t.holder_count_recent_window,
        t.reported_shares_recent_window,
        t.top_holder_shares_recent_window,
        case
            when coalesce(f.shares_outstanding_raw, 0) <= 0 then null
            else greatest(0, least(1, 1.0 - least(coalesce(t.reported_shares_recent_window, 0) / nullif(f.shares_outstanding_raw, 0), 1.0)))
        end as free_float_pct_raw,
        case
            when coalesce(t.reported_shares_recent_window, 0) <= 0 then null
            else greatest(0, least(1, 1.0 - least(coalesce(t.top_holder_shares_recent_window, 0) / nullif(t.reported_shares_recent_window, 0), 1.0)))
        end as investability_factor_raw
    from tickers t
    left join facts f on t.run_id = f.run_id and t.business_date = f.business_date and t.cik = f.cik
)

select
    security_id, 'security_shareholder' as pipeline_code, 'security_master' as dataset_code,
    run_id, business_date, ticker as source_record_id, cik, ticker, issuer_name, exchange, source_file_id,
    shares_outstanding_raw, free_float_pct_raw, investability_factor_raw,
    round(coalesce(shares_outstanding_raw, 0) * coalesce(free_float_pct_raw, 0), 6) as free_float_shares,
    round(coalesce(shares_outstanding_raw, 0) * coalesce(free_float_pct_raw, 0) * coalesce(investability_factor_raw, 0), 6) as investable_shares,
    round(coalesce(shares_outstanding_raw, 0) * coalesce(free_float_pct_raw, 0) * coalesce(investability_factor_raw, 0) / 1000000.0, 6) as review_materiality_score,
    md5(coalesce(cik,'') || '|' || coalesce(ticker,'') || '|' || coalesce(issuer_name,'') || '|' || coalesce(exchange,'') || '|' || coalesce(shares_outstanding_raw::text,'')) as row_hash
from base
