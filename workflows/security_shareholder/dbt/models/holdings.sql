{{ config(materialized='table', schema='marts', tags=['security_shareholder']) }}

with filers as (
    select *, row_number() over (partition by run_id, filer_cik order by report_period desc nulls last, accession_number desc) as rn
    from {{ source('raw', 'holdings_13f_filers') }}
    where run_id = {{ dagflow_pipeline_run_id("security_shareholder") }}
),

active_filers as (
    select run_id, business_date, accession_number, filer_cik, filer_name, report_period,
           {{ dagflow_bigint_key(["filer_cik"]) }} as shareholder_id
    from filers where rn = 1
),

raw_holdings as (
    select raw_id, source_file_id, run_id, business_date,
           accession_number, filer_cik, security_identifier, cusip,
           coalesce(shares_held::numeric, 0) as shares_held,
           coalesce(market_value::numeric, 0) as market_value,
           source_payload_hash, row_hash
    from {{ source('raw', 'holdings_13f') }}
    where run_id = {{ dagflow_pipeline_run_id("security_shareholder") }}
),

securities as (
    select security_id, ticker, issuer_name, shares_outstanding_raw, business_date
    from {{ ref('securities') }}
),

portfolio_totals as (
    select run_id, filer_cik, sum(market_value) as total_market_value
    from raw_holdings
    group by run_id, filer_cik
),

enriched as (
    select
        {{ dagflow_bigint_key(["h.run_id", "h.accession_number", "h.filer_cik", "h.security_identifier"]) }} as holding_id,
        'security_shareholder' as pipeline_code,
        'shareholder_holdings' as dataset_code,
        h.run_id, h.business_date, h.accession_number, h.filer_cik, h.security_identifier,
        h.cusip, h.source_file_id, h.source_payload_hash, h.row_hash,
        f.filer_name, f.shareholder_id, f.report_period,
        s.security_id, coalesce(s.issuer_name, h.security_identifier) as security_name,
        h.security_identifier as source_record_id,
        h.shares_held as shares_held_raw,
        h.market_value as reviewed_market_value_raw,
        coalesce(s.shares_outstanding_raw, 0) as shares_outstanding_raw,
        0.50::numeric as source_confidence_raw,
        case when h.shares_held > 0 and h.market_value > 0 then round(h.market_value / h.shares_held, 6) else null end as derived_price_per_share,
        case when coalesce(s.shares_outstanding_raw, 0) > 0 then round(h.shares_held / s.shares_outstanding_raw, 8) else null end as holding_pct_of_outstanding,
        case when coalesce(pt.total_market_value, 0) > 0 then round(h.market_value / pt.total_market_value, 8) else null end as portfolio_weight
    from raw_holdings h
    inner join active_filers f on f.run_id = h.run_id and f.filer_cik = h.filer_cik and f.accession_number = h.accession_number
    left join securities s on s.business_date = h.business_date and s.ticker = h.security_identifier
    left join portfolio_totals pt on pt.run_id = h.run_id and pt.filer_cik = h.filer_cik
)

select * from enriched
