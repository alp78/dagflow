-- Add a validity issue when total institutional holdings exceed shares outstanding.
-- This typically indicates a multi-class share problem where the wrong class
-- is reported in the SEC XBRL data.

drop function if exists audit.fn_current_security_review_issues(uuid, bigint);

create function audit.fn_current_security_review_issues(
    p_run_id uuid default null,
    p_review_row_id bigint default null
)
returns table (
    pipeline_code text,
    dataset_code text,
    run_id uuid,
    business_date date,
    review_table text,
    review_row_id bigint,
    current_row_hash text,
    issue_code text,
    issue_message text,
    offending_columns text[],
    row_snapshot jsonb,
    issue_category text
)
language sql
stable
security definer
as
$$
with base as (
    select
        s.pipeline_code,
        s.dataset_code,
        s.run_id,
        s.business_date,
        s.ticker,
        'review.security_master_daily'::text as review_table,
        s.review_row_id,
        s.current_row_hash,
        to_jsonb(s) as row_snapshot,
        coalesce(s.shares_outstanding_override, s.shares_outstanding_raw) as shares_outstanding_value,
        coalesce(s.free_float_pct_override, s.free_float_pct_raw) as free_float_pct_value,
        coalesce(s.investability_factor_override, s.investability_factor_raw) as investability_factor_value,
        s.free_float_shares,
        s.investable_shares
    from review.security_master_daily s
    where (p_run_id is null or s.run_id = p_run_id)
      and (p_review_row_id is null or s.review_row_id = p_review_row_id)
),
holdings_agg as (
    select
        h.security_review_row_id,
        coalesce(sum(coalesce(h.shares_held_override, h.shares_held_raw, 0)), 0) as total_held
    from review.shareholder_holdings_daily h
    where h.security_review_row_id in (select review_row_id from base)
    group by h.security_review_row_id
),
prior_period as (
    select distinct on (prior.pipeline_code, prior.ticker)
        prior.pipeline_code,
        prior.ticker,
        coalesce(prior.shares_outstanding_override, prior.shares_outstanding_raw) as prior_shares_outstanding,
        coalesce(prior.free_float_pct_override, prior.free_float_pct_raw) as prior_free_float_pct,
        coalesce(prior.investability_factor_override, prior.investability_factor_raw) as prior_investability_factor
    from review.security_master_daily prior
    where prior.pipeline_code = (select pipeline_code from base limit 1)
      and prior.business_date < (select business_date from base limit 1)
      and prior.ticker in (select ticker from base)
    order by prior.pipeline_code, prior.ticker, prior.business_date desc
)

-- validity: missing ticker
select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'missing_ticker'::text, 'Ticker is missing.'::text,
    array['ticker']::text[], row_snapshot, 'validity'::text
from base where coalesce(nullif(btrim(row_snapshot ->> 'ticker'), ''), '') = ''

union all

-- validity: missing issuer name
select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'missing_issuer_name', 'Issuer name is missing.',
    array['issuer_name']::text[], row_snapshot, 'validity'
from base where coalesce(nullif(btrim(row_snapshot ->> 'issuer_name'), ''), '') = ''

union all

-- validity: invalid shares outstanding
select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_shares_outstanding', 'Shares outstanding is missing or non-positive.',
    array['shares_outstanding_raw']::text[], row_snapshot, 'validity'
from base where coalesce(shares_outstanding_value, 0) <= 0

union all

-- validity: holdings exceed outstanding (multi-class share problem)
select b.pipeline_code, b.dataset_code, b.run_id, b.business_date, b.review_table, b.review_row_id, b.current_row_hash,
    'holdings_exceed_outstanding',
    format('Total institutional holdings (%s) exceed shares outstanding (%s). Likely a multi-class share issue — update shares outstanding to the correct class.',
           to_char(ha.total_held, 'FM999,999,999,999'), to_char(b.shares_outstanding_value, 'FM999,999,999,999')),
    array['shares_outstanding_raw', 'free_float_pct_raw']::text[], b.row_snapshot, 'validity'
from base b
join holdings_agg ha on ha.security_review_row_id = b.review_row_id
where b.shares_outstanding_value > 0 and ha.total_held > b.shares_outstanding_value

union all

-- validity: invalid free float pct
select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_free_float_pct', 'Free float percentage is zero or negative despite positive outstanding shares.',
    array['free_float_pct_raw']::text[], row_snapshot, 'validity'
from base where shares_outstanding_value > 0 and coalesce(free_float_pct_value, 0) <= 0
  and review_row_id not in (select b2.review_row_id from base b2 join holdings_agg ha2 on ha2.security_review_row_id = b2.review_row_id where ha2.total_held > b2.shares_outstanding_value)

union all

-- validity: invalid investability
select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_investability_factor', 'Investability factor is zero or negative.',
    array['investability_factor_raw']::text[], row_snapshot, 'validity'
from base where coalesce(investability_factor_value, 0) <= 0

union all

-- validity: invalid free float shares
select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_free_float_shares', 'Free float shares is zero or negative despite positive outstanding.',
    array['free_float_shares']::text[], row_snapshot, 'validity'
from base where shares_outstanding_value > 0 and coalesce(free_float_shares, 0) <= 0
  and review_row_id not in (select b2.review_row_id from base b2 join holdings_agg ha2 on ha2.security_review_row_id = b2.review_row_id where ha2.total_held > b2.shares_outstanding_value)

union all

-- validity: invalid investable shares
select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_investable_shares', 'Investable shares is zero or negative despite positive outstanding.',
    array['investable_shares']::text[], row_snapshot, 'validity'
from base where shares_outstanding_value > 0 and coalesce(investable_shares, 0) <= 0
  and review_row_id not in (select b2.review_row_id from base b2 join holdings_agg ha2 on ha2.security_review_row_id = b2.review_row_id where ha2.total_held > b2.shares_outstanding_value)

union all

-- anomaly: shares outstanding changed >15%
select b.pipeline_code, b.dataset_code, b.run_id, b.business_date, b.review_table, b.review_row_id, b.current_row_hash,
    'shares_outstanding_anomaly', format('Shares outstanding changed >15%% vs prior period (was %s).', to_char(pp.prior_shares_outstanding, 'FM999,999,999,999')),
    array['shares_outstanding_raw']::text[], b.row_snapshot, 'anomaly'
from base b join prior_period pp on pp.pipeline_code = b.pipeline_code and pp.ticker = b.ticker
where pp.prior_shares_outstanding > 0 and b.shares_outstanding_value > 0
  and abs(b.shares_outstanding_value - pp.prior_shares_outstanding) / pp.prior_shares_outstanding > 0.15

union all

-- anomaly: free float pct changed >10pp
select b.pipeline_code, b.dataset_code, b.run_id, b.business_date, b.review_table, b.review_row_id, b.current_row_hash,
    'free_float_pct_anomaly', format('Free float %% changed >10pp vs prior period (was %s).', round(pp.prior_free_float_pct * 100, 2)),
    array['free_float_pct_raw']::text[], b.row_snapshot, 'anomaly'
from base b join prior_period pp on pp.pipeline_code = b.pipeline_code and pp.ticker = b.ticker
where abs(coalesce(b.free_float_pct_value, 0) - coalesce(pp.prior_free_float_pct, 0)) > 0.10

union all

-- anomaly: investability changed >10pp
select b.pipeline_code, b.dataset_code, b.run_id, b.business_date, b.review_table, b.review_row_id, b.current_row_hash,
    'investability_factor_anomaly', format('Investability factor changed >10pp vs prior period (was %s).', round(pp.prior_investability_factor * 100, 2)),
    array['investability_factor_raw']::text[], b.row_snapshot, 'anomaly'
from base b join prior_period pp on pp.pipeline_code = b.pipeline_code and pp.ticker = b.ticker
where abs(coalesce(b.investability_factor_value, 0) - coalesce(pp.prior_investability_factor, 0)) > 0.10
;
$$;
