create index if not exists idx_security_review_snapshot_display
    on review.security_master_daily (run_id, business_date, ticker, issuer_name, review_row_id);

create index if not exists idx_holdings_review_snapshot_display
    on review.shareholder_holdings_daily (run_id, business_date, security_identifier, filer_name, review_row_id);

create index if not exists idx_holdings_review_security_row
    on review.shareholder_holdings_daily (security_review_row_id);

create index if not exists idx_holdings_review_run_filer
    on review.shareholder_holdings_daily (run_id, filer_cik);

create index if not exists idx_review_data_issues_correction_lookup
    on audit.review_data_issues (dataset_code, run_id, correction_of_issue_audit_id)
    where correction_of_issue_audit_id is not null;

create or replace function query_api.fn_review_data_issues(
    p_dataset_code text,
    p_run_id uuid
)
returns table (
    issue_pair_id bigint,
    issue_role text,
    issue_audit_id bigint,
    correction_of_issue_audit_id bigint,
    issue_status text,
    issue_code text,
    issue_message text,
    created_at timestamptz,
    business_date date,
    run_id uuid,
    review_table text,
    review_row_id bigint,
    offending_columns text[],
    corrected_columns text[],
    ticker text,
    issuer_name text,
    exchange text,
    shares_outstanding_raw numeric,
    free_float_pct_raw numeric,
    investability_factor_raw numeric,
    free_float_shares numeric,
    investable_shares numeric,
    security_identifier text,
    security_name text,
    filer_name text,
    shares_held_raw numeric,
    reviewed_market_value_raw numeric,
    holding_pct_of_outstanding numeric,
    derived_price_per_share numeric,
    portfolio_weight numeric,
    source_confidence_raw numeric
)
language sql
stable
security definer
as
$$
select
    coalesce(issue.correction_of_issue_audit_id, issue.issue_audit_id) as issue_pair_id,
    case
        when issue.issue_status = 'corrected' then 'corrected'
        when issue.issue_status in ('open', 'superseded') then 'offending'
        else issue.issue_status
    end as issue_role,
    issue.issue_audit_id,
    issue.correction_of_issue_audit_id,
    issue.issue_status,
    issue.issue_code,
    issue.issue_message,
    issue.created_at,
    issue.business_date,
    issue.run_id,
    issue.review_table,
    issue.review_row_id,
    issue.offending_columns,
    issue.corrected_columns,
    issue.row_snapshot ->> 'ticker' as ticker,
    issue.row_snapshot ->> 'issuer_name' as issuer_name,
    issue.row_snapshot ->> 'exchange' as exchange,
    nullif(issue.row_snapshot ->> 'shares_outstanding_raw', '')::numeric as shares_outstanding_raw,
    nullif(issue.row_snapshot ->> 'free_float_pct_raw', '')::numeric as free_float_pct_raw,
    nullif(issue.row_snapshot ->> 'investability_factor_raw', '')::numeric as investability_factor_raw,
    nullif(issue.row_snapshot ->> 'free_float_shares', '')::numeric as free_float_shares,
    nullif(issue.row_snapshot ->> 'investable_shares', '')::numeric as investable_shares,
    issue.row_snapshot ->> 'security_identifier' as security_identifier,
    issue.row_snapshot ->> 'security_name' as security_name,
    issue.row_snapshot ->> 'filer_name' as filer_name,
    nullif(issue.row_snapshot ->> 'shares_held_raw', '')::numeric as shares_held_raw,
    nullif(issue.row_snapshot ->> 'reviewed_market_value_raw', '')::numeric as reviewed_market_value_raw,
    nullif(issue.row_snapshot ->> 'holding_pct_of_outstanding', '')::numeric as holding_pct_of_outstanding,
    nullif(issue.row_snapshot ->> 'derived_price_per_share', '')::numeric as derived_price_per_share,
    nullif(issue.row_snapshot ->> 'portfolio_weight', '')::numeric as portfolio_weight,
    nullif(issue.row_snapshot ->> 'source_confidence_raw', '')::numeric as source_confidence_raw
from audit.review_data_issues issue
where issue.dataset_code = p_dataset_code
  and issue.run_id = p_run_id
  and (
      issue.issue_status in ('open', 'corrected')
      or (
          issue.issue_status = 'superseded'
          and exists (
              select 1
              from audit.review_data_issues corrected
              where corrected.dataset_code = issue.dataset_code
                and corrected.run_id = issue.run_id
                and corrected.issue_status = 'corrected'
                and corrected.correction_of_issue_audit_id = issue.issue_audit_id
          )
      )
  )
order by
    coalesce(issue.correction_of_issue_audit_id, issue.issue_audit_id) desc,
    case
        when issue.issue_status in ('open', 'superseded') then 0
        else 1
    end,
    issue.created_at desc,
    issue.issue_audit_id desc;
$$;

grant execute on function query_api.fn_review_data_issues(text, uuid) to pipeline_svc, ui_svc, dashboard_ro_svc;
