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
with current_review_rows as (
    select review_row_id
    from review.security_master_daily
    where p_dataset_code = 'security_master'
      and run_id = p_run_id

    union all

    select review_row_id
    from review.shareholder_holdings_daily
    where p_dataset_code = 'shareholder_holdings'
      and run_id = p_run_id
),
issue_base as (
    select
        issue.*,
        coalesce(issue.correction_of_issue_audit_id, issue.issue_audit_id) as issue_pair_id,
        case
            when issue.issue_status = 'corrected' then 'corrected'
            when issue.issue_status = 'open' then 'offending'
            else issue.issue_status
        end as issue_role
    from audit.review_data_issues issue
    join current_review_rows current_rows
      on current_rows.review_row_id = issue.review_row_id
    where issue.dataset_code = p_dataset_code
      and issue.run_id = p_run_id
      and issue.issue_status in ('open', 'corrected')
),
issue_groups as (
    select
        issue.run_id,
        issue.review_table,
        issue.review_row_id,
        issue.issue_role,
        max(issue.issue_audit_id) as issue_audit_id,
        case
            when count(distinct issue.issue_pair_id) = 1 then max(issue.issue_pair_id)
            else null
        end as issue_pair_id,
        case
            when count(distinct issue.correction_of_issue_audit_id) filter (where issue.correction_of_issue_audit_id is not null) = 1
            then max(issue.correction_of_issue_audit_id)
            else null
        end as correction_of_issue_audit_id,
        case
            when bool_or(issue.issue_status = 'corrected') and not bool_or(issue.issue_status = 'open')
            then 'corrected'
            else 'open'
        end as issue_status,
        max(issue.created_at) as created_at,
        max(issue.business_date) as business_date
    from issue_base issue
    group by
        issue.run_id,
        issue.review_table,
        issue.review_row_id,
        issue.issue_role
)
select
    issue_group.issue_pair_id,
    issue_group.issue_role,
    issue_group.issue_audit_id,
    issue_group.correction_of_issue_audit_id,
    issue_group.issue_status,
    (
        select string_agg(code.issue_code, ', ')
        from (
            select distinct issue.issue_code
            from issue_base issue
            where issue.run_id = issue_group.run_id
              and issue.review_table = issue_group.review_table
              and issue.review_row_id = issue_group.review_row_id
              and issue.issue_role = issue_group.issue_role
            order by issue.issue_code
        ) code
    ) as issue_code,
    (
        select string_agg(message.issue_message, ' | ')
        from (
            select distinct issue.issue_message
            from issue_base issue
            where issue.run_id = issue_group.run_id
              and issue.review_table = issue_group.review_table
              and issue.review_row_id = issue_group.review_row_id
              and issue.issue_role = issue_group.issue_role
            order by issue.issue_message
        ) message
    ) as issue_message,
    issue_group.created_at,
    issue_group.business_date,
    issue_group.run_id,
    issue_group.review_table,
    issue_group.review_row_id,
    (
        select coalesce(array_agg(distinct offending_column order by offending_column), '{}'::text[])
        from issue_base issue
        cross join lateral unnest(issue.offending_columns) offending_column
        where issue.run_id = issue_group.run_id
          and issue.review_table = issue_group.review_table
          and issue.review_row_id = issue_group.review_row_id
          and issue.issue_role = issue_group.issue_role
    ) as offending_columns,
    (
        select coalesce(array_agg(distinct corrected_column order by corrected_column), '{}'::text[])
        from issue_base issue
        cross join lateral unnest(issue.corrected_columns) corrected_column
        where issue.run_id = issue_group.run_id
          and issue.review_table = issue_group.review_table
          and issue.review_row_id = issue_group.review_row_id
          and issue.issue_role = issue_group.issue_role
    ) as corrected_columns,
    latest_issue.row_snapshot ->> 'ticker' as ticker,
    latest_issue.row_snapshot ->> 'issuer_name' as issuer_name,
    latest_issue.row_snapshot ->> 'exchange' as exchange,
    nullif(latest_issue.row_snapshot ->> 'shares_outstanding_raw', '')::numeric as shares_outstanding_raw,
    nullif(latest_issue.row_snapshot ->> 'free_float_pct_raw', '')::numeric as free_float_pct_raw,
    nullif(latest_issue.row_snapshot ->> 'investability_factor_raw', '')::numeric as investability_factor_raw,
    nullif(latest_issue.row_snapshot ->> 'free_float_shares', '')::numeric as free_float_shares,
    nullif(latest_issue.row_snapshot ->> 'investable_shares', '')::numeric as investable_shares,
    latest_issue.row_snapshot ->> 'security_identifier' as security_identifier,
    latest_issue.row_snapshot ->> 'security_name' as security_name,
    latest_issue.row_snapshot ->> 'filer_name' as filer_name,
    nullif(latest_issue.row_snapshot ->> 'shares_held_raw', '')::numeric as shares_held_raw,
    nullif(latest_issue.row_snapshot ->> 'reviewed_market_value_raw', '')::numeric as reviewed_market_value_raw,
    nullif(latest_issue.row_snapshot ->> 'holding_pct_of_outstanding', '')::numeric as holding_pct_of_outstanding,
    nullif(latest_issue.row_snapshot ->> 'derived_price_per_share', '')::numeric as derived_price_per_share,
    nullif(latest_issue.row_snapshot ->> 'portfolio_weight', '')::numeric as portfolio_weight,
    nullif(latest_issue.row_snapshot ->> 'source_confidence_raw', '')::numeric as source_confidence_raw
from issue_groups issue_group
join issue_base latest_issue
  on latest_issue.issue_audit_id = issue_group.issue_audit_id
order by
    case
        when issue_group.issue_role = 'offending' then 0
        else 1
    end,
    issue_group.created_at desc,
    issue_group.review_row_id asc;
$$;

grant execute on function query_api.fn_review_data_issues(text, uuid) to pipeline_svc, ui_svc, dashboard_ro_svc;
