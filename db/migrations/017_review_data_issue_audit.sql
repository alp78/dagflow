create table if not exists audit.review_data_issues (
    issue_audit_id bigint generated always as identity primary key,
    pipeline_code text not null,
    dataset_code text not null,
    run_id uuid not null,
    business_date date not null,
    review_table text not null,
    review_row_id bigint not null,
    current_row_hash text,
    issue_code text not null,
    issue_status text not null default 'open'
        check (issue_status in ('open', 'corrected', 'superseded')),
    issue_severity text not null default 'warning'
        check (issue_severity in ('warning', 'error')),
    issue_message text not null,
    offending_columns text[] not null default '{}'::text[],
    corrected_columns text[] not null default '{}'::text[],
    row_snapshot jsonb not null default '{}'::jsonb,
    correction_of_issue_audit_id bigint references audit.review_data_issues (issue_audit_id),
    created_by text not null default 'system',
    created_at timestamptz not null default timezone('utc', now())
);

create index if not exists idx_review_data_issues_run_status
    on audit.review_data_issues (dataset_code, run_id, issue_status, created_at desc);

create index if not exists idx_review_data_issues_row
    on audit.review_data_issues (dataset_code, review_row_id, created_at desc);


create or replace function audit.fn_review_row_snapshot(
    p_dataset_code text,
    p_review_row_id bigint
)
returns table (
    pipeline_code text,
    dataset_code text,
    run_id uuid,
    business_date date,
    review_table text,
    review_row_id bigint,
    current_row_hash text,
    row_snapshot jsonb
)
language plpgsql
stable
security definer
as
$$
begin
    if p_dataset_code = 'security_master' then
        return query
        select
            s.pipeline_code,
            s.dataset_code,
            s.run_id,
            s.business_date,
            'review.security_master_daily'::text,
            s.review_row_id,
            s.current_row_hash,
            to_jsonb(s)
        from review.security_master_daily s
        where s.review_row_id = p_review_row_id;
        return;
    end if;

    if p_dataset_code = 'shareholder_holdings' then
        return query
        select
            h.pipeline_code,
            h.dataset_code,
            h.run_id,
            h.business_date,
            'review.shareholder_holdings_daily'::text,
            h.review_row_id,
            h.current_row_hash,
            to_jsonb(h)
        from review.shareholder_holdings_daily h
        where h.review_row_id = p_review_row_id;
        return;
    end if;

    raise exception 'Unsupported dataset code for review row snapshot: %', p_dataset_code;
end;
$$;


create or replace function audit.fn_current_security_review_issues(
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
    row_snapshot jsonb
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
)
select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'missing_ticker'::text as issue_code,
    'Ticker is missing from the review row.'::text as issue_message,
    array['ticker']::text[] as offending_columns,
    row_snapshot
from base
where coalesce(nullif(btrim(row_snapshot ->> 'ticker'), ''), '') = ''

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'missing_issuer_name',
    'Issuer name is missing from the review row.',
    array['issuer_name']::text[],
    row_snapshot
from base
where coalesce(nullif(btrim(row_snapshot ->> 'issuer_name'), ''), '') = ''

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_shares_outstanding',
    'Shares outstanding is missing or non-positive.',
    array['shares_outstanding_raw']::text[],
    row_snapshot
from base
where shares_outstanding_value is null
   or shares_outstanding_value <= 0

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_free_float_pct',
    'Free-float percentage must stay within the 0 to 1 range.',
    array['free_float_pct_raw']::text[],
    row_snapshot
from base
where free_float_pct_value is null
   or free_float_pct_value < 0
   or free_float_pct_value > 1

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_investability_factor',
    'Investability factor must stay within the 0 to 1 range.',
    array['investability_factor_raw']::text[],
    row_snapshot
from base
where investability_factor_value is null
   or investability_factor_value < 0
   or investability_factor_value > 1

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_free_float_shares',
    'Free-float shares are missing, negative, or exceed shares outstanding.',
    array['free_float_shares']::text[],
    row_snapshot
from base
where free_float_shares is null
   or free_float_shares < 0
   or (
       shares_outstanding_value is not null
       and free_float_shares > shares_outstanding_value
   )

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_investable_shares',
    'Investable shares are missing, negative, or exceed free-float shares.',
    array['investable_shares']::text[],
    row_snapshot
from base
where investable_shares is null
   or investable_shares < 0
   or (
       free_float_shares is not null
       and investable_shares > free_float_shares
   );
$$;


create or replace function audit.fn_current_holding_review_issues(
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
    row_snapshot jsonb
)
language sql
stable
security definer
as
$$
with base as (
    select
        h.pipeline_code,
        h.dataset_code,
        h.run_id,
        h.business_date,
        'review.shareholder_holdings_daily'::text as review_table,
        h.review_row_id,
        h.current_row_hash,
        to_jsonb(h) as row_snapshot,
        coalesce(h.shares_held_override, h.shares_held_raw) as shares_held_value,
        coalesce(h.reviewed_market_value_override, h.reviewed_market_value_raw) as market_value_value,
        coalesce(h.source_confidence_override, h.source_confidence_raw) as source_confidence_value,
        h.holding_pct_of_outstanding,
        h.derived_price_per_share,
        h.portfolio_weight
    from review.shareholder_holdings_daily h
    where (p_run_id is null or h.run_id = p_run_id)
      and (p_review_row_id is null or h.review_row_id = p_review_row_id)
)
select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'missing_security_identifier'::text as issue_code,
    'Security identifier is missing from the holdings row.'::text as issue_message,
    array['security_identifier']::text[] as offending_columns,
    row_snapshot
from base
where coalesce(nullif(btrim(row_snapshot ->> 'security_identifier'), ''), '') = ''

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'missing_filer_name',
    'Filer name is missing from the holdings row.',
    array['filer_name']::text[],
    row_snapshot
from base
where coalesce(nullif(btrim(row_snapshot ->> 'filer_name'), ''), '') = ''

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_shares_held',
    'Shares held is missing or non-positive.',
    array['shares_held_raw']::text[],
    row_snapshot
from base
where shares_held_value is null
   or shares_held_value <= 0

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_market_value',
    'Reviewed market value is missing or negative.',
    array['reviewed_market_value_raw']::text[],
    row_snapshot
from base
where market_value_value is null
   or market_value_value < 0

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_holding_pct_of_outstanding',
    'Holding percentage of outstanding must stay within the 0 to 1.05 range.',
    array['holding_pct_of_outstanding']::text[],
    row_snapshot
from base
where holding_pct_of_outstanding is null
   or holding_pct_of_outstanding < 0
   or holding_pct_of_outstanding > 1.05

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_derived_price_per_share',
    'Derived price per share is missing or negative.',
    array['derived_price_per_share']::text[],
    row_snapshot
from base
where derived_price_per_share is null
   or derived_price_per_share < 0

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_portfolio_weight',
    'Portfolio weight must stay within the 0 to 1.05 range.',
    array['portfolio_weight']::text[],
    row_snapshot
from base
where portfolio_weight is null
   or portfolio_weight < 0
   or portfolio_weight > 1.05

union all

select
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_table,
    review_row_id,
    current_row_hash,
    'invalid_source_confidence',
    'Source confidence must stay within the 0 to 1 range.',
    array['source_confidence_raw']::text[],
    row_snapshot
from base
where source_confidence_value is null
   or source_confidence_value < 0
   or source_confidence_value > 1;
$$;


create or replace function audit.fn_current_review_data_issues(
    p_dataset_code text,
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
    row_snapshot jsonb
)
language plpgsql
stable
security definer
as
$$
begin
    if p_dataset_code = 'security_master' then
        return query
        select *
        from audit.fn_current_security_review_issues(p_run_id, p_review_row_id);
        return;
    end if;

    if p_dataset_code = 'shareholder_holdings' then
        return query
        select *
        from audit.fn_current_holding_review_issues(p_run_id, p_review_row_id);
        return;
    end if;

    raise exception 'Unsupported dataset code for review issue scan: %', p_dataset_code;
end;
$$;


create or replace function audit.refresh_review_data_issues_for_run(
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_actor text default 'pipeline_scan'
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_issue_count integer := 0;
begin
    update audit.review_data_issues
    set issue_status = 'superseded'
    where dataset_code = p_dataset_code
      and run_id = p_run_id
      and issue_status = 'open';

    insert into audit.review_data_issues (
        pipeline_code,
        dataset_code,
        run_id,
        business_date,
        review_table,
        review_row_id,
        current_row_hash,
        issue_code,
        issue_status,
        issue_message,
        offending_columns,
        row_snapshot,
        created_by
    )
    select
        pipeline_code,
        dataset_code,
        run_id,
        business_date,
        review_table,
        review_row_id,
        current_row_hash,
        issue_code,
        'open',
        issue_message,
        offending_columns,
        row_snapshot,
        p_actor
    from audit.fn_current_review_data_issues(p_dataset_code, p_run_id, null);

    get diagnostics v_issue_count = row_count;

    return jsonb_build_object(
        'dataset_code', p_dataset_code,
        'run_id', p_run_id,
        'business_date', p_business_date,
        'issue_count', v_issue_count
    );
end;
$$;


create or replace function audit.refresh_review_data_issues_for_row(
    p_dataset_code text,
    p_review_row_id bigint,
    p_actor text default 'reviewer'
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_context record;
    v_previous_issue record;
    v_open_issue_count integer := 0;
    v_corrected_count integer := 0;
begin
    select *
    into v_context
    from audit.fn_review_row_snapshot(p_dataset_code, p_review_row_id)
    limit 1;

    if v_context.review_row_id is null then
        return jsonb_build_object(
            'dataset_code', p_dataset_code,
            'review_row_id', p_review_row_id,
            'open_issue_count', 0,
            'corrected_count', 0
        );
    end if;

    for v_previous_issue in
        select *
        from audit.review_data_issues
        where dataset_code = p_dataset_code
          and run_id = v_context.run_id
          and review_row_id = p_review_row_id
          and issue_status = 'open'
    loop
        if not exists (
            select 1
            from audit.fn_current_review_data_issues(
                p_dataset_code,
                v_context.run_id,
                p_review_row_id
            ) current_issue
            where current_issue.issue_code = v_previous_issue.issue_code
        ) then
            insert into audit.review_data_issues (
                pipeline_code,
                dataset_code,
                run_id,
                business_date,
                review_table,
                review_row_id,
                current_row_hash,
                issue_code,
                issue_status,
                issue_message,
                corrected_columns,
                row_snapshot,
                correction_of_issue_audit_id,
                created_by
            )
            values (
                v_context.pipeline_code,
                v_context.dataset_code,
                v_context.run_id,
                v_context.business_date,
                v_context.review_table,
                v_context.review_row_id,
                v_context.current_row_hash,
                v_previous_issue.issue_code,
                'corrected',
                'Reviewer corrected the row and removed the data issue.',
                coalesce(v_previous_issue.offending_columns, '{}'::text[]),
                v_context.row_snapshot,
                v_previous_issue.issue_audit_id,
                p_actor
            );
            v_corrected_count := v_corrected_count + 1;
        end if;
    end loop;

    update audit.review_data_issues
    set issue_status = 'superseded'
    where dataset_code = p_dataset_code
      and run_id = v_context.run_id
      and review_row_id = p_review_row_id
      and issue_status = 'open';

    insert into audit.review_data_issues (
        pipeline_code,
        dataset_code,
        run_id,
        business_date,
        review_table,
        review_row_id,
        current_row_hash,
        issue_code,
        issue_status,
        issue_message,
        offending_columns,
        row_snapshot,
        created_by
    )
    select
        pipeline_code,
        dataset_code,
        run_id,
        business_date,
        review_table,
        review_row_id,
        current_row_hash,
        issue_code,
        'open',
        issue_message,
        offending_columns,
        row_snapshot,
        p_actor
    from audit.fn_current_review_data_issues(
        p_dataset_code,
        v_context.run_id,
        p_review_row_id
    );

    get diagnostics v_open_issue_count = row_count;

    return jsonb_build_object(
        'dataset_code', p_dataset_code,
        'review_row_id', p_review_row_id,
        'run_id', v_context.run_id,
        'open_issue_count', v_open_issue_count,
        'corrected_count', v_corrected_count
    );
end;
$$;


create or replace function query_api.fn_review_data_issues(
    p_dataset_code text,
    p_run_id uuid
)
returns table (
    issue_audit_id bigint,
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
    issue.issue_audit_id,
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
  and issue.issue_status <> 'superseded'
order by
    case issue.issue_status
        when 'open' then 0
        when 'corrected' then 1
        else 2
    end,
    issue.created_at desc,
    issue.issue_audit_id desc;
$$;


create or replace function calc.recalc_security_review_fields(
    p_row_id bigint,
    p_changed_by text default 'system'
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_result jsonb;
begin
    update review.security_master_daily as s
    set free_float_shares =
            coalesce(shares_outstanding_override, shares_outstanding_raw, 0)
            * coalesce(free_float_pct_override, free_float_pct_raw, 1),
        investable_shares =
            coalesce(shares_outstanding_override, shares_outstanding_raw, 0)
            * coalesce(free_float_pct_override, free_float_pct_raw, 1)
            * coalesce(investability_factor_override, investability_factor_raw, 1),
        review_materiality_score =
            round(
                (
                    coalesce(shares_outstanding_override, shares_outstanding_raw, 0)
                    * coalesce(free_float_pct_override, free_float_pct_raw, 1)
                    * coalesce(investability_factor_override, investability_factor_raw, 1)
                ) / 1000000.0,
                6
            ),
        approval_state = case
            when approval_state = 'pending_review' then 'in_review'
            else approval_state
        end,
        updated_at = timezone('utc', now())
    where review_row_id = p_row_id
    returning row_to_json(s)::jsonb into v_result;

    perform audit.refresh_review_data_issues_for_row(
        'security_master',
        p_row_id,
        p_changed_by
    );

    return coalesce(v_result, '{}'::jsonb);
end;
$$;


create or replace function calc.recalc_holding_review_fields(
    p_row_id bigint,
    p_changed_by text default 'system'
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_result jsonb;
    v_run_id uuid;
    v_filer_cik text;
    v_security_review_row_id bigint;
begin
    select run_id, filer_cik, security_review_row_id
    into v_run_id, v_filer_cik, v_security_review_row_id
    from review.shareholder_holdings_daily
    where review_row_id = p_row_id;

    with scoped_rows as (
        select review_row_id
        from review.shareholder_holdings_daily
        where run_id = v_run_id
          and filer_cik = v_filer_cik
    ),
    metrics as (
        select
            h.review_row_id,
            coalesce(h.shares_held_override, h.shares_held_raw, 0) as shares_held_value,
            coalesce(h.reviewed_market_value_override, h.reviewed_market_value_raw, 0) as market_value_value,
            nullif(coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0), 0) as outstanding_shares,
            sum(coalesce(h.reviewed_market_value_override, h.reviewed_market_value_raw, 0))
                over (partition by h.run_id, h.filer_cik) as filer_market_value
        from review.shareholder_holdings_daily h
        join scoped_rows scoped
            on scoped.review_row_id = h.review_row_id
        left join review.security_master_daily s
            on s.review_row_id = h.security_review_row_id
    )
    update review.shareholder_holdings_daily as h
    set derived_price_per_share = case
            when m.shares_held_value = 0 then null
            else round(m.market_value_value / nullif(m.shares_held_value, 0), 6)
        end,
        holding_pct_of_outstanding = case
            when m.outstanding_shares is null then null
            else round(m.shares_held_value / m.outstanding_shares, 6)
        end,
        portfolio_weight = case
            when coalesce(m.filer_market_value, 0) = 0 then null
            else round(m.market_value_value / m.filer_market_value, 6)
        end,
        approval_state = case
            when approval_state = 'pending_review' then 'in_review'
            else approval_state
        end,
        updated_at = timezone('utc', now())
    from metrics m
    where h.review_row_id = m.review_row_id;

    if v_security_review_row_id is not null then
        perform calc.refresh_security_metrics_from_holdings(v_security_review_row_id, true);
    end if;

    perform audit.refresh_review_data_issues_for_row(
        'shareholder_holdings',
        p_row_id,
        p_changed_by
    );

    if v_security_review_row_id is not null then
        perform audit.refresh_review_data_issues_for_row(
            'security_master',
            v_security_review_row_id,
            p_changed_by
        );
    end if;

    select row_to_json(h)::jsonb
    into v_result
    from review.shareholder_holdings_daily h
    where h.review_row_id = p_row_id;

    return coalesce(v_result, '{}'::jsonb);
end;
$$;


grant select, insert, update on audit.review_data_issues to pipeline_svc, ui_svc;
grant usage, select on sequence audit.review_data_issues_issue_audit_id_seq to pipeline_svc, ui_svc;

grant execute on function audit.fn_review_row_snapshot(text, bigint) to pipeline_svc, ui_svc, dashboard_ro_svc;
grant execute on function audit.fn_current_security_review_issues(uuid, bigint) to pipeline_svc, ui_svc, dashboard_ro_svc;
grant execute on function audit.fn_current_holding_review_issues(uuid, bigint) to pipeline_svc, ui_svc, dashboard_ro_svc;
grant execute on function audit.fn_current_review_data_issues(text, uuid, bigint) to pipeline_svc, ui_svc, dashboard_ro_svc;
grant execute on function audit.refresh_review_data_issues_for_run(text, uuid, date, text) to pipeline_svc, ui_svc;
grant execute on function audit.refresh_review_data_issues_for_row(text, bigint, text) to pipeline_svc, ui_svc;
grant execute on function query_api.fn_review_data_issues(text, uuid) to pipeline_svc, ui_svc, dashboard_ro_svc;
