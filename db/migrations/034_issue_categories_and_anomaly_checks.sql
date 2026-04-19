-- Migration 034: issue_category field + anomaly / confidence / concentration checks
--
-- Adds an issue_category column to audit.review_data_issues so the UI can group
-- rows into separate tabs:
--   validity     – missing or structurally invalid numbers (existing checks)
--   anomaly      – period-over-period changes that exceed thresholds
--   confidence   – holdings rows with low source-confidence score
--   concentration – holdings where a single filer holds > 10 % of outstanding
--
-- All existing rows default to 'validity'.

-- ── 1. Schema ────────────────────────────────────────────────────────────────

alter table audit.review_data_issues
    add column if not exists issue_category text not null default 'validity';

-- ── 2. Lookup indices for prior-period joins ─────────────────────────────────

create index if not exists idx_security_review_prior_lookup
    on review.security_master_daily (pipeline_code, ticker, business_date);

create index if not exists idx_holdings_review_prior_lookup
    on review.shareholder_holdings_daily (pipeline_code, filer_cik, security_identifier, business_date);

-- ── 3. Security master scan ──────────────────────────────────────────────────

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
        coalesce(s.free_float_pct_override,      s.free_float_pct_raw)      as free_float_pct_value,
        coalesce(s.investability_factor_override, s.investability_factor_raw) as investability_factor_value,
        s.free_float_shares,
        s.investable_shares
    from review.security_master_daily s
    where (p_run_id is null or s.run_id = p_run_id)
      and (p_review_row_id is null or s.review_row_id = p_review_row_id)
),
prior_period as (
    select distinct on (prior.pipeline_code, prior.ticker)
        prior.pipeline_code,
        prior.ticker,
        coalesce(prior.shares_outstanding_override, prior.shares_outstanding_raw) as prior_shares_outstanding,
        coalesce(prior.free_float_pct_override,      prior.free_float_pct_raw)      as prior_free_float_pct,
        coalesce(prior.investability_factor_override, prior.investability_factor_raw) as prior_investability_factor
    from review.security_master_daily prior
    where prior.pipeline_code = (select pipeline_code from base limit 1)
      and prior.business_date  < (select business_date  from base limit 1)
      and prior.ticker in (select ticker from base)
    order by prior.pipeline_code, prior.ticker, prior.business_date desc
)

-- ── validity: missing / structurally invalid ─────────────────────────────────

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'missing_ticker'::text,
    'Ticker is missing from the review row.'::text,
    array['ticker']::text[], row_snapshot, 'validity'::text
from base
where coalesce(nullif(btrim(row_snapshot ->> 'ticker'), ''), '') = ''

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'missing_issuer_name',
    'Issuer name is missing from the review row.',
    array['issuer_name']::text[], row_snapshot, 'validity'
from base
where coalesce(nullif(btrim(row_snapshot ->> 'issuer_name'), ''), '') = ''

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_shares_outstanding',
    'Shares outstanding is missing or non-positive.',
    array['shares_outstanding_raw']::text[], row_snapshot, 'validity'
from base
where shares_outstanding_value is null or shares_outstanding_value <= 0

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_free_float_pct',
    'Free-float percentage must stay within the 0 to 1 range.',
    array['free_float_pct_raw']::text[], row_snapshot, 'validity'
from base
where free_float_pct_value is null or free_float_pct_value < 0 or free_float_pct_value > 1

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_investability_factor',
    'Investability factor must stay within the 0 to 1 range.',
    array['investability_factor_raw']::text[], row_snapshot, 'validity'
from base
where investability_factor_value is null
   or investability_factor_value < 0
   or investability_factor_value > 1

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_free_float_shares',
    'Free-float shares are missing, negative, or exceed shares outstanding.',
    array['free_float_shares']::text[], row_snapshot, 'validity'
from base
where free_float_shares is null
   or free_float_shares < 0
   or (shares_outstanding_value is not null and free_float_shares > shares_outstanding_value)

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_investable_shares',
    'Investable shares are missing, negative, or exceed free-float shares.',
    array['investable_shares']::text[], row_snapshot, 'validity'
from base
where investable_shares is null
   or investable_shares < 0
   or (free_float_shares is not null and investable_shares > free_float_shares)

-- ── anomaly: period-over-period changes ──────────────────────────────────────
-- Threshold: >15 % relative change for shares_outstanding; >10 pp absolute
-- change for free_float_pct and investability_factor.

union all

select
    b.pipeline_code, b.dataset_code, b.run_id, b.business_date,
    b.review_table, b.review_row_id, b.current_row_hash,
    'shares_outstanding_anomaly'::text,
    format(
        'Shares outstanding changed by %s%% vs. prior period (was %s, now %s).',
        round(abs(b.shares_outstanding_value - p.prior_shares_outstanding)
              / p.prior_shares_outstanding * 100, 1),
        to_char(round(p.prior_shares_outstanding), 'FM999,999,999,999'),
        to_char(round(b.shares_outstanding_value), 'FM999,999,999,999')
    ),
    array['shares_outstanding_raw']::text[],
    b.row_snapshot,
    'anomaly'::text
from base b
join prior_period p on p.pipeline_code = b.pipeline_code and p.ticker = b.ticker
where b.shares_outstanding_value is not null
  and p.prior_shares_outstanding is not null
  and p.prior_shares_outstanding > 0
  and abs(b.shares_outstanding_value - p.prior_shares_outstanding)
      / p.prior_shares_outstanding > 0.15

union all

select
    b.pipeline_code, b.dataset_code, b.run_id, b.business_date,
    b.review_table, b.review_row_id, b.current_row_hash,
    'free_float_pct_anomaly'::text,
    format(
        'Free-float percentage changed by %s pp vs. prior period (was %s%%, now %s%%).',
        round(abs(b.free_float_pct_value - p.prior_free_float_pct) * 100, 1),
        round(p.prior_free_float_pct      * 100, 1),
        round(b.free_float_pct_value      * 100, 1)
    ),
    array['free_float_pct_raw']::text[],
    b.row_snapshot,
    'anomaly'::text
from base b
join prior_period p on p.pipeline_code = b.pipeline_code and p.ticker = b.ticker
where b.free_float_pct_value is not null
  and p.prior_free_float_pct is not null
  and abs(b.free_float_pct_value - p.prior_free_float_pct) > 0.10

union all

select
    b.pipeline_code, b.dataset_code, b.run_id, b.business_date,
    b.review_table, b.review_row_id, b.current_row_hash,
    'investability_factor_anomaly'::text,
    format(
        'Investability factor changed by %s pp vs. prior period (was %s%%, now %s%%).',
        round(abs(b.investability_factor_value - p.prior_investability_factor) * 100, 1),
        round(p.prior_investability_factor      * 100, 1),
        round(b.investability_factor_value      * 100, 1)
    ),
    array['investability_factor_raw']::text[],
    b.row_snapshot,
    'anomaly'::text
from base b
join prior_period p on p.pipeline_code = b.pipeline_code and p.ticker = b.ticker
where b.investability_factor_value is not null
  and p.prior_investability_factor is not null
  and abs(b.investability_factor_value - p.prior_investability_factor) > 0.10;
$$;


-- ── 4. Holdings scan ─────────────────────────────────────────────────────────

drop function if exists audit.fn_current_holding_review_issues(uuid, bigint);

create function audit.fn_current_holding_review_issues(
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
        h.pipeline_code,
        h.dataset_code,
        h.run_id,
        h.business_date,
        h.filer_cik,
        h.security_identifier,
        'review.shareholder_holdings_daily'::text as review_table,
        h.review_row_id,
        h.current_row_hash,
        to_jsonb(h) as row_snapshot,
        coalesce(h.shares_held_override,            h.shares_held_raw)            as shares_held_value,
        coalesce(h.reviewed_market_value_override,  h.reviewed_market_value_raw)  as market_value_value,
        coalesce(h.source_confidence_override,      h.source_confidence_raw)      as source_confidence_value,
        h.holding_pct_of_outstanding,
        h.derived_price_per_share,
        h.portfolio_weight
    from review.shareholder_holdings_daily h
    where (p_run_id is null or h.run_id = p_run_id)
      and (p_review_row_id is null or h.review_row_id = p_review_row_id)
)

-- ── validity ──────────────────────────────────────────────────────────────────

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'missing_security_identifier'::text,
    'Security identifier is missing from the holdings row.'::text,
    array['security_identifier']::text[], row_snapshot, 'validity'::text
from base
where coalesce(nullif(btrim(row_snapshot ->> 'security_identifier'), ''), '') = ''

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'missing_filer_name',
    'Filer name is missing from the holdings row.',
    array['filer_name']::text[], row_snapshot, 'validity'
from base
where coalesce(nullif(btrim(row_snapshot ->> 'filer_name'), ''), '') = ''

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_shares_held',
    'Shares held is missing or non-positive.',
    array['shares_held_raw']::text[], row_snapshot, 'validity'
from base
where shares_held_value is null or shares_held_value <= 0

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_market_value',
    'Reviewed market value is missing or negative.',
    array['reviewed_market_value_raw']::text[], row_snapshot, 'validity'
from base
where market_value_value is null or market_value_value < 0

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_holding_pct_of_outstanding',
    'Holding percentage of outstanding must stay within the 0 to 1.05 range.',
    array['holding_pct_of_outstanding']::text[], row_snapshot, 'validity'
from base
where holding_pct_of_outstanding is null
   or holding_pct_of_outstanding < 0
   or holding_pct_of_outstanding > 1.05

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_derived_price_per_share',
    'Derived price per share is missing or negative.',
    array['derived_price_per_share']::text[], row_snapshot, 'validity'
from base
where derived_price_per_share is null or derived_price_per_share < 0

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_portfolio_weight',
    'Portfolio weight must stay within the 0 to 1.05 range.',
    array['portfolio_weight']::text[], row_snapshot, 'validity'
from base
where portfolio_weight is null or portfolio_weight < 0 or portfolio_weight > 1.05

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'invalid_source_confidence',
    'Source confidence must stay within the 0 to 1 range.',
    array['source_confidence_raw']::text[], row_snapshot, 'validity'
from base
where source_confidence_value is null or source_confidence_value < 0 or source_confidence_value > 1

-- ── confidence: low source-confidence score ───────────────────────────────────
-- Threshold: < 0.30.  Rows already flagged as invalid (outside 0-1) are excluded.

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'low_source_confidence'::text,
    format(
        'Source confidence is low (%s). Verify shares held and market value against an alternative source.',
        round(source_confidence_value, 2)
    ),
    array['source_confidence_raw']::text[],
    row_snapshot,
    'confidence'::text
from base
where source_confidence_value is not null
  and source_confidence_value >= 0
  and source_confidence_value < 0.30

-- ── concentration: single filer holds > 10 % of outstanding ──────────────────
-- Rows already flagged as invalid (> 1.05) are excluded to avoid duplication.

union all

select pipeline_code, dataset_code, run_id, business_date, review_table, review_row_id, current_row_hash,
    'high_concentration_holding'::text,
    format(
        'Filer holds %s%% of shares outstanding. Confirm position size is accurate.',
        round(holding_pct_of_outstanding * 100, 2)
    ),
    array['holding_pct_of_outstanding']::text[],
    row_snapshot,
    'concentration'::text
from base
where holding_pct_of_outstanding is not null
  and holding_pct_of_outstanding >  0.10
  and holding_pct_of_outstanding <= 1.05;
$$;


-- ── 5. Dispatcher ────────────────────────────────────────────────────────────

drop function if exists audit.fn_current_review_data_issues(text, uuid, bigint);

create function audit.fn_current_review_data_issues(
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
    row_snapshot jsonb,
    issue_category text
)
language plpgsql
stable
security definer
as
$$
begin
    if p_dataset_code = 'security_master' then
        return query
        select * from audit.fn_current_security_review_issues(p_run_id, p_review_row_id);
        return;
    end if;

    if p_dataset_code = 'shareholder_holdings' then
        return query
        select * from audit.fn_current_holding_review_issues(p_run_id, p_review_row_id);
        return;
    end if;

    raise exception 'Unsupported dataset code for review issue scan: %', p_dataset_code;
end;
$$;


-- ── 6. Full-run refresh ──────────────────────────────────────────────────────

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
        created_by,
        issue_category
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
        p_actor,
        issue_category
    from audit.fn_current_review_data_issues(p_dataset_code, p_run_id, null);

    get diagnostics v_issue_count = row_count;

    return jsonb_build_object(
        'dataset_code',  p_dataset_code,
        'run_id',        p_run_id,
        'business_date', p_business_date,
        'issue_count',   v_issue_count
    );
end;
$$;


-- ── 7. Per-row refresh (called after a reviewer edits a cell) ────────────────

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
    v_corrected_count  integer := 0;
begin
    select *
    into v_context
    from audit.fn_review_row_snapshot(p_dataset_code, p_review_row_id)
    limit 1;

    if v_context.review_row_id is null then
        return jsonb_build_object(
            'dataset_code',   p_dataset_code,
            'review_row_id',  p_review_row_id,
            'open_issue_count', 0,
            'corrected_count',  0
        );
    end if;

    for v_previous_issue in
        select *
        from audit.review_data_issues
        where dataset_code  = p_dataset_code
          and run_id         = v_context.run_id
          and review_row_id  = p_review_row_id
          and issue_status   = 'open'
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
                created_by,
                issue_category
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
                p_actor,
                v_previous_issue.issue_category
            );
            v_corrected_count := v_corrected_count + 1;
        end if;
    end loop;

    update audit.review_data_issues
    set issue_status = 'superseded'
    where dataset_code  = p_dataset_code
      and run_id         = v_context.run_id
      and review_row_id  = p_review_row_id
      and issue_status   = 'open';

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
        created_by,
        issue_category
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
        p_actor,
        issue_category
    from audit.fn_current_review_data_issues(
        p_dataset_code,
        v_context.run_id,
        p_review_row_id
    );

    get diagnostics v_open_issue_count = row_count;

    return jsonb_build_object(
        'dataset_code',     p_dataset_code,
        'review_row_id',    p_review_row_id,
        'run_id',           v_context.run_id,
        'open_issue_count', v_open_issue_count,
        'corrected_count',  v_corrected_count
    );
end;
$$;


-- ── 8. Query-API function surfaced to the UI ──────────────────────────────────

drop function if exists query_api.fn_review_data_issues(text, uuid);

create or replace function query_api.fn_review_data_issues(
    p_dataset_code text,
    p_run_id uuid
)
returns table (
    issue_pair_id                  bigint,
    issue_role                     text,
    issue_audit_id                 bigint,
    correction_of_issue_audit_id   bigint,
    issue_status                   text,
    issue_code                     text,
    issue_message                  text,
    issue_category                 text,
    created_at                     timestamptz,
    business_date                  date,
    run_id                         uuid,
    review_table                   text,
    review_row_id                  bigint,
    offending_columns              text[],
    corrected_columns              text[],
    ticker                         text,
    issuer_name                    text,
    exchange                       text,
    shares_outstanding_raw         numeric,
    free_float_pct_raw             numeric,
    investability_factor_raw       numeric,
    free_float_shares              numeric,
    investable_shares              numeric,
    security_identifier            text,
    security_name                  text,
    filer_name                     text,
    shares_held_raw                numeric,
    reviewed_market_value_raw      numeric,
    holding_pct_of_outstanding     numeric,
    derived_price_per_share        numeric,
    portfolio_weight               numeric,
    source_confidence_raw          numeric
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
manual_change_rows as (
    select distinct
        change_log.run_id,
        change_log.review_table,
        change_log.row_pk::bigint as review_row_id
    from audit.change_log change_log
    where change_log.dataset_code = p_dataset_code
      and change_log.run_id       = p_run_id
      and change_log.row_pk ~ '^[0-9]+$'
),
issue_base as (
    select
        issue.*,
        coalesce(issue.correction_of_issue_audit_id, issue.issue_audit_id) as issue_pair_id,
        case
            when issue.issue_status = 'corrected' then 'corrected'
            when issue.issue_status = 'open'      then 'offending'
            else issue.issue_status
        end as issue_role
    from audit.review_data_issues issue
    join current_review_rows current_rows
      on current_rows.review_row_id = issue.review_row_id
    left join manual_change_rows manual_change
      on manual_change.run_id       = issue.run_id
     and manual_change.review_table = issue.review_table
     and manual_change.review_row_id = issue.review_row_id
    where issue.dataset_code = p_dataset_code
      and issue.run_id       = p_run_id
      and issue.issue_status in ('open', 'corrected')
      and (
            issue.issue_status <> 'corrected'
         or manual_change.review_row_id is not null
      )
),
issue_groups as (
    select
        issue.run_id,
        issue.review_table,
        issue.review_row_id,
        issue.issue_role,
        issue.issue_category,
        max(issue.issue_audit_id) as issue_audit_id,
        case
            when count(distinct issue.issue_pair_id) = 1 then max(issue.issue_pair_id)
            else null
        end as issue_pair_id,
        case
            when count(distinct issue.correction_of_issue_audit_id)
                 filter (where issue.correction_of_issue_audit_id is not null) = 1
            then max(issue.correction_of_issue_audit_id)
            else null
        end as correction_of_issue_audit_id,
        case
            when bool_or(issue.issue_status = 'corrected') and not bool_or(issue.issue_status = 'open')
            then 'corrected'
            else 'open'
        end as issue_status,
        max(issue.created_at)     as created_at,
        max(issue.business_date)  as business_date
    from issue_base issue
    group by
        issue.run_id,
        issue.review_table,
        issue.review_row_id,
        issue.issue_role,
        issue.issue_category
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
            where issue.run_id          = issue_group.run_id
              and issue.review_table    = issue_group.review_table
              and issue.review_row_id   = issue_group.review_row_id
              and issue.issue_role      = issue_group.issue_role
              and issue.issue_category  = issue_group.issue_category
            order by issue.issue_code
        ) code
    ) as issue_code,
    (
        select string_agg(message.issue_message, ' | ')
        from (
            select distinct issue.issue_message
            from issue_base issue
            where issue.run_id          = issue_group.run_id
              and issue.review_table    = issue_group.review_table
              and issue.review_row_id   = issue_group.review_row_id
              and issue.issue_role      = issue_group.issue_role
              and issue.issue_category  = issue_group.issue_category
            order by issue.issue_message
        ) message
    ) as issue_message,
    issue_group.issue_category,
    issue_group.created_at,
    issue_group.business_date,
    issue_group.run_id,
    issue_group.review_table,
    issue_group.review_row_id,
    (
        select coalesce(array_agg(distinct offending_column order by offending_column), '{}'::text[])
        from issue_base issue
        cross join lateral unnest(issue.offending_columns) offending_column
        where issue.run_id          = issue_group.run_id
          and issue.review_table    = issue_group.review_table
          and issue.review_row_id   = issue_group.review_row_id
          and issue.issue_role      = issue_group.issue_role
          and issue.issue_category  = issue_group.issue_category
    ) as offending_columns,
    (
        select coalesce(array_agg(distinct corrected_column order by corrected_column), '{}'::text[])
        from issue_base issue
        cross join lateral unnest(issue.corrected_columns) corrected_column
        where issue.run_id          = issue_group.run_id
          and issue.review_table    = issue_group.review_table
          and issue.review_row_id   = issue_group.review_row_id
          and issue.issue_role      = issue_group.issue_role
          and issue.issue_category  = issue_group.issue_category
    ) as corrected_columns,
    latest_issue.row_snapshot ->> 'ticker'                                          as ticker,
    latest_issue.row_snapshot ->> 'issuer_name'                                     as issuer_name,
    latest_issue.row_snapshot ->> 'exchange'                                        as exchange,
    nullif(latest_issue.row_snapshot ->> 'shares_outstanding_raw', '')::numeric     as shares_outstanding_raw,
    nullif(latest_issue.row_snapshot ->> 'free_float_pct_raw',     '')::numeric     as free_float_pct_raw,
    nullif(latest_issue.row_snapshot ->> 'investability_factor_raw','')::numeric    as investability_factor_raw,
    nullif(latest_issue.row_snapshot ->> 'free_float_shares',       '')::numeric    as free_float_shares,
    nullif(latest_issue.row_snapshot ->> 'investable_shares',       '')::numeric    as investable_shares,
    latest_issue.row_snapshot ->> 'security_identifier'                             as security_identifier,
    latest_issue.row_snapshot ->> 'security_name'                                   as security_name,
    latest_issue.row_snapshot ->> 'filer_name'                                      as filer_name,
    nullif(latest_issue.row_snapshot ->> 'shares_held_raw',            '')::numeric as shares_held_raw,
    nullif(latest_issue.row_snapshot ->> 'reviewed_market_value_raw',  '')::numeric as reviewed_market_value_raw,
    nullif(latest_issue.row_snapshot ->> 'holding_pct_of_outstanding', '')::numeric as holding_pct_of_outstanding,
    nullif(latest_issue.row_snapshot ->> 'derived_price_per_share',    '')::numeric as derived_price_per_share,
    nullif(latest_issue.row_snapshot ->> 'portfolio_weight',           '')::numeric as portfolio_weight,
    nullif(latest_issue.row_snapshot ->> 'source_confidence_raw',      '')::numeric as source_confidence_raw
from issue_groups issue_group
join issue_base latest_issue
  on latest_issue.issue_audit_id = issue_group.issue_audit_id
order by
    case when issue_group.issue_role = 'offending' then 0 else 1 end,
    issue_group.created_at desc,
    issue_group.review_row_id asc;
$$;


-- ── Grants ────────────────────────────────────────────────────────────────────

grant execute on function audit.fn_current_security_review_issues(uuid, bigint)
    to pipeline_svc, ui_svc, dashboard_ro_svc;

grant execute on function audit.fn_current_holding_review_issues(uuid, bigint)
    to pipeline_svc, ui_svc, dashboard_ro_svc;

grant execute on function audit.fn_current_review_data_issues(text, uuid, bigint)
    to pipeline_svc, ui_svc, dashboard_ro_svc;

grant execute on function audit.refresh_review_data_issues_for_run(text, uuid, date, text)
    to pipeline_svc, ui_svc;

grant execute on function audit.refresh_review_data_issues_for_row(text, bigint, text)
    to pipeline_svc, ui_svc;

grant execute on function query_api.fn_review_data_issues(text, uuid)
    to pipeline_svc, ui_svc, dashboard_ro_svc;
