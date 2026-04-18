create or replace function workflow.reset_pipeline_run_data(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_actor text default 'system',
    p_notes text default null
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_deleted_counts jsonb := '{}'::jsonb;
    v_row_count bigint := 0;
    v_source_file_count bigint := 0;
    v_table text;
    v_tables text[];
    v_state jsonb := '{}'::jsonb;
begin
    if p_pipeline_code = 'shareholder_holdings' then
        v_tables := array[
            'export.shareholder_holdings_final',
            'staging_export.shareholder_holdings_final',
            'review.shareholder_holdings_daily',
            'marts.fact_shareholder_holding',
            'staging_marts.fact_shareholder_holding',
            'marts.dim_shareholder',
            'staging_marts.dim_shareholder',
            'intermediate.int_holding_with_security',
            'staging_intermediate.int_holding_with_security',
            'intermediate.int_shareholder_base',
            'staging_intermediate.int_shareholder_base',
            'staging.stg_13f_holdings',
            'staging_staging.stg_13f_holdings',
            'staging.stg_13f_filers',
            'staging_staging.stg_13f_filers',
            'raw.holdings_13f',
            'raw.holdings_13f_filers'
        ];
    elsif p_pipeline_code = 'security_master' then
        if to_regclass('review.shareholder_holdings_daily') is not null and p_run_id is not null then
            update review.shareholder_holdings_daily
            set security_review_row_id = null,
                updated_at = timezone('utc', now())
            where security_review_row_id in (
                select review_row_id
                from review.security_master_daily
                where run_id = p_run_id
            );
        end if;

        v_tables := array[
            'export.security_master_final',
            'staging_export.security_master_final',
            'review.security_master_daily',
            'marts.dim_security',
            'staging_marts.dim_security',
            'intermediate.int_security_attributes',
            'staging_intermediate.int_security_attributes',
            'intermediate.int_security_base',
            'staging_intermediate.int_security_base',
            'staging.stg_sec_company_tickers',
            'staging_staging.stg_sec_company_tickers',
            'staging.stg_sec_company_facts',
            'staging_staging.stg_sec_company_facts',
            'raw.sec_company_tickers',
            'raw.sec_company_facts'
        ];
    else
        v_tables := array[]::text[];
    end if;

    if p_run_id is not null then
        foreach v_table in array v_tables
        loop
            if to_regclass(v_table) is null then
                continue;
            end if;

            execute format('delete from %s where run_id = $1', v_table)
            using p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object(v_table, v_row_count);
        end loop;

        delete from workflow.dataset_review_state
        where pipeline_code = p_pipeline_code
          and dataset_code = p_dataset_code
          and run_id = p_run_id;

        delete from audit.review_data_issues
        where pipeline_code = p_pipeline_code
          and dataset_code = p_dataset_code
          and run_id = p_run_id;

        delete from audit.change_log
        where pipeline_code = p_pipeline_code
          and dataset_code = p_dataset_code
          and run_id = p_run_id;

        delete from audit.workflow_events
        where pipeline_code = p_pipeline_code
          and dataset_code = p_dataset_code
          and run_id = p_run_id;

        delete from lineage.row_lineage_edges
        where pipeline_code = p_pipeline_code
          and dataset_code = p_dataset_code
          and run_id = p_run_id;

        delete from lineage.entity_lineage_summary
        where pipeline_code = p_pipeline_code
          and dataset_code = p_dataset_code
          and run_id = p_run_id;

        delete from lineage.export_file_lineage
        where pipeline_code = p_pipeline_code
          and dataset_code = p_dataset_code
          and run_id = p_run_id;

        delete from observability.pipeline_failures
        where pipeline_code = p_pipeline_code
          and dataset_code = p_dataset_code
          and run_id = p_run_id;

        delete from observability.data_quality_results
        where pipeline_code = p_pipeline_code
          and dataset_code = p_dataset_code
          and run_id = p_run_id;

        delete from observability.pipeline_runs
        where pipeline_code = p_pipeline_code
          and dataset_code = p_dataset_code
          and run_id = p_run_id;
    end if;

    delete from control.source_file_registry
    where pipeline_code = p_pipeline_code
      and business_date = p_business_date;
    get diagnostics v_source_file_count = row_count;

    v_state := workflow.clear_pipeline_operational_state(p_pipeline_code, p_actor);

    return jsonb_build_object(
        'pipeline_code', p_pipeline_code,
        'dataset_code', p_dataset_code,
        'run_id', p_run_id,
        'business_date', p_business_date,
        'deleted', p_run_id is not null,
        'deleted_counts', v_deleted_counts,
        'source_file_count_deleted', v_source_file_count,
        'pipeline_state', v_state
    );
end;
$$;


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
)
select
    issue.issue_pair_id,
    issue.issue_role,
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
from issue_base issue
order by
    issue.issue_pair_id desc,
    case
        when issue.issue_role = 'offending' then 0
        else 1
    end,
    issue.created_at desc,
    issue.issue_audit_id desc;
$$;

delete from audit.review_data_issues issue
where (
        issue.dataset_code = 'security_master'
    and not exists (
        select 1
        from review.security_master_daily review
        where review.run_id = issue.run_id
          and review.review_row_id = issue.review_row_id
    )
    )
   or (
        issue.dataset_code = 'shareholder_holdings'
    and not exists (
        select 1
        from review.shareholder_holdings_daily review
        where review.run_id = issue.run_id
          and review.review_row_id = issue.review_row_id
    )
    );

grant execute on function query_api.fn_review_data_issues(text, uuid) to pipeline_svc, ui_svc, dashboard_ro_svc;
