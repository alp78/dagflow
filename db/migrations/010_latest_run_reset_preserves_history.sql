create or replace function workflow.restore_pipeline_state_from_history(
    p_pipeline_code text,
    p_actor text default 'system'
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_latest record;
begin
    select
        pr.run_id,
        pr.business_date,
        coalesce(drs.review_state, 'pending_review') as review_state
    into v_latest
    from observability.pipeline_runs pr
    left join workflow.dataset_review_state drs
      on drs.pipeline_code = pr.pipeline_code
     and drs.dataset_code = pr.dataset_code
     and drs.run_id = pr.run_id
    where pr.pipeline_code = p_pipeline_code
      and pr.dataset_code = p_pipeline_code
    order by pr.business_date desc, pr.started_at desc
    limit 1;

    if v_latest is null then
        update control.pipeline_state
        set approval_state = 'pending_review',
            last_run_id = null,
            last_business_date = null,
            paused_at = null,
            last_transition_by = p_actor,
            updated_at = timezone('utc', now())
        where pipeline_code = p_pipeline_code;

        return jsonb_build_object(
            'pipeline_code', p_pipeline_code,
            'last_run_id', null,
            'last_business_date', null,
            'approval_state', 'pending_review'
        );
    end if;

    update control.pipeline_state
    set approval_state = v_latest.review_state,
        last_run_id = v_latest.run_id,
        last_business_date = v_latest.business_date,
        paused_at = null,
        last_transition_by = p_actor,
        updated_at = timezone('utc', now())
    where pipeline_code = p_pipeline_code;

    return jsonb_build_object(
        'pipeline_code', p_pipeline_code,
        'last_run_id', v_latest.run_id,
        'last_business_date', v_latest.business_date,
        'approval_state', v_latest.review_state
    );
end;
$$;


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
    v_table text;
    v_tables text[];
begin
    if p_run_id is null then
        return jsonb_build_object(
            'pipeline_code', p_pipeline_code,
            'dataset_code', p_dataset_code,
            'run_id', null,
            'business_date', p_business_date,
            'deleted', false
        );
    end if;

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
        if to_regclass('review.shareholder_holdings_daily') is not null then
            update review.shareholder_holdings_daily
            set security_review_row_id = null,
                holding_pct_of_outstanding = null,
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

    insert into audit.workflow_events (
        pipeline_code,
        dataset_code,
        run_id,
        business_date,
        action_name,
        action_status,
        actor,
        notes,
        event_payload
    )
    values (
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        'reset_pipeline_run_data',
        'reset',
        p_actor,
        coalesce(p_notes, 'Latest daily run reset'),
        jsonb_build_object('deleted_counts', v_deleted_counts)
    );

    return jsonb_build_object(
        'pipeline_code', p_pipeline_code,
        'dataset_code', p_dataset_code,
        'run_id', p_run_id,
        'business_date', p_business_date,
        'deleted', true,
        'deleted_counts', v_deleted_counts
    );
end;
$$;


create or replace function workflow.reset_demo_runs(
    p_actor text default 'system',
    p_notes text default null
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_business_date date;
    v_results jsonb := '[]'::jsonb;
    v_run record;
    v_state_results jsonb := '[]'::jsonb;
begin
    select coalesce(max(last_business_date), current_date)
    into v_business_date
    from control.pipeline_state
    where pipeline_code in ('security_master', 'shareholder_holdings');

    for v_run in
        select run_id, business_date
        from observability.pipeline_runs
        where pipeline_code = 'shareholder_holdings'
          and dataset_code = 'shareholder_holdings'
          and business_date = v_business_date
        order by started_at desc
    loop
        v_results := v_results || jsonb_build_array(
            workflow.reset_pipeline_run_data(
                'shareholder_holdings',
                'shareholder_holdings',
                v_run.run_id,
                v_run.business_date,
                p_actor,
                p_notes
            )
        );
    end loop;

    for v_run in
        select run_id, business_date
        from observability.pipeline_runs
        where pipeline_code = 'security_master'
          and dataset_code = 'security_master'
          and business_date = v_business_date
        order by started_at desc
    loop
        v_results := v_results || jsonb_build_array(
            workflow.reset_pipeline_run_data(
                'security_master',
                'security_master',
                v_run.run_id,
                v_run.business_date,
                p_actor,
                p_notes
            )
        );
    end loop;

    v_state_results := v_state_results
        || jsonb_build_array(workflow.restore_pipeline_state_from_history('security_master', p_actor))
        || jsonb_build_array(workflow.restore_pipeline_state_from_history('shareholder_holdings', p_actor));

    insert into audit.workflow_events (
        pipeline_code,
        dataset_code,
        business_date,
        action_name,
        action_status,
        actor,
        notes,
        event_payload
    )
    values
        (
            'security_master',
            'security_master',
            v_business_date,
            'reset_demo_runs',
            'reset',
            p_actor,
            coalesce(p_notes, 'Latest daily run reset'),
            jsonb_build_object('business_date', v_business_date)
        ),
        (
            'shareholder_holdings',
            'shareholder_holdings',
            v_business_date,
            'reset_demo_runs',
            'reset',
            p_actor,
            coalesce(p_notes, 'Latest daily run reset'),
            jsonb_build_object('business_date', v_business_date)
        );

    return jsonb_build_object(
        'business_date', v_business_date,
        'results', v_results,
        'pipeline_state', v_state_results
    );
end;
$$;
