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
        if to_regclass('export.shareholder_holdings_final') is not null then
            delete from export.shareholder_holdings_final where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('export.shareholder_holdings_final', v_row_count);
        end if;

        if to_regclass('staging_export.shareholder_holdings_final') is not null then
            delete from staging_export.shareholder_holdings_final where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_export.shareholder_holdings_final', v_row_count);
        end if;

        if to_regclass('review.shareholder_holdings_daily') is not null then
            delete from review.shareholder_holdings_daily where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('review.shareholder_holdings_daily', v_row_count);
        end if;

        if to_regclass('marts.fact_shareholder_holding') is not null then
            delete from marts.fact_shareholder_holding where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('marts.fact_shareholder_holding', v_row_count);
        end if;

        if to_regclass('staging_marts.fact_shareholder_holding') is not null then
            delete from staging_marts.fact_shareholder_holding where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_marts.fact_shareholder_holding', v_row_count);
        end if;

        if to_regclass('marts.dim_shareholder') is not null then
            delete from marts.dim_shareholder where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('marts.dim_shareholder', v_row_count);
        end if;

        if to_regclass('staging_marts.dim_shareholder') is not null then
            delete from staging_marts.dim_shareholder where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_marts.dim_shareholder', v_row_count);
        end if;

        if to_regclass('intermediate.int_holding_with_security') is not null then
            delete from intermediate.int_holding_with_security where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('intermediate.int_holding_with_security', v_row_count);
        end if;

        if to_regclass('staging_intermediate.int_holding_with_security') is not null then
            delete from staging_intermediate.int_holding_with_security where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_intermediate.int_holding_with_security', v_row_count);
        end if;

        if to_regclass('intermediate.int_shareholder_base') is not null then
            delete from intermediate.int_shareholder_base where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('intermediate.int_shareholder_base', v_row_count);
        end if;

        if to_regclass('staging_intermediate.int_shareholder_base') is not null then
            delete from staging_intermediate.int_shareholder_base where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_intermediate.int_shareholder_base', v_row_count);
        end if;

        if to_regclass('staging.stg_13f_holdings') is not null then
            delete from staging.stg_13f_holdings where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging.stg_13f_holdings', v_row_count);
        end if;

        if to_regclass('staging_staging.stg_13f_holdings') is not null then
            delete from staging_staging.stg_13f_holdings where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_staging.stg_13f_holdings', v_row_count);
        end if;

        if to_regclass('staging.stg_13f_filers') is not null then
            delete from staging.stg_13f_filers where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging.stg_13f_filers', v_row_count);
        end if;

        if to_regclass('staging_staging.stg_13f_filers') is not null then
            delete from staging_staging.stg_13f_filers where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_staging.stg_13f_filers', v_row_count);
        end if;

        delete from raw.holdings_13f where run_id = p_run_id;
        get diagnostics v_row_count = row_count;
        v_deleted_counts := v_deleted_counts || jsonb_build_object('raw.holdings_13f', v_row_count);

        delete from raw.holdings_13f_filers where run_id = p_run_id;
        get diagnostics v_row_count = row_count;
        v_deleted_counts := v_deleted_counts || jsonb_build_object('raw.holdings_13f_filers', v_row_count);
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

        if to_regclass('export.security_master_final') is not null then
            delete from export.security_master_final where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('export.security_master_final', v_row_count);
        end if;

        if to_regclass('staging_export.security_master_final') is not null then
            delete from staging_export.security_master_final where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_export.security_master_final', v_row_count);
        end if;

        if to_regclass('review.security_master_daily') is not null then
            delete from review.security_master_daily where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('review.security_master_daily', v_row_count);
        end if;

        if to_regclass('marts.dim_security_snapshot') is not null then
            delete from marts.dim_security_snapshot where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('marts.dim_security_snapshot', v_row_count);
        end if;

        if to_regclass('marts.dim_security') is not null then
            delete from marts.dim_security where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('marts.dim_security', v_row_count);
        end if;

        if to_regclass('staging_marts.dim_security') is not null then
            delete from staging_marts.dim_security where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_marts.dim_security', v_row_count);
        end if;

        if to_regclass('intermediate.int_security_attributes') is not null then
            delete from intermediate.int_security_attributes where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('intermediate.int_security_attributes', v_row_count);
        end if;

        if to_regclass('staging_intermediate.int_security_attributes') is not null then
            delete from staging_intermediate.int_security_attributes where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_intermediate.int_security_attributes', v_row_count);
        end if;

        if to_regclass('intermediate.int_security_base') is not null then
            delete from intermediate.int_security_base where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('intermediate.int_security_base', v_row_count);
        end if;

        if to_regclass('staging_intermediate.int_security_base') is not null then
            delete from staging_intermediate.int_security_base where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_intermediate.int_security_base', v_row_count);
        end if;

        if to_regclass('staging.stg_sec_company_tickers') is not null then
            delete from staging.stg_sec_company_tickers where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging.stg_sec_company_tickers', v_row_count);
        end if;

        if to_regclass('staging_staging.stg_sec_company_tickers') is not null then
            delete from staging_staging.stg_sec_company_tickers where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_staging.stg_sec_company_tickers', v_row_count);
        end if;

        if to_regclass('staging.stg_sec_company_facts') is not null then
            delete from staging.stg_sec_company_facts where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging.stg_sec_company_facts', v_row_count);
        end if;

        if to_regclass('staging_staging.stg_sec_company_facts') is not null then
            delete from staging_staging.stg_sec_company_facts where run_id = p_run_id;
            get diagnostics v_row_count = row_count;
            v_deleted_counts := v_deleted_counts || jsonb_build_object('staging_staging.stg_sec_company_facts', v_row_count);
        end if;

        delete from raw.sec_company_tickers where run_id = p_run_id;
        get diagnostics v_row_count = row_count;
        v_deleted_counts := v_deleted_counts || jsonb_build_object('raw.sec_company_tickers', v_row_count);

        delete from raw.sec_company_facts where run_id = p_run_id;
        get diagnostics v_row_count = row_count;
        v_deleted_counts := v_deleted_counts || jsonb_build_object('raw.sec_company_facts', v_row_count);
    end if;

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
        coalesce(p_notes, 'Demo run data reset'),
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
