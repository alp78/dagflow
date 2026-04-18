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
