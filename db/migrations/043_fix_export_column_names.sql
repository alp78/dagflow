-- Align write_export_bundle SQL function with dbt export model column names.
-- dbt models use: review_row_id, review_row_hash
-- Old function used: origin_review_row_id, origin_review_row_hash, file_id

create or replace function workflow.write_export_bundle(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_result jsonb;
begin
    if p_dataset_code = 'security_master' then
        insert into export.security_master_final (
            run_id,
            review_row_id,
            review_row_hash,
            business_date,
            ticker,
            issuer_name,
            investable_shares,
            review_materiality_score,
            _row_export_key
        )
        select
            run_id,
            review_row_id,
            current_row_hash,
            business_date,
            coalesce(ticker, 'UNKNOWN'),
            issuer_name,
            investable_shares,
            review_materiality_score,
            md5(run_id::text || '|' || review_row_id::text)
        from review.security_master_daily
        where run_id = p_run_id
          and approval_state in ('approved', 'exported')
        on conflict do nothing;
    elsif p_dataset_code = 'shareholder_holdings' then
        insert into export.shareholder_holdings_final (
            run_id,
            review_row_id,
            review_row_hash,
            business_date,
            filer_name,
            security_name,
            shares_held,
            holding_pct_of_outstanding,
            _row_export_key
        )
        select
            run_id,
            review_row_id,
            current_row_hash,
            business_date,
            filer_name,
            security_name,
            coalesce(shares_held_override, shares_held_raw, 0),
            holding_pct_of_outstanding,
            md5(run_id::text || '|' || review_row_id::text)
        from review.shareholder_holdings_daily
        where run_id = p_run_id
          and approval_state in ('approved', 'exported')
        on conflict do nothing;
    end if;

    v_result := workflow.set_dataset_review_state(
        p_pipeline_code, p_dataset_code, p_run_id, p_business_date,
        'exported', 'dagster', 'Export bundle written'
    );

    return jsonb_build_object(
        'pipeline_code', p_pipeline_code,
        'dataset_code', p_dataset_code,
        'run_id', p_run_id,
        'business_date', p_business_date,
        'export_row_count', (
            case p_dataset_code
                when 'security_master' then (select count(*) from export.security_master_final where run_id = p_run_id)
                when 'shareholder_holdings' then (select count(*) from export.shareholder_holdings_final where run_id = p_run_id)
                else 0
            end
        )
    );
end;
$$;
