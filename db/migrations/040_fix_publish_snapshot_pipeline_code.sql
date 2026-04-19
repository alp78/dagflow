-- Fix publish_review_snapshot to route by dataset_code instead of pipeline_code,
-- since the unified pipeline uses 'security_shareholder' for both datasets.

create or replace function workflow.publish_review_snapshot(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_actor text default 'dagster',
    p_notes text default null
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_result jsonb;
begin
    if p_dataset_code = 'security_master' and to_regclass('marts.securities') is not null then
        insert into review.security_master_daily (
            run_id, business_date, origin_mart_row_id, origin_mart_row_hash, current_row_hash,
            source_record_id, cik, ticker, issuer_name, exchange,
            shares_outstanding_raw, free_float_pct_raw, investability_factor_raw,
            free_float_shares, investable_shares, review_materiality_score
        )
        select
            p_run_id, p_business_date, security_id, row_hash, row_hash,
            source_record_id, cik, ticker, issuer_name, exchange,
            shares_outstanding_raw, free_float_pct_raw, investability_factor_raw,
            free_float_shares, investable_shares, review_materiality_score
        from marts.securities
        where run_id = p_run_id
        ;

    elsif p_dataset_code = 'shareholder_holdings' and to_regclass('marts.holdings') is not null then
        insert into review.shareholder_holdings_daily (
            run_id, business_date, pipeline_code, dataset_code,
            origin_mart_row_id, origin_mart_row_hash, current_row_hash,
            source_record_id, accession_number, filer_cik, filer_name,
            security_identifier, security_name, security_review_row_id,
            shares_held_raw, reviewed_market_value_raw, source_confidence_raw,
            holding_pct_of_outstanding, derived_price_per_share, portfolio_weight
        )
        select
            p_run_id, p_business_date, p_pipeline_code, p_dataset_code,
            h.holding_id, h.row_hash, h.row_hash,
            h.source_record_id, h.accession_number, h.filer_cik, h.filer_name,
            h.security_identifier, h.security_name,
            s.review_row_id,
            h.shares_held_raw, h.reviewed_market_value_raw, h.source_confidence_raw,
            h.holding_pct_of_outstanding, h.derived_price_per_share, h.portfolio_weight
        from marts.holdings h
        left join review.security_master_daily s
          on s.run_id = p_run_id
         and s.ticker = h.security_identifier
        where h.run_id = p_run_id
        ;
    end if;

    insert into workflow.dataset_review_state (
        pipeline_code, dataset_code, run_id, business_date,
        review_state, review_required, review_started_at,
        last_action, last_actor, notes
    )
    values (
        p_pipeline_code, p_dataset_code, p_run_id, p_business_date,
        'pending_review', true, timezone('utc', now()),
        'published', p_actor, p_notes
    )
    on conflict (pipeline_code, dataset_code, run_id) do update
    set review_state = 'pending_review',
        last_action = 'republished',
        last_actor = excluded.last_actor,
        notes = excluded.notes,
        updated_at = timezone('utc', now());

    select jsonb_build_object(
        'pipeline_code', p_pipeline_code,
        'dataset_code', p_dataset_code,
        'run_id', p_run_id,
        'business_date', p_business_date,
        'review_state', 'pending_review',
        'actor', p_actor
    ) into v_result;

    return v_result;
end;
$$;
