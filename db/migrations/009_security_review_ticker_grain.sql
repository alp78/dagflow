alter table review.security_master_daily
    drop constraint if exists security_master_daily_run_id_cik_key;

do
$$
begin
    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'review.security_master_daily'::regclass
          and conname = 'security_master_daily_run_id_ticker_key'
    ) then
        alter table review.security_master_daily
            add constraint security_master_daily_run_id_ticker_key unique (run_id, ticker);
    end if;
end;
$$;

create or replace function workflow.publish_review_snapshot(
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
    v_result jsonb;
begin
    if p_pipeline_code = 'security_master' and to_regclass('marts.dim_security') is not null then
        insert into review.security_master_daily (
            run_id,
            business_date,
            origin_mart_row_id,
            origin_mart_row_hash,
            current_row_hash,
            source_record_id,
            cik,
            ticker,
            issuer_name,
            exchange,
            shares_outstanding_raw,
            free_float_pct_raw,
            investability_factor_raw,
            free_float_shares,
            investable_shares,
            review_materiality_score
        )
        select
            p_run_id,
            p_business_date,
            security_id,
            row_hash,
            row_hash,
            source_record_id,
            cik,
            ticker,
            issuer_name,
            exchange,
            shares_outstanding_raw,
            free_float_pct_raw,
            investability_factor_raw,
            free_float_shares,
            investable_shares,
            review_materiality_score
        from marts.dim_security
        where run_id = p_run_id
        on conflict (run_id, ticker) do update
        set origin_mart_row_id = excluded.origin_mart_row_id,
            origin_mart_row_hash = excluded.origin_mart_row_hash,
            current_row_hash = excluded.current_row_hash,
            source_record_id = excluded.source_record_id,
            cik = excluded.cik,
            issuer_name = excluded.issuer_name,
            exchange = excluded.exchange,
            shares_outstanding_raw = excluded.shares_outstanding_raw,
            free_float_pct_raw = excluded.free_float_pct_raw,
            investability_factor_raw = excluded.investability_factor_raw,
            free_float_shares = excluded.free_float_shares,
            investable_shares = excluded.investable_shares,
            review_materiality_score = excluded.review_materiality_score,
            updated_at = timezone('utc', now());
    elsif p_pipeline_code = 'shareholder_holdings' and to_regclass('marts.fact_shareholder_holding') is not null then
        insert into review.shareholder_holdings_daily (
            run_id,
            business_date,
            origin_mart_row_id,
            origin_mart_row_hash,
            current_row_hash,
            source_record_id,
            accession_number,
            filer_cik,
            filer_name,
            security_identifier,
            security_name,
            security_review_row_id,
            shares_held_raw,
            reviewed_market_value_raw,
            source_confidence_raw,
            holding_pct_of_outstanding,
            derived_price_per_share,
            portfolio_weight
        )
        select
            p_run_id,
            p_business_date,
            h.holding_id,
            h.row_hash,
            h.row_hash,
            h.source_record_id,
            h.accession_number,
            h.filer_cik,
            h.filer_name,
            h.security_identifier,
            h.security_name,
            s.review_row_id,
            h.shares_held_raw,
            h.reviewed_market_value_raw,
            h.source_confidence_raw,
            h.holding_pct_of_outstanding,
            h.derived_price_per_share,
            h.portfolio_weight
        from marts.fact_shareholder_holding h
        left join control.pipeline_state ps
            on ps.pipeline_code = 'security_master'
        left join review.security_master_daily s
            on s.run_id = ps.last_run_id
           and s.ticker = h.security_identifier
        where h.run_id = p_run_id
        on conflict (run_id, accession_number, filer_cik, security_identifier) do update
        set origin_mart_row_id = excluded.origin_mart_row_id,
            origin_mart_row_hash = excluded.origin_mart_row_hash,
            current_row_hash = excluded.current_row_hash,
            source_record_id = excluded.source_record_id,
            filer_name = excluded.filer_name,
            security_name = excluded.security_name,
            security_review_row_id = excluded.security_review_row_id,
            shares_held_raw = excluded.shares_held_raw,
            reviewed_market_value_raw = excluded.reviewed_market_value_raw,
            source_confidence_raw = excluded.source_confidence_raw,
            holding_pct_of_outstanding = excluded.holding_pct_of_outstanding,
            derived_price_per_share = excluded.derived_price_per_share,
            portfolio_weight = excluded.portfolio_weight,
            updated_at = timezone('utc', now());
    end if;

    v_result := workflow.set_dataset_review_state(
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        'pending_review',
        p_actor,
        coalesce(p_notes, 'Review snapshot published')
    );

    return v_result;
end;
$$;
