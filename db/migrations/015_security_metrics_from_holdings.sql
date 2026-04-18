create or replace function calc.refresh_security_metrics_from_holdings(
    p_security_review_row_id bigint,
    p_touch_state boolean default false
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_result jsonb;
begin
    with holding_rollup as (
        select
            h.security_review_row_id,
            count(*) filter (
                where coalesce(h.shares_held_override, h.shares_held_raw, 0) > 0
            )::numeric as holder_count,
            coalesce(sum(coalesce(h.shares_held_override, h.shares_held_raw, 0)), 0)::numeric
                as total_held_shares,
            coalesce(max(coalesce(h.shares_held_override, h.shares_held_raw, 0)), 0)::numeric
                as top_holder_shares
        from review.shareholder_holdings_daily h
        where h.security_review_row_id = p_security_review_row_id
        group by h.security_review_row_id
    ),
    security_input as (
        select
            s.review_row_id,
            coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)::numeric
                as outstanding_shares,
            coalesce(h.holder_count, 0)::numeric as holder_count,
            coalesce(h.total_held_shares, 0)::numeric as total_held_shares,
            coalesce(h.top_holder_shares, 0)::numeric as top_holder_shares
        from review.security_master_daily s
        left join holding_rollup h
            on h.security_review_row_id = s.review_row_id
        where s.review_row_id = p_security_review_row_id
    ),
    computed as (
        select
            review_row_id,
            outstanding_shares,
            case
                when outstanding_shares <= 0 then 1::numeric
                else greatest(
                    0::numeric,
                    least(
                        1::numeric,
                        1::numeric
                        - least(total_held_shares / nullif(outstanding_shares, 0), 1::numeric)
                    )
                )
            end as computed_free_float_pct,
            case
                when outstanding_shares <= 0 then 0.70::numeric
                else greatest(
                    0.35::numeric,
                    least(
                        1::numeric,
                        0.45::numeric
                        + (least(holder_count, 100::numeric) / 250::numeric)
                        + (
                            greatest(
                                0::numeric,
                                least(
                                    1::numeric,
                                    1::numeric
                                    - least(total_held_shares / nullif(outstanding_shares, 0), 1::numeric)
                                )
                            ) * 0.25::numeric
                        )
                        - (
                            least(top_holder_shares / nullif(outstanding_shares, 0), 1::numeric)
                            * 0.50::numeric
                        )
                    )
                )
            end as computed_investability_factor
        from security_input
    )
    update review.security_master_daily as s
    set free_float_pct_raw = round(c.computed_free_float_pct, 6),
        investability_factor_raw = round(c.computed_investability_factor, 6),
        free_float_shares = round(
            c.outstanding_shares
            * coalesce(s.free_float_pct_override, c.computed_free_float_pct),
            6
        ),
        investable_shares = round(
            c.outstanding_shares
            * coalesce(s.free_float_pct_override, c.computed_free_float_pct)
            * coalesce(s.investability_factor_override, c.computed_investability_factor),
            6
        ),
        review_materiality_score = round(
            (
                c.outstanding_shares
                * coalesce(s.free_float_pct_override, c.computed_free_float_pct)
                * coalesce(s.investability_factor_override, c.computed_investability_factor)
            ) / 1000000.0,
            6
        ),
        approval_state = case
            when p_touch_state and s.approval_state = 'pending_review' then 'in_review'
            else s.approval_state
        end,
        updated_at = timezone('utc', now())
    from computed c
    where s.review_row_id = c.review_row_id
    returning row_to_json(s)::jsonb into v_result;

    return coalesce(v_result, '{}'::jsonb);
end;
$$;

create or replace function calc.refresh_security_metrics_for_holdings_run(
    p_holdings_run_id uuid,
    p_touch_state boolean default false
)
returns bigint
language plpgsql
security definer
as
$$
declare
    v_security_review_row_id bigint;
    v_rows bigint := 0;
begin
    for v_security_review_row_id in
        select distinct h.security_review_row_id
        from review.shareholder_holdings_daily h
        where h.run_id = p_holdings_run_id
          and h.security_review_row_id is not null
    loop
        perform calc.refresh_security_metrics_from_holdings(
            v_security_review_row_id,
            p_touch_state
        );
        v_rows := v_rows + 1;
    end loop;

    return v_rows;
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

    select row_to_json(h)::jsonb
    into v_result
    from review.shareholder_holdings_daily h
    where h.review_row_id = p_row_id;

    return coalesce(v_result, '{}'::jsonb);
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

        perform calc.refresh_security_metrics_for_holdings_run(p_run_id, false);
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

grant execute on function calc.refresh_security_metrics_from_holdings(bigint, boolean) to pipeline_svc, ui_svc, dashboard_ro_svc;
grant execute on function calc.refresh_security_metrics_for_holdings_run(uuid, boolean) to pipeline_svc, ui_svc, dashboard_ro_svc;
