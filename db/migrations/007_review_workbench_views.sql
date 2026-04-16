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
    v_run_id uuid;
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
    returning s.run_id, row_to_json(s)::jsonb into v_run_id, v_result;

    if v_run_id is not null then
        perform calc.recalc_security_rollups(v_run_id);
    end if;

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
begin
    select run_id, filer_cik
    into v_run_id, v_filer_cik
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

    select row_to_json(h)::jsonb
    into v_result
    from review.shareholder_holdings_daily h
    where h.review_row_id = p_row_id;

    return coalesce(v_result, '{}'::jsonb);
end;
$$;

create or replace function query_api.fn_security_holder_concentration(
    p_security_review_row_id bigint
)
returns table (
    holder_segment text,
    holder_count bigint,
    total_shares numeric,
    pct_of_outstanding numeric,
    avg_portfolio_weight numeric
)
language sql
stable
as
$$
    with security_base as (
        select nullif(coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0), 0) as outstanding_shares
        from review.security_master_daily s
        where s.review_row_id = p_security_review_row_id
    ),
    ranked_holders as (
        select
            coalesce(h.shares_held_override, h.shares_held_raw, 0) as shares_held,
            h.portfolio_weight,
            row_number() over (
                order by coalesce(h.shares_held_override, h.shares_held_raw, 0) desc nulls last, h.filer_name
            ) as holder_rank
        from review.shareholder_holdings_daily h
        where h.security_review_row_id = p_security_review_row_id
    ),
    bucketed as (
        select
            case
                when holder_rank <= 5 then 'Top 5 holders'
                when holder_rank <= 15 then 'Next 10 holders'
                else 'Remaining holders'
            end as holder_segment,
            shares_held,
            portfolio_weight,
            case
                when holder_rank <= 5 then 1
                when holder_rank <= 15 then 2
                else 3
            end as segment_rank
        from ranked_holders
    )
    select
        b.holder_segment,
        count(*)::bigint as holder_count,
        round(sum(b.shares_held), 6) as total_shares,
        round(sum(b.shares_held) / nullif(max(security_base.outstanding_shares), 0), 6) as pct_of_outstanding,
        round(avg(b.portfolio_weight), 6) as avg_portfolio_weight
    from bucketed b
    cross join security_base
    group by b.holder_segment, b.segment_rank
    order by b.segment_rank;
$$;

create or replace function query_api.fn_security_holder_approval_mix(
    p_security_review_row_id bigint
)
returns table (
    approval_state text,
    holder_count bigint,
    total_shares numeric,
    avg_portfolio_weight numeric,
    max_holding_pct numeric
)
language sql
stable
as
$$
    select
        h.approval_state,
        count(*)::bigint as holder_count,
        round(sum(coalesce(h.shares_held_override, h.shares_held_raw, 0)), 6) as total_shares,
        round(avg(h.portfolio_weight), 6) as avg_portfolio_weight,
        round(max(h.holding_pct_of_outstanding), 6) as max_holding_pct
    from review.shareholder_holdings_daily h
    where h.security_review_row_id = p_security_review_row_id
    group by h.approval_state
    order by holder_count desc, h.approval_state;
$$;

create or replace function query_api.fn_holding_peer_holders(
    p_holding_review_row_id bigint
)
returns table (
    filer_name text,
    shares_held numeric,
    holding_pct_of_outstanding numeric,
    portfolio_weight numeric,
    approval_state text
)
language sql
stable
as
$$
    with selected_holding as (
        select security_review_row_id
        from review.shareholder_holdings_daily
        where review_row_id = p_holding_review_row_id
    )
    select
        h.filer_name,
        coalesce(h.shares_held_override, h.shares_held_raw) as shares_held,
        h.holding_pct_of_outstanding,
        h.portfolio_weight,
        h.approval_state
    from review.shareholder_holdings_daily h
    join selected_holding selected
        on selected.security_review_row_id = h.security_review_row_id
    order by coalesce(h.shares_held_override, h.shares_held_raw) desc nulls last, h.filer_name;
$$;

create or replace function query_api.fn_filer_portfolio_snapshot(
    p_holding_review_row_id bigint
)
returns table (
    security_identifier text,
    security_name text,
    shares_held numeric,
    market_value numeric,
    portfolio_weight numeric,
    holding_pct_of_outstanding numeric,
    approval_state text
)
language sql
stable
as
$$
    with selected_holding as (
        select run_id, filer_cik
        from review.shareholder_holdings_daily
        where review_row_id = p_holding_review_row_id
    )
    select
        h.security_identifier,
        h.security_name,
        coalesce(h.shares_held_override, h.shares_held_raw) as shares_held,
        coalesce(h.reviewed_market_value_override, h.reviewed_market_value_raw) as market_value,
        h.portfolio_weight,
        h.holding_pct_of_outstanding,
        h.approval_state
    from review.shareholder_holdings_daily h
    join selected_holding selected
        on selected.run_id = h.run_id
       and selected.filer_cik = h.filer_cik
    order by h.portfolio_weight desc nulls last, h.security_identifier;
$$;

create or replace function query_api.fn_filer_weight_bands(
    p_holding_review_row_id bigint
)
returns table (
    weight_band text,
    positions bigint,
    total_market_value numeric,
    avg_weight numeric
)
language sql
stable
as
$$
    with selected_holding as (
        select run_id, filer_cik
        from review.shareholder_holdings_daily
        where review_row_id = p_holding_review_row_id
    ),
    bucketed as (
        select
            case
                when coalesce(h.portfolio_weight, 0) >= 0.15 then 'Core positions'
                when coalesce(h.portfolio_weight, 0) >= 0.05 then 'Conviction positions'
                else 'Satellite positions'
            end as weight_band,
            coalesce(h.reviewed_market_value_override, h.reviewed_market_value_raw, 0) as market_value,
            h.portfolio_weight,
            case
                when coalesce(h.portfolio_weight, 0) >= 0.15 then 1
                when coalesce(h.portfolio_weight, 0) >= 0.05 then 2
                else 3
            end as band_rank
        from review.shareholder_holdings_daily h
        join selected_holding selected
            on selected.run_id = h.run_id
           and selected.filer_cik = h.filer_cik
    )
    select
        b.weight_band,
        count(*)::bigint as positions,
        round(sum(b.market_value), 6) as total_market_value,
        round(avg(b.portfolio_weight), 6) as avg_weight
    from bucketed b
    group by b.weight_band, b.band_rank
    order by b.band_rank;
$$;

grant execute on all functions in schema query_api, calc to pipeline_svc, ui_svc, dashboard_ro_svc;
