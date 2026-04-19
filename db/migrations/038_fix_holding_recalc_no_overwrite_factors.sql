-- Fix: recalc_holding_review_fields was calling refresh_security_metrics_from_holdings
-- which overwrites free_float_pct_raw and investability_factor_raw with a different
-- formula, causing investability to jump to 1. Replace with a simple derived-column
-- recalc that preserves the original factor values.

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
    set holding_pct_of_outstanding = case
            when m.outstanding_shares is null then null
            else round(m.shares_held_value / m.outstanding_shares, 8)
        end,
        derived_price_per_share = case
            when m.shares_held_value = 0 then null
            else round(m.market_value_value / m.shares_held_value, 6)
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

    -- Recalculate the parent security's derived columns WITHOUT overwriting factors
    if v_security_review_row_id is not null then
        perform calc.recalc_security_review_fields(v_security_review_row_id, p_changed_by);
    end if;

    perform audit.refresh_review_data_issues_for_row(
        'shareholder_holdings',
        p_row_id,
        p_changed_by
    );

    select row_to_json(h)::jsonb
    into v_result
    from review.shareholder_holdings_daily h
    where h.review_row_id = p_row_id;

    return coalesce(v_result, '{}'::jsonb);
end;
$$;
