-- Fix: free_float_pct_raw depends on shares_outstanding (it's held/outstanding),
-- so it MUST be recomputed when shares_outstanding changes.
-- investability_factor_raw does NOT depend on shares_outstanding, so it's preserved.

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
    v_outstanding_shares numeric;
    v_total_held numeric;
    v_new_free_float numeric;
begin
    -- Recompute free_float_pct from holdings if holdings exist
    select coalesce(sum(coalesce(h.shares_held_override, h.shares_held_raw, 0)), 0)
    into v_total_held
    from review.shareholder_holdings_daily h
    where h.security_review_row_id = p_row_id;

    select coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)
    into v_outstanding_shares
    from review.security_master_daily s
    where s.review_row_id = p_row_id;

    if v_outstanding_shares > 0 and v_total_held > 0 then
        v_new_free_float := greatest(0, least(1, 1.0 - least(v_total_held / v_outstanding_shares, 1.0)));
    else
        -- No holdings data or no outstanding — keep existing value
        select s.free_float_pct_raw into v_new_free_float
        from review.security_master_daily s where s.review_row_id = p_row_id;
    end if;

    update review.security_master_daily as s
    set free_float_pct_raw = coalesce(round(v_new_free_float, 6), s.free_float_pct_raw),
        free_float_shares = case
            when coalesce(s.free_float_pct_override, v_new_free_float) is null then null
            else round(
                coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)
                * coalesce(s.free_float_pct_override, v_new_free_float),
                6
            )
        end,
        investable_shares = case
            when coalesce(s.free_float_pct_override, v_new_free_float) is null
              or coalesce(s.investability_factor_override, s.investability_factor_raw) is null
            then null
            else round(
                coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)
                * coalesce(s.free_float_pct_override, v_new_free_float)
                * coalesce(s.investability_factor_override, s.investability_factor_raw),
                6
            )
        end,
        review_materiality_score = case
            when coalesce(s.free_float_pct_override, v_new_free_float) is null
              or coalesce(s.investability_factor_override, s.investability_factor_raw) is null
            then null
            else round(
                (
                    coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)
                    * coalesce(s.free_float_pct_override, v_new_free_float)
                    * coalesce(s.investability_factor_override, s.investability_factor_raw)
                ) / 1000000.0,
                6
            )
        end,
        approval_state = case
            when s.approval_state = 'pending_review' then 'in_review'
            else s.approval_state
        end,
        updated_at = timezone('utc', now())
    where s.review_row_id = p_row_id;

    select
        row_to_json(s)::jsonb,
        nullif(
            coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0),
            0
        )
    into v_result, v_outstanding_shares
    from review.security_master_daily s
    where s.review_row_id = p_row_id;

    update review.shareholder_holdings_daily as h
    set holding_pct_of_outstanding = case
            when v_outstanding_shares is null then null
            else round(
                coalesce(h.shares_held_override, h.shares_held_raw, 0) / v_outstanding_shares,
                8
            )
        end,
        approval_state = case
            when approval_state = 'pending_review' then 'in_review'
            else approval_state
        end,
        updated_at = timezone('utc', now())
    where h.security_review_row_id = p_row_id;

    perform audit.refresh_review_data_issues_for_row(
        'security_master',
        p_row_id,
        p_changed_by
    );

    return coalesce(v_result, '{}'::jsonb);
end;
$$;
