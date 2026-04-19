-- Fix: recalc should only recompute derived columns (free_float_shares,
-- investable_shares, review_materiality_score) from the existing
-- free_float_pct_raw and investability_factor_raw values.
-- It must NOT overwrite these factor columns — they were computed correctly
-- by dbt during the pipeline run and should not change during interactive edits.

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
begin
    update review.security_master_daily as s
    set free_float_shares = case
            when coalesce(s.free_float_pct_override, s.free_float_pct_raw) is null then null
            else round(
                coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)
                * coalesce(s.free_float_pct_override, s.free_float_pct_raw),
                6
            )
        end,
        investable_shares = case
            when coalesce(s.free_float_pct_override, s.free_float_pct_raw) is null
              or coalesce(s.investability_factor_override, s.investability_factor_raw) is null
            then null
            else round(
                coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)
                * coalesce(s.free_float_pct_override, s.free_float_pct_raw)
                * coalesce(s.investability_factor_override, s.investability_factor_raw),
                6
            )
        end,
        review_materiality_score = case
            when coalesce(s.free_float_pct_override, s.free_float_pct_raw) is null
              or coalesce(s.investability_factor_override, s.investability_factor_raw) is null
            then null
            else round(
                (
                    coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)
                    * coalesce(s.free_float_pct_override, s.free_float_pct_raw)
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
