-- Fix two issues in calc.recalc_security_review_fields:
-- 1. The else branch (no holdings evidence) re-fetched free_float_pct_raw and
--    investability_factor_raw from marts.dim_security, overwriting the review
--    row values with NULLs when the marts table was empty/stale after reset.
--    Now uses the review row's own values for derived column calculation.
-- 2. The per-row loop calling audit.refresh_review_data_issues_for_row for
--    every linked holding row (~700+ per security) was extremely slow.
--    Replaced with a single batched call.

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
    v_has_holdings_evidence boolean := false;
begin
    select exists (
        select 1
        from review.shareholder_holdings_daily h
        where h.security_review_row_id = p_row_id
          and coalesce(h.shares_held_override, h.shares_held_raw, 0) > 0
    )
    into v_has_holdings_evidence;

    if v_has_holdings_evidence then
        perform calc.refresh_security_metrics_from_holdings(p_row_id, true);
    else
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
    end if;

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
