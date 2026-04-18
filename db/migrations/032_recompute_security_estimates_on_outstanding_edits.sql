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
    v_holding_review_row_id bigint;
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
        with source_metrics as (
            select
                s.review_row_id,
                m.free_float_pct_raw as source_free_float_pct_raw,
                m.investability_factor_raw as source_investability_factor_raw
            from review.security_master_daily s
            left join marts.dim_security m
              on m.security_id = s.origin_mart_row_id
             and m.run_id = s.run_id
            where s.review_row_id = p_row_id
        )
        update review.security_master_daily as s
        set free_float_pct_raw = source_metrics.source_free_float_pct_raw,
            investability_factor_raw = source_metrics.source_investability_factor_raw,
            free_float_shares = case
                when coalesce(s.free_float_pct_override, source_metrics.source_free_float_pct_raw) is null then null
                else round(
                    coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)
                    * coalesce(s.free_float_pct_override, source_metrics.source_free_float_pct_raw),
                    6
                )
            end,
            investable_shares = case
                when coalesce(s.free_float_pct_override, source_metrics.source_free_float_pct_raw) is null
                  or coalesce(s.investability_factor_override, source_metrics.source_investability_factor_raw) is null
                then null
                else round(
                    coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)
                    * coalesce(s.free_float_pct_override, source_metrics.source_free_float_pct_raw)
                    * coalesce(s.investability_factor_override, source_metrics.source_investability_factor_raw),
                    6
                )
            end,
            review_materiality_score = case
                when coalesce(s.free_float_pct_override, source_metrics.source_free_float_pct_raw) is null
                  or coalesce(s.investability_factor_override, source_metrics.source_investability_factor_raw) is null
                then null
                else round(
                    (
                        coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)
                        * coalesce(s.free_float_pct_override, source_metrics.source_free_float_pct_raw)
                        * coalesce(s.investability_factor_override, source_metrics.source_investability_factor_raw)
                    ) / 1000000.0,
                    6
                )
            end,
            approval_state = case
                when s.approval_state = 'pending_review' then 'in_review'
                else s.approval_state
            end,
            updated_at = timezone('utc', now())
        from source_metrics
        where s.review_row_id = source_metrics.review_row_id;
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

    for v_holding_review_row_id in
        select h.review_row_id
        from review.shareholder_holdings_daily h
        where h.security_review_row_id = p_row_id
    loop
        perform audit.refresh_review_data_issues_for_row(
            'shareholder_holdings',
            v_holding_review_row_id,
            p_changed_by
        );
    end loop;

    return coalesce(v_result, '{}'::jsonb);
end;
$$;
