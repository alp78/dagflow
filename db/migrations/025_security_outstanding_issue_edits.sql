create or replace function review.is_editable_review_column(
    p_review_table regclass,
    p_column_name text
)
returns boolean
language plpgsql
stable
security definer
as
$$
declare
    v_table_fq text;
begin
    select format('%I.%I', n.nspname, c.relname)
    into v_table_fq
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where c.oid = p_review_table;

    if v_table_fq = 'review.security_master_daily' then
        return p_column_name = 'shares_outstanding_raw';
    end if;

    if v_table_fq = 'review.shareholder_holdings_daily' then
        return p_column_name = 'shares_held_raw';
    end if;

    return false;
end;
$$;


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
    returning row_to_json(s)::jsonb,
        nullif(
            coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0),
            0
        )
    into v_result, v_outstanding_shares;

    update review.shareholder_holdings_daily as h
    set holding_pct_of_outstanding = case
            when v_outstanding_shares is null then null
            else round(
                coalesce(h.shares_held_override, h.shares_held_raw, 0) / v_outstanding_shares,
                6
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
