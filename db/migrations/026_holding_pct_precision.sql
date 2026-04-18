alter table review.shareholder_holdings_daily
    alter column holding_pct_of_outstanding type numeric(24, 8);

alter table if exists export.shareholder_holdings_final
    alter column holding_pct_of_outstanding type numeric(24, 8);

alter table if exists export.shareholder_holdings_preview
    alter column holding_pct_of_outstanding type numeric(24, 8);


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
            else round(m.shares_held_value / m.outstanding_shares, 8)
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

    perform audit.refresh_review_data_issues_for_row(
        'shareholder_holdings',
        p_row_id,
        p_changed_by
    );

    if v_security_review_row_id is not null then
        perform audit.refresh_review_data_issues_for_row(
            'security_master',
            v_security_review_row_id,
            p_changed_by
        );
    end if;

    select row_to_json(h)::jsonb
    into v_result
    from review.shareholder_holdings_daily h
    where h.review_row_id = p_row_id;

    return coalesce(v_result, '{}'::jsonb);
end;
$$;


create or replace function calc.recalc_security_rollups(
    p_run_id uuid
)
returns bigint
language plpgsql
security definer
as
$$
declare
    v_rows bigint;
begin
    update review.shareholder_holdings_daily as h
    set holding_pct_of_outstanding = round(
            coalesce(h.shares_held_override, h.shares_held_raw, 0)
            / nullif(coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0), 0),
            8
        ),
        updated_at = timezone('utc', now())
    from review.security_master_daily s
    where h.security_review_row_id = s.review_row_id
      and h.run_id = p_run_id;

    get diagnostics v_rows = row_count;
    return v_rows;
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


do
$$
begin
    if to_regclass('marts.fact_shareholder_holding') is not null
       and to_regclass('marts.dim_security') is not null then
        update marts.fact_shareholder_holding as h
        set holding_pct_of_outstanding = round(
                h.shares_held_raw / nullif(coalesce(s.shares_outstanding_raw, 0), 0),
                8
            )
        from marts.dim_security s
        where s.security_id = h.security_id
          and s.run_id = h.run_id;
    end if;

    if to_regclass('review.shareholder_holdings_daily') is not null
       and to_regclass('review.security_master_daily') is not null then
        update review.shareholder_holdings_daily as h
        set holding_pct_of_outstanding = round(
                coalesce(h.shares_held_override, h.shares_held_raw, 0)
                / nullif(coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0), 0),
                8
            )
        from review.security_master_daily s
        where s.review_row_id = h.security_review_row_id;
    end if;

    if to_regclass('export.shareholder_holdings_final') is not null
       and to_regclass('review.shareholder_holdings_daily') is not null then
        update export.shareholder_holdings_final as e
        set holding_pct_of_outstanding = round(
                coalesce(r.holding_pct_of_outstanding, e.holding_pct_of_outstanding),
                8
            )
        from review.shareholder_holdings_daily r
        where r.review_row_id = e.origin_review_row_id;
    end if;

    if to_regclass('export.shareholder_holdings_preview') is not null
       and to_regclass('review.shareholder_holdings_daily') is not null then
        update export.shareholder_holdings_preview as e
        set holding_pct_of_outstanding = round(
                coalesce(r.holding_pct_of_outstanding, e.holding_pct_of_outstanding),
                8
            )
        from review.shareholder_holdings_daily r
        where r.review_row_id = e.origin_review_row_id;
    end if;
end;
$$;
