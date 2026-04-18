do
$$
begin
    if to_regclass('marts.fact_shareholder_holding') is not null
       and to_regclass('review.shareholder_holdings_daily') is not null
       and to_regclass('review.security_master_daily') is not null then
        update marts.fact_shareholder_holding as m
        set holding_pct_of_outstanding = round(
                (
                    case
                        when coalesce(r.edited_cells, '{}'::jsonb) ? 'shares_held_raw' then
                            coalesce(
                                nullif(
                                    r.edited_cells -> 'shares_held_raw' ->> 'old',
                                    ''
                                )::numeric,
                                r.shares_held_raw,
                                0
                            )
                        else coalesce(r.shares_held_raw, 0)
                    end
                ) / nullif(
                    (
                        case
                            when coalesce(s.edited_cells, '{}'::jsonb) ? 'shares_outstanding_raw' then
                                coalesce(
                                    nullif(
                                        s.edited_cells -> 'shares_outstanding_raw' ->> 'old',
                                        ''
                                    )::numeric,
                                    s.shares_outstanding_raw,
                                    0
                                )
                            else coalesce(s.shares_outstanding_raw, 0)
                        end
                    ),
                    0
                ),
                8
            )
        from review.shareholder_holdings_daily as r
        join review.security_master_daily as s
            on s.review_row_id = r.security_review_row_id
        where r.origin_mart_row_id = m.holding_id;
    end if;
end;
$$;
