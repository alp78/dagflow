drop function if exists query_api.fn_security_shareholder_breakdown(bigint);

create function query_api.fn_security_shareholder_breakdown(
    p_security_review_row_id bigint
)
returns table (
    holding_review_row_id bigint,
    filer_name text,
    shares_held numeric,
    holding_pct_of_outstanding numeric,
    portfolio_weight numeric,
    approval_state text,
    shares_held_manually_edited boolean
)
language sql
stable
as
$$
    select
        h.review_row_id as holding_review_row_id,
        h.filer_name,
        coalesce(h.shares_held_override, h.shares_held_raw) as shares_held,
        h.holding_pct_of_outstanding,
        h.portfolio_weight,
        h.approval_state,
        coalesce(h.edited_cells ? 'shares_held_raw', false) as shares_held_manually_edited
    from review.shareholder_holdings_daily h
    where h.security_review_row_id = p_security_review_row_id
    order by coalesce(h.shares_held_override, h.shares_held_raw) desc nulls last, h.filer_name;
$$;

grant execute on function query_api.fn_security_shareholder_breakdown(bigint) to pipeline_svc, ui_svc, dashboard_ro_svc;
