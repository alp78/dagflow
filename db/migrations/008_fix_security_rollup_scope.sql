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
            6
        ),
        updated_at = timezone('utc', now())
    from review.security_master_daily s
    where h.security_review_row_id = s.review_row_id
      and s.run_id = p_run_id;

    get diagnostics v_rows = row_count;
    return v_rows;
end;
$$;

grant execute on function calc.recalc_security_rollups(uuid) to pipeline_svc, ui_svc, dashboard_ro_svc;
