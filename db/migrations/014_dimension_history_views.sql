create or replace function query_api.fn_security_dimension_history(
    p_security_review_row_id bigint
)
returns table (
    scd_version bigint,
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean,
    business_date date,
    run_id uuid,
    ticker text,
    issuer_name text,
    exchange text,
    shares_outstanding_raw numeric,
    free_float_pct_raw numeric,
    investability_factor_raw numeric,
    free_float_shares numeric,
    investable_shares numeric,
    review_materiality_score numeric,
    changed_columns text[]
)
language plpgsql
stable
as
$$
begin
    if to_regclass('marts.dim_security_snapshot') is null then
        return;
    end if;

    return query
    with selected_security as (
        select ticker
        from review.security_master_daily
        where review_row_id = p_security_review_row_id
    ),
    history as (
        select
            row_number() over (
                partition by snapshot.ticker
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as scd_version,
            lag(snapshot.issuer_name) over (
                partition by snapshot.ticker
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_issuer_name,
            lag(snapshot.exchange) over (
                partition by snapshot.ticker
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_exchange,
            lag(snapshot.shares_outstanding_raw) over (
                partition by snapshot.ticker
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_shares_outstanding_raw,
            lag(snapshot.free_float_pct_raw) over (
                partition by snapshot.ticker
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_free_float_pct_raw,
            lag(snapshot.investability_factor_raw) over (
                partition by snapshot.ticker
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_investability_factor_raw,
            lag(snapshot.free_float_shares) over (
                partition by snapshot.ticker
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_free_float_shares,
            lag(snapshot.investable_shares) over (
                partition by snapshot.ticker
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_investable_shares,
            lag(snapshot.review_materiality_score) over (
                partition by snapshot.ticker
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_review_materiality_score,
            snapshot.*
        from marts.dim_security_snapshot snapshot
        join selected_security selected
          on selected.ticker = snapshot.ticker
    )
    select
        history.scd_version,
        history.dbt_valid_from as valid_from,
        history.dbt_valid_to as valid_to,
        history.dbt_valid_to is null as is_current,
        history.business_date,
        history.run_id,
        history.ticker,
        history.issuer_name,
        history.exchange,
        history.shares_outstanding_raw,
        history.free_float_pct_raw,
        history.investability_factor_raw,
        history.free_float_shares,
        history.investable_shares,
        history.review_materiality_score,
        array_remove(
            array[
                case when history.scd_version > 1 and history.prev_issuer_name is distinct from history.issuer_name then 'issuer_name' end,
                case when history.scd_version > 1 and history.prev_exchange is distinct from history.exchange then 'exchange' end,
                case when history.scd_version > 1 and history.prev_shares_outstanding_raw is distinct from history.shares_outstanding_raw then 'shares_outstanding_raw' end,
                case when history.scd_version > 1 and history.prev_free_float_pct_raw is distinct from history.free_float_pct_raw then 'free_float_pct_raw' end,
                case when history.scd_version > 1 and history.prev_investability_factor_raw is distinct from history.investability_factor_raw then 'investability_factor_raw' end,
                case when history.scd_version > 1 and history.prev_free_float_shares is distinct from history.free_float_shares then 'free_float_shares' end,
                case when history.scd_version > 1 and history.prev_investable_shares is distinct from history.investable_shares then 'investable_shares' end,
                case when history.scd_version > 1 and history.prev_review_materiality_score is distinct from history.review_materiality_score then 'review_materiality_score' end
            ],
            null
        ) as changed_columns
    from history
    order by history.scd_version desc;
end;
$$;

create or replace function query_api.fn_holding_dimension_history(
    p_holding_review_row_id bigint
)
returns table (
    scd_version bigint,
    valid_from timestamp,
    valid_to timestamp,
    is_current boolean,
    business_date date,
    run_id uuid,
    filer_cik text,
    filer_name text,
    accession_number text,
    report_period date,
    changed_columns text[]
)
language plpgsql
stable
as
$$
begin
    if to_regclass('marts.dim_shareholder_snapshot') is null then
        return;
    end if;

    return query
    with selected_holding as (
        select filer_cik
        from review.shareholder_holdings_daily
        where review_row_id = p_holding_review_row_id
    ),
    history as (
        select
            row_number() over (
                partition by snapshot.filer_cik
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as scd_version,
            lag(snapshot.filer_name) over (
                partition by snapshot.filer_cik
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_filer_name,
            lag(snapshot.accession_number) over (
                partition by snapshot.filer_cik
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_accession_number,
            lag(snapshot.report_period) over (
                partition by snapshot.filer_cik
                order by snapshot.dbt_valid_from, coalesce(snapshot.dbt_valid_to, 'infinity'::timestamp)
            ) as prev_report_period,
            snapshot.*
        from marts.dim_shareholder_snapshot snapshot
        join selected_holding selected
          on selected.filer_cik = snapshot.filer_cik
    )
    select
        history.scd_version,
        history.dbt_valid_from as valid_from,
        history.dbt_valid_to as valid_to,
        history.dbt_valid_to is null as is_current,
        history.business_date,
        history.run_id,
        history.filer_cik,
        history.filer_name,
        history.accession_number,
        history.report_period,
        array_remove(
            array[
                case when history.scd_version > 1 and history.prev_filer_name is distinct from history.filer_name then 'filer_name' end,
                case when history.scd_version > 1 and history.prev_accession_number is distinct from history.accession_number then 'accession_number' end,
                case when history.scd_version > 1 and history.prev_report_period is distinct from history.report_period then 'report_period' end
            ],
            null
        ) as changed_columns
    from history
    order by history.scd_version desc;
end;
$$;

grant execute on function query_api.fn_security_dimension_history(bigint) to pipeline_svc, ui_svc, dashboard_ro_svc;
grant execute on function query_api.fn_holding_dimension_history(bigint) to pipeline_svc, ui_svc, dashboard_ro_svc;
