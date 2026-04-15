create or replace function audit.log_review_change(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_review_table text,
    p_row_pk text,
    p_column_name text,
    p_old_value text,
    p_new_value text,
    p_change_reason text,
    p_changed_by text,
    p_origin_mart_row_hash text,
    p_current_row_hash text
)
returns bigint
language plpgsql
security definer
as
$$
declare
    v_change_id bigint;
begin
    insert into audit.change_log (
        pipeline_code,
        dataset_code,
        run_id,
        business_date,
        review_table,
        row_pk,
        column_name,
        old_value,
        new_value,
        change_reason,
        changed_by,
        origin_mart_row_hash,
        current_row_hash
    )
    values (
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        p_review_table,
        p_row_pk,
        p_column_name,
        p_old_value,
        p_new_value,
        p_change_reason,
        p_changed_by,
        p_origin_mart_row_hash,
        p_current_row_hash
    )
    returning change_id into v_change_id;

    return v_change_id;
end;
$$;

create or replace function lineage.record_row_edge(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_from_layer text,
    p_from_table text,
    p_from_row_hash text,
    p_to_layer text,
    p_to_table text,
    p_to_row_hash text,
    p_relationship_type text default 'derived_from',
    p_metadata jsonb default '{}'::jsonb
)
returns bigint
language plpgsql
security definer
as
$$
declare
    v_edge_id bigint;
begin
    insert into lineage.row_lineage_edges (
        pipeline_code,
        dataset_code,
        run_id,
        business_date,
        from_layer,
        from_table,
        from_row_hash,
        to_layer,
        to_table,
        to_row_hash,
        relationship_type,
        metadata
    )
    values (
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        p_from_layer,
        p_from_table,
        p_from_row_hash,
        p_to_layer,
        p_to_table,
        p_to_row_hash,
        p_relationship_type,
        coalesce(p_metadata, '{}'::jsonb)
    )
    returning edge_id into v_edge_id;

    return v_edge_id;
end;
$$;

create or replace function observability.capture_failure(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_step_name text,
    p_stage text,
    p_business_date date,
    p_source_context jsonb,
    p_error_class text,
    p_error_message text,
    p_diagnostic_metadata jsonb default '{}'::jsonb,
    p_failure_rows jsonb default '[]'::jsonb
)
returns uuid
language plpgsql
security definer
as
$$
declare
    v_failure_id uuid;
    v_row jsonb;
begin
    insert into observability.pipeline_failures (
        run_id,
        pipeline_code,
        dataset_code,
        step_name,
        stage,
        business_date,
        source_context,
        error_class,
        error_message,
        diagnostic_metadata
    )
    values (
        p_run_id,
        p_pipeline_code,
        p_dataset_code,
        p_step_name,
        p_stage,
        p_business_date,
        coalesce(p_source_context, '{}'::jsonb),
        p_error_class,
        p_error_message,
        coalesce(p_diagnostic_metadata, '{}'::jsonb)
    )
    returning failure_id into v_failure_id;

    for v_row in
        select value
        from jsonb_array_elements(coalesce(p_failure_rows, '[]'::jsonb))
    loop
        insert into observability.pipeline_failure_rows (
            failure_id,
            source_record_id,
            source_file_name,
            source_file_row_number,
            row_hash,
            row_context
        )
        values (
            v_failure_id,
            v_row ->> 'source_record_id',
            v_row ->> 'source_file_name',
            nullif(v_row ->> 'source_file_row_number', '')::integer,
            v_row ->> 'row_hash',
            v_row
        );
    end loop;

    return v_failure_id;
end;
$$;

create or replace function review.apply_cell_edit(
    p_review_table regclass,
    p_row_id bigint,
    p_column_name text,
    p_new_value text,
    p_changed_by text,
    p_change_reason text default null
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_schema_name text;
    v_table_name text;
    v_table_fq text;
    v_target_type text;
    v_old_value text;
    v_pipeline_code text;
    v_dataset_code text;
    v_run_id uuid;
    v_business_date date;
    v_origin_hash text;
    v_current_hash text;
    v_result jsonb;
begin
    select n.nspname, c.relname, format('%I.%I', n.nspname, c.relname)
    into v_schema_name, v_table_name, v_table_fq
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where c.oid = p_review_table;

    if v_schema_name <> 'review' then
        raise exception 'Only review schema tables are editable';
    end if;

    if not exists (
        select 1
        from control.dataset_registry
        where review_table_name = v_table_fq
    ) then
        raise exception 'Review table % is not registered in control.dataset_registry', v_table_fq;
    end if;

    select format_type(a.atttypid, a.atttypmod)
    into v_target_type
    from pg_attribute a
    where a.attrelid = p_review_table
      and a.attname = p_column_name
      and a.attnum > 0
      and not a.attisdropped;

    if v_target_type is null then
        raise exception 'Column % not found on %', p_column_name, v_table_fq;
    end if;

    execute format('select %I::text from %s where review_row_id = $1', p_column_name, v_table_fq)
    into v_old_value
    using p_row_id;

    execute format(
        'update %s as t
         set %I = cast($1 as %s),
             edited_cells = coalesce(t.edited_cells, ''{}''::jsonb) || jsonb_build_object(
                 $2,
                 jsonb_build_object(
                     ''old'', to_jsonb($3),
                     ''new'', to_jsonb($1),
                     ''changed_by'', $4,
                     ''changed_at'', timezone(''utc'', now()),
                     ''reason'', $5
                 )
             ),
             manual_edit_count = coalesce(t.manual_edit_count, 0) + 1,
             row_version = coalesce(t.row_version, 0) + 1,
             current_row_hash = md5(coalesce(t.origin_mart_row_hash, '''') || ''|'' || $2 || ''|'' || coalesce($1, '''') || ''|'' || (coalesce(t.row_version, 0) + 1)::text),
             updated_at = timezone(''utc'', now())
         where t.review_row_id = $6
         returning jsonb_build_object(
             ''review_row_id'', t.review_row_id,
             ''table_name'', %L,
             ''column_name'', $2,
             ''old_value'', $3,
             ''new_value'', $1,
             ''row_version'', t.row_version,
             ''current_row_hash'', t.current_row_hash
         )',
        v_table_fq,
        p_column_name,
        v_target_type,
        v_table_fq
    )
    into v_result
    using p_new_value, p_column_name, v_old_value, p_changed_by, p_change_reason, p_row_id;

    if v_result is null then
        raise exception 'Row % not found in %', p_row_id, v_table_fq;
    end if;

    execute format(
        'select pipeline_code, dataset_code, run_id, business_date, origin_mart_row_hash, current_row_hash
         from %s
         where review_row_id = $1',
        v_table_fq
    )
    into v_pipeline_code, v_dataset_code, v_run_id, v_business_date, v_origin_hash, v_current_hash
    using p_row_id;

    perform audit.log_review_change(
        v_pipeline_code,
        v_dataset_code,
        v_run_id,
        v_business_date,
        v_table_fq,
        p_row_id::text,
        p_column_name,
        v_old_value,
        p_new_value,
        p_change_reason,
        p_changed_by,
        v_origin_hash,
        v_current_hash
    );

    return v_result;
end;
$$;

create or replace function review.apply_row_edit(
    p_review_table regclass,
    p_row_id bigint,
    p_updates jsonb,
    p_changed_by text,
    p_change_reason text default null
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_column_name text;
    v_value text;
begin
    for v_column_name, v_value in
        select key, value
        from jsonb_each_text(coalesce(p_updates, '{}'::jsonb))
    loop
        perform review.apply_cell_edit(
            p_review_table,
            p_row_id,
            v_column_name,
            v_value,
            p_changed_by,
            p_change_reason
        );
    end loop;

    return review.get_row_diff(p_review_table, p_row_id);
end;
$$;

create or replace function review.get_edited_cells(
    p_review_table regclass,
    p_row_id bigint
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_table_fq text;
    v_result jsonb;
begin
    select format('%I.%I', n.nspname, c.relname)
    into v_table_fq
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where c.oid = p_review_table;

    execute format('select edited_cells from %s where review_row_id = $1', v_table_fq)
    into v_result
    using p_row_id;

    return coalesce(v_result, '{}'::jsonb);
end;
$$;

create or replace function review.get_row_diff(
    p_review_table regclass,
    p_row_id bigint
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_table_fq text;
    v_result jsonb;
begin
    select format('%I.%I', n.nspname, c.relname)
    into v_table_fq
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where c.oid = p_review_table;

    execute format(
        'select jsonb_build_object(
            ''review_row_id'', review_row_id,
            ''origin_mart_row_hash'', origin_mart_row_hash,
            ''current_row_hash'', current_row_hash,
            ''row_version'', row_version,
            ''edited_cells'', edited_cells
        )
        from %s
        where review_row_id = $1',
        v_table_fq
    )
    into v_result
    using p_row_id;

    return coalesce(v_result, '{}'::jsonb);
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
    returning row_to_json(s)::jsonb into v_result;

    return coalesce(v_result, '{}'::jsonb);
end;
$$;

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
begin
    with metrics as (
        select
            h.review_row_id,
            coalesce(h.shares_held_override, h.shares_held_raw, 0) as shares_held_value,
            coalesce(h.reviewed_market_value_override, h.reviewed_market_value_raw, 0) as market_value_value,
            nullif(coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0), 0) as outstanding_shares,
            sum(coalesce(h.reviewed_market_value_override, h.reviewed_market_value_raw, 0))
                over (partition by h.run_id, h.filer_cik) as filer_market_value
        from review.shareholder_holdings_daily h
        left join review.security_master_daily s
            on s.review_row_id = h.security_review_row_id
        where h.review_row_id = p_row_id
    )
    update review.shareholder_holdings_daily as h
    set derived_price_per_share = case
            when m.shares_held_value = 0 then null
            else round(m.market_value_value / nullif(m.shares_held_value, 0), 6)
        end,
        holding_pct_of_outstanding = case
            when m.outstanding_shares is null then null
            else round(m.shares_held_value / m.outstanding_shares, 6)
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
    where h.review_row_id = m.review_row_id
    returning row_to_json(h)::jsonb into v_result;

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
            6
        ),
        updated_at = timezone('utc', now())
    from review.security_master_daily s
    where h.security_review_row_id = s.review_row_id
      and h.run_id = p_run_id;

    get diagnostics v_rows = row_count;
    return v_rows;
end;
$$;

create or replace function workflow.set_dataset_review_state(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_next_state text,
    p_actor text,
    p_notes text default null
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_result jsonb;
begin
    if p_dataset_code = 'security_master' then
        update review.security_master_daily
        set approval_state = p_next_state,
            updated_at = timezone('utc', now())
        where run_id = p_run_id;
    elsif p_dataset_code = 'shareholder_holdings' then
        update review.shareholder_holdings_daily
        set approval_state = p_next_state,
            updated_at = timezone('utc', now())
        where run_id = p_run_id;
    end if;

    insert into workflow.dataset_review_state (
        pipeline_code,
        dataset_code,
        run_id,
        business_date,
        review_state,
        review_required,
        review_started_at,
        review_completed_at,
        last_action,
        last_actor,
        notes
    )
    values (
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        p_next_state,
        true,
        case when p_next_state in ('in_review', 'pending_review', 'reopened') then timezone('utc', now()) else null end,
        case when p_next_state in ('approved', 'rejected', 'exported') then timezone('utc', now()) else null end,
        p_next_state,
        p_actor,
        p_notes
    )
    on conflict (pipeline_code, dataset_code, run_id) do update
    set business_date = excluded.business_date,
        review_state = excluded.review_state,
        review_started_at = case
            when excluded.review_state in ('in_review', 'pending_review', 'reopened') then timezone('utc', now())
            else workflow.dataset_review_state.review_started_at
        end,
        review_completed_at = case
            when excluded.review_state in ('approved', 'rejected', 'exported') then timezone('utc', now())
            else null
        end,
        last_action = excluded.last_action,
        last_actor = excluded.last_actor,
        notes = coalesce(excluded.notes, workflow.dataset_review_state.notes),
        updated_at = timezone('utc', now());

    update control.pipeline_state
    set approval_state = p_next_state,
        last_run_id = p_run_id,
        last_business_date = p_business_date,
        last_transition_by = p_actor,
        paused_at = case when p_next_state = 'pending_review' then timezone('utc', now()) else null end,
        updated_at = timezone('utc', now())
    where pipeline_code = p_pipeline_code;

    insert into audit.workflow_events (
        pipeline_code,
        dataset_code,
        run_id,
        business_date,
        action_name,
        action_status,
        actor,
        notes,
        event_payload
    )
    values (
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        'set_review_state',
        p_next_state,
        p_actor,
        p_notes,
        jsonb_build_object('next_state', p_next_state)
    );

    select jsonb_build_object(
        'pipeline_code', p_pipeline_code,
        'dataset_code', p_dataset_code,
        'run_id', p_run_id,
        'business_date', p_business_date,
        'review_state', p_next_state,
        'actor', p_actor
    )
    into v_result;

    return v_result;
end;
$$;

create or replace function workflow.publish_review_snapshot(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_actor text default 'system',
    p_notes text default null
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_result jsonb;
begin
    if p_pipeline_code = 'security_master' and to_regclass('marts.dim_security') is not null then
        insert into review.security_master_daily (
            run_id,
            business_date,
            origin_mart_row_id,
            origin_mart_row_hash,
            current_row_hash,
            source_record_id,
            cik,
            ticker,
            issuer_name,
            exchange,
            shares_outstanding_raw,
            free_float_pct_raw,
            investability_factor_raw,
            free_float_shares,
            investable_shares,
            review_materiality_score
        )
        select
            p_run_id,
            p_business_date,
            security_id,
            row_hash,
            row_hash,
            source_record_id,
            cik,
            ticker,
            issuer_name,
            exchange,
            shares_outstanding_raw,
            free_float_pct_raw,
            investability_factor_raw,
            free_float_shares,
            investable_shares,
            review_materiality_score
        from marts.dim_security
        where run_id = p_run_id
        on conflict (run_id, cik) do update
        set origin_mart_row_id = excluded.origin_mart_row_id,
            origin_mart_row_hash = excluded.origin_mart_row_hash,
            current_row_hash = excluded.current_row_hash,
            source_record_id = excluded.source_record_id,
            ticker = excluded.ticker,
            issuer_name = excluded.issuer_name,
            exchange = excluded.exchange,
            shares_outstanding_raw = excluded.shares_outstanding_raw,
            free_float_pct_raw = excluded.free_float_pct_raw,
            investability_factor_raw = excluded.investability_factor_raw,
            free_float_shares = excluded.free_float_shares,
            investable_shares = excluded.investable_shares,
            review_materiality_score = excluded.review_materiality_score,
            updated_at = timezone('utc', now());
    elsif p_pipeline_code = 'shareholder_holdings' and to_regclass('marts.fact_shareholder_holding') is not null then
        insert into review.shareholder_holdings_daily (
            run_id,
            business_date,
            origin_mart_row_id,
            origin_mart_row_hash,
            current_row_hash,
            source_record_id,
            accession_number,
            filer_cik,
            filer_name,
            security_identifier,
            security_name,
            security_review_row_id,
            shares_held_raw,
            reviewed_market_value_raw,
            source_confidence_raw,
            holding_pct_of_outstanding,
            derived_price_per_share,
            portfolio_weight
        )
        select
            p_run_id,
            p_business_date,
            h.holding_id,
            h.row_hash,
            h.row_hash,
            h.source_record_id,
            h.accession_number,
            h.filer_cik,
            h.filer_name,
            h.security_identifier,
            h.security_name,
            s.review_row_id,
            h.shares_held_raw,
            h.reviewed_market_value_raw,
            h.source_confidence_raw,
            h.holding_pct_of_outstanding,
            h.derived_price_per_share,
            h.portfolio_weight
        from marts.fact_shareholder_holding h
        left join review.security_master_daily s
            on s.business_date = p_business_date
           and s.ticker = h.security_identifier
        where h.run_id = p_run_id
        on conflict (run_id, accession_number, filer_cik, security_identifier) do update
        set origin_mart_row_id = excluded.origin_mart_row_id,
            origin_mart_row_hash = excluded.origin_mart_row_hash,
            current_row_hash = excluded.current_row_hash,
            source_record_id = excluded.source_record_id,
            filer_name = excluded.filer_name,
            security_name = excluded.security_name,
            security_review_row_id = excluded.security_review_row_id,
            shares_held_raw = excluded.shares_held_raw,
            reviewed_market_value_raw = excluded.reviewed_market_value_raw,
            source_confidence_raw = excluded.source_confidence_raw,
            holding_pct_of_outstanding = excluded.holding_pct_of_outstanding,
            derived_price_per_share = excluded.derived_price_per_share,
            portfolio_weight = excluded.portfolio_weight,
            updated_at = timezone('utc', now());
    end if;

    v_result := workflow.set_dataset_review_state(
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        'pending_review',
        p_actor,
        coalesce(p_notes, 'Review snapshot published')
    );

    return v_result;
end;
$$;

create or replace function workflow.approve_dataset(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_actor text default 'system',
    p_notes text default null
)
returns jsonb
language sql
security definer
as
$$
    select workflow.set_dataset_review_state(
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        'approved',
        p_actor,
        coalesce(p_notes, 'Dataset approved')
    );
$$;

create or replace function workflow.reject_dataset(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_actor text default 'system',
    p_notes text default null
)
returns jsonb
language sql
security definer
as
$$
    select workflow.set_dataset_review_state(
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        'rejected',
        p_actor,
        coalesce(p_notes, 'Dataset rejected')
    );
$$;

create or replace function workflow.reopen_dataset(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_actor text default 'system',
    p_notes text default null
)
returns jsonb
language sql
security definer
as
$$
    select workflow.set_dataset_review_state(
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        'reopened',
        p_actor,
        coalesce(p_notes, 'Dataset reopened')
    );
$$;

create or replace function workflow.finalize_export(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid,
    p_business_date date,
    p_actor text default 'system',
    p_notes text default null
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_result jsonb;
begin
    if p_pipeline_code = 'security_master' then
        insert into export.security_master_final (
            run_id,
            business_date,
            origin_review_row_id,
            origin_review_row_hash,
            file_id,
            ticker,
            issuer_name,
            investable_shares,
            review_materiality_score
        )
        select
            p_run_id,
            p_business_date,
            review_row_id,
            current_row_hash,
            concat('security_master_', p_business_date::text, '.csv'),
            ticker,
            issuer_name,
            investable_shares,
            review_materiality_score
        from review.security_master_daily
        where run_id = p_run_id
          and approval_state in ('approved', 'exported')
        on conflict (run_id, origin_review_row_id) do update
        set origin_review_row_hash = excluded.origin_review_row_hash,
            file_id = excluded.file_id,
            ticker = excluded.ticker,
            issuer_name = excluded.issuer_name,
            investable_shares = excluded.investable_shares,
            review_materiality_score = excluded.review_materiality_score,
            exported_at = timezone('utc', now());
    elsif p_pipeline_code = 'shareholder_holdings' then
        insert into export.shareholder_holdings_final (
            run_id,
            business_date,
            origin_review_row_id,
            origin_review_row_hash,
            file_id,
            filer_name,
            security_name,
            shares_held,
            holding_pct_of_outstanding
        )
        select
            p_run_id,
            p_business_date,
            review_row_id,
            current_row_hash,
            concat('shareholder_holdings_', p_business_date::text, '.csv'),
            filer_name,
            security_name,
            coalesce(shares_held_override, shares_held_raw),
            holding_pct_of_outstanding
        from review.shareholder_holdings_daily
        where run_id = p_run_id
          and approval_state in ('approved', 'exported')
        on conflict (run_id, origin_review_row_id) do update
        set origin_review_row_hash = excluded.origin_review_row_hash,
            file_id = excluded.file_id,
            filer_name = excluded.filer_name,
            security_name = excluded.security_name,
            shares_held = excluded.shares_held,
            holding_pct_of_outstanding = excluded.holding_pct_of_outstanding,
            exported_at = timezone('utc', now());
    end if;

    v_result := workflow.set_dataset_review_state(
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        'exported',
        p_actor,
        coalesce(p_notes, 'Export finalized')
    );

    return v_result;
end;
$$;

create or replace function workflow.set_pipeline_enabled(
    p_pipeline_code text,
    p_is_enabled boolean,
    p_actor text default 'system'
)
returns jsonb
language plpgsql
security definer
as
$$
declare
    v_result jsonb;
begin
    update control.pipeline_registry
    set is_enabled = p_is_enabled,
        updated_at = timezone('utc', now())
    where pipeline_code = p_pipeline_code;

    update control.pipeline_state
    set is_active = p_is_enabled,
        activated_at = case when p_is_enabled then timezone('utc', now()) else activated_at end,
        deactivated_at = case when p_is_enabled then null else timezone('utc', now()) end,
        last_transition_by = p_actor,
        updated_at = timezone('utc', now())
    where pipeline_code = p_pipeline_code;

    insert into audit.workflow_events (
        pipeline_code,
        dataset_code,
        action_name,
        action_status,
        actor,
        event_payload
    )
    select
        p.pipeline_code,
        p.dataset_code,
        'set_pipeline_enabled',
        case when p_is_enabled then 'enabled' else 'disabled' end,
        p_actor,
        jsonb_build_object('is_enabled', p_is_enabled)
    from control.pipeline_registry p
    where p.pipeline_code = p_pipeline_code;

    select jsonb_build_object(
        'pipeline_code', pipeline_code,
        'is_enabled', is_enabled
    )
    into v_result
    from control.pipeline_registry
    where pipeline_code = p_pipeline_code;

    return coalesce(v_result, '{}'::jsonb);
end;
$$;

create or replace function query_api.fn_security_shareholder_breakdown(
    p_security_review_row_id bigint
)
returns table (
    filer_name text,
    shares_held numeric,
    holding_pct_of_outstanding numeric,
    portfolio_weight numeric,
    approval_state text
)
language sql
stable
as
$$
    select
        h.filer_name,
        coalesce(h.shares_held_override, h.shares_held_raw) as shares_held,
        h.holding_pct_of_outstanding,
        h.portfolio_weight,
        h.approval_state
    from review.shareholder_holdings_daily h
    where h.security_review_row_id = p_security_review_row_id
    order by coalesce(h.shares_held_override, h.shares_held_raw) desc nulls last;
$$;

create or replace function query_api.fn_security_history(
    p_security_review_row_id bigint
)
returns table (
    changed_at timestamptz,
    column_name text,
    old_value text,
    new_value text,
    changed_by text,
    change_reason text
)
language sql
stable
as
$$
    select
        c.changed_at,
        c.column_name,
        c.old_value,
        c.new_value,
        c.changed_by,
        c.change_reason
    from audit.change_log c
    where c.review_table = 'review.security_master_daily'
      and c.row_pk = p_security_review_row_id::text
    order by c.changed_at desc;
$$;

create or replace function query_api.fn_current_run_edit_summary(
    p_pipeline_code text,
    p_dataset_code text,
    p_run_id uuid
)
returns table (
    changed_by text,
    edits bigint,
    distinct_rows bigint,
    last_changed_at timestamptz
)
language sql
stable
as
$$
    select
        c.changed_by,
        count(*) as edits,
        count(distinct c.row_pk) as distinct_rows,
        max(c.changed_at) as last_changed_at
    from audit.change_log c
    where c.pipeline_code = p_pipeline_code
      and c.dataset_code = p_dataset_code
      and c.run_id = p_run_id
    group by c.changed_by
    order by edits desc;
$$;

create or replace function query_api.fn_run_failure_rows(
    p_run_id uuid
)
returns table (
    failure_id uuid,
    pipeline_code text,
    dataset_code text,
    step_name text,
    stage text,
    error_class text,
    error_message text,
    source_record_id text,
    source_file_name text,
    source_file_row_number integer,
    row_hash text,
    row_context jsonb,
    created_at timestamptz
)
language sql
stable
as
$$
    select
        f.failure_id,
        f.pipeline_code,
        f.dataset_code,
        f.step_name,
        f.stage,
        f.error_class,
        f.error_message,
        r.source_record_id,
        r.source_file_name,
        r.source_file_row_number,
        r.row_hash,
        r.row_context,
        f.created_at
    from observability.pipeline_failures f
    left join observability.pipeline_failure_rows r
        on r.failure_id = f.failure_id
    where f.run_id = p_run_id
    order by f.created_at desc, r.failure_row_id;
$$;

create or replace function query_api.fn_row_lineage_trace(
    p_row_hash text
)
returns table (
    pipeline_code text,
    dataset_code text,
    run_id uuid,
    from_layer text,
    from_table text,
    from_row_hash text,
    to_layer text,
    to_table text,
    to_row_hash text,
    relationship_type text,
    metadata jsonb,
    recorded_at timestamptz
)
language sql
stable
as
$$
    select
        pipeline_code,
        dataset_code,
        run_id,
        from_layer,
        from_table,
        from_row_hash,
        to_layer,
        to_table,
        to_row_hash,
        relationship_type,
        metadata,
        recorded_at
    from lineage.row_lineage_edges
    where from_row_hash = p_row_hash
       or to_row_hash = p_row_hash
    order by recorded_at;
$$;

grant execute on all functions in schema audit, review, workflow, query_api, calc, observability, lineage
    to pipeline_svc, ui_svc;

grant execute on function query_api.fn_security_shareholder_breakdown(bigint) to dashboard_ro_svc;
grant execute on function query_api.fn_security_history(bigint) to dashboard_ro_svc;
grant execute on function query_api.fn_current_run_edit_summary(text, text, uuid) to dashboard_ro_svc;
grant execute on function query_api.fn_run_failure_rows(uuid) to dashboard_ro_svc;
grant execute on function query_api.fn_row_lineage_trace(text) to dashboard_ro_svc;
