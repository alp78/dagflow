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

    if v_table_fq = 'review.shareholder_holdings_daily' then
        return p_column_name = 'shares_held_raw';
    end if;

    return false;
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
    v_table_fq text;
    v_target_type text;
    v_old_value text;
    v_result jsonb;
    v_pipeline_code text;
    v_dataset_code text;
    v_run_id uuid;
    v_business_date date;
    v_origin_hash text;
    v_current_hash text;
begin
    select n.nspname, format('%I.%I', n.nspname, c.relname)
    into v_schema_name, v_table_fq
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

    if not review.is_editable_review_column(p_review_table, p_column_name) then
        raise exception 'Column % on % is not editable in review. Only factual provider corrections are allowed.',
            p_column_name,
            v_table_fq;
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


grant execute on function review.is_editable_review_column(regclass, text) to pipeline_svc, ui_svc;
