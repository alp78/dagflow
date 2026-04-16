create or replace function workflow.validate_dataset(
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
    v_current_state text;
begin
    select review_state
    into v_current_state
    from workflow.dataset_review_state
    where pipeline_code = p_pipeline_code
      and dataset_code = p_dataset_code
      and run_id = p_run_id;

    if v_current_state = 'exported' then
        raise exception
            'Run % for pipeline % has already been exported. Reset the latest daily run before validating again.',
            p_run_id,
            p_pipeline_code;
    end if;

    return workflow.set_dataset_review_state(
        p_pipeline_code,
        p_dataset_code,
        p_run_id,
        p_business_date,
        'approved',
        p_actor,
        coalesce(
            p_notes,
            'Dataset validated by reviewer and handed back to Dagster for export'
        )
    );
end;
$$;
