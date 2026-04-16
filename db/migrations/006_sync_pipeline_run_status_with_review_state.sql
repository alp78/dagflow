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

    update observability.pipeline_runs
    set status = p_next_state,
        ended_at = case
            when p_next_state in ('approved', 'rejected', 'exported') then coalesce(ended_at, timezone('utc', now()))
            else ended_at
        end,
        metadata = metadata || jsonb_build_object('workflow_state', p_next_state, 'workflow_actor', p_actor)
    where run_id = p_run_id;

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

update observability.pipeline_runs as runs
set status = review.review_state,
    metadata = runs.metadata || jsonb_build_object('workflow_state', review.review_state, 'workflow_actor', review.last_actor)
from workflow.dataset_review_state as review
where review.run_id = runs.run_id
  and runs.status is distinct from review.review_state;
