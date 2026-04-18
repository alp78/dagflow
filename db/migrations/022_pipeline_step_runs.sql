create table if not exists observability.pipeline_step_runs (
    step_run_id bigint generated always as identity primary key,
    run_id uuid not null references observability.pipeline_runs (run_id) on delete cascade,
    pipeline_code text not null,
    dataset_code text not null,
    business_date date not null,
    step_key text not null,
    step_label text not null,
    step_group text not null,
    sort_order integer not null,
    status text not null default 'pending',
    started_at timestamptz,
    ended_at timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    unique (run_id, step_key)
);

create index if not exists idx_pipeline_step_runs_run
    on observability.pipeline_step_runs (run_id, sort_order);
