create extension if not exists pgcrypto;

do
$$
begin
    if not exists (select 1 from pg_roles where rolname = 'pipeline_svc') then
        create role pipeline_svc;
    end if;
    if not exists (select 1 from pg_roles where rolname = 'ui_svc') then
        create role ui_svc;
    end if;
    if not exists (select 1 from pg_roles where rolname = 'dashboard_ro_svc') then
        create role dashboard_ro_svc;
    end if;
    if not exists (select 1 from pg_roles where rolname = 'migration_admin') then
        create role migration_admin;
    end if;
end
$$;

create schema if not exists control;
create schema if not exists raw;
create schema if not exists staging;
create schema if not exists intermediate;
create schema if not exists marts;
create schema if not exists review;
create schema if not exists audit;
create schema if not exists export;
create schema if not exists observability;
create schema if not exists lineage;
create schema if not exists workflow;
create schema if not exists query_api;
create schema if not exists calc;

revoke create on schema public from public;

create or replace function control.touch_updated_at()
returns trigger
language plpgsql
as
$$
begin
    new.updated_at := timezone('utc', now());
    return new;
end;
$$;

create table if not exists control.dataset_registry (
    dataset_code text primary key,
    dataset_name text not null,
    description text not null,
    review_required boolean not null default true,
    review_table_name text not null unique,
    export_table_name text not null unique,
    export_contract_code text not null,
    calc_package_code text not null,
    approval_workflow_code text not null,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists control.pipeline_registry (
    pipeline_code text primary key,
    pipeline_name text not null,
    dataset_code text not null references control.dataset_registry (dataset_code),
    source_type text not null,
    adapter_type text not null,
    is_enabled boolean not null default true,
    is_schedulable boolean not null default true,
    default_schedule text not null,
    review_required boolean not null default true,
    review_table_name text not null,
    export_contract_code text not null,
    calc_package_code text not null,
    approval_workflow_code text not null,
    storage_prefix text not null,
    owner_team text not null default 'platform',
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists control.pipeline_state (
    pipeline_code text primary key references control.pipeline_registry (pipeline_code),
    dataset_code text not null references control.dataset_registry (dataset_code),
    is_active boolean not null default true,
    approval_state text not null default 'pending_review'
        check (approval_state in ('pending_review', 'in_review', 'approved', 'rejected', 'reopened', 'exported')),
    last_run_id uuid,
    last_business_date date,
    activated_at timestamptz not null default timezone('utc', now()),
    deactivated_at timestamptz,
    paused_at timestamptz,
    pause_reason text,
    last_transition_by text not null default 'system',
    updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists workflow.dataset_review_state (
    pipeline_code text not null references control.pipeline_registry (pipeline_code),
    dataset_code text not null references control.dataset_registry (dataset_code),
    run_id uuid not null,
    business_date date not null,
    review_state text not null default 'pending_review'
        check (review_state in ('pending_review', 'in_review', 'approved', 'rejected', 'reopened', 'exported')),
    review_required boolean not null default true,
    review_started_at timestamptz,
    review_completed_at timestamptz,
    last_action text not null default 'created',
    last_actor text not null default 'system',
    notes text,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now()),
    primary key (pipeline_code, dataset_code, run_id)
);

create table if not exists observability.pipeline_runs (
    run_id uuid primary key default gen_random_uuid(),
    pipeline_code text not null,
    dataset_code text not null,
    business_date date not null,
    status text not null,
    triggered_by text not null default 'system',
    started_at timestamptz not null default timezone('utc', now()),
    ended_at timestamptz,
    orchestrator_run_id text,
    metadata jsonb not null default '{}'::jsonb
);

create table if not exists observability.pipeline_asset_metrics (
    metric_id bigint generated always as identity primary key,
    run_id uuid not null references observability.pipeline_runs (run_id) on delete cascade,
    pipeline_code text not null,
    dataset_code text not null,
    asset_group text not null,
    asset_name text not null,
    metric_name text not null,
    metric_value numeric(24, 6),
    row_count bigint,
    measured_at timestamptz not null default timezone('utc', now()),
    metadata jsonb not null default '{}'::jsonb
);

create table if not exists observability.data_quality_results (
    dq_result_id bigint generated always as identity primary key,
    run_id uuid,
    pipeline_code text,
    dataset_code text,
    test_name text not null,
    status text not null,
    severity text not null default 'error',
    failures bigint,
    invocation_id text,
    details jsonb not null default '{}'::jsonb,
    tested_at timestamptz not null default timezone('utc', now())
);

create table if not exists observability.pipeline_failures (
    failure_id uuid primary key default gen_random_uuid(),
    run_id uuid,
    pipeline_code text not null,
    dataset_code text not null,
    step_name text not null,
    stage text not null,
    business_date date,
    source_context jsonb not null default '{}'::jsonb,
    error_class text not null,
    error_message text not null,
    diagnostic_metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default timezone('utc', now())
);

create table if not exists observability.pipeline_failure_rows (
    failure_row_id bigint generated always as identity primary key,
    failure_id uuid not null references observability.pipeline_failures (failure_id) on delete cascade,
    source_record_id text,
    source_file_name text,
    source_file_row_number integer,
    row_hash text,
    row_context jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default timezone('utc', now())
);

create table if not exists audit.change_log (
    change_id bigint generated always as identity primary key,
    pipeline_code text not null,
    dataset_code text not null,
    run_id uuid,
    business_date date,
    review_table text not null,
    row_pk text not null,
    column_name text not null,
    old_value text,
    new_value text,
    change_reason text,
    changed_by text not null,
    changed_at timestamptz not null default timezone('utc', now()),
    origin_mart_row_hash text,
    current_row_hash text
);

create table if not exists audit.workflow_events (
    workflow_event_id bigint generated always as identity primary key,
    pipeline_code text not null,
    dataset_code text not null,
    run_id uuid,
    business_date date,
    action_name text not null,
    action_status text not null,
    actor text not null,
    notes text,
    event_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default timezone('utc', now())
);

create table if not exists lineage.row_lineage_edges (
    edge_id bigint generated always as identity primary key,
    pipeline_code text not null,
    dataset_code text not null,
    run_id uuid,
    business_date date,
    from_layer text not null,
    from_table text not null,
    from_row_hash text not null,
    to_layer text not null,
    to_table text not null,
    to_row_hash text not null,
    relationship_type text not null default 'derived_from',
    metadata jsonb not null default '{}'::jsonb,
    recorded_at timestamptz not null default timezone('utc', now())
);

create table if not exists lineage.entity_lineage_summary (
    entity_lineage_id bigint generated always as identity primary key,
    pipeline_code text not null,
    dataset_code text not null,
    run_id uuid,
    business_date date,
    entity_key text not null,
    origin_layer text not null,
    current_layer text not null,
    current_row_hash text not null,
    summary jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default timezone('utc', now())
);

create table if not exists lineage.export_file_lineage (
    export_file_lineage_id bigint generated always as identity primary key,
    pipeline_code text not null,
    dataset_code text not null,
    run_id uuid,
    export_batch_id uuid not null,
    file_id text not null,
    origin_review_row_hash text not null,
    export_row_hash text not null,
    created_at timestamptz not null default timezone('utc', now())
);

create table if not exists raw.sec_company_tickers (
    raw_id bigint generated always as identity primary key,
    pipeline_code text not null default 'security_master',
    dataset_code text not null default 'security_master',
    run_id uuid not null,
    business_date date not null,
    ingestion_batch_id uuid not null default gen_random_uuid(),
    source_record_id text not null,
    source_payload jsonb not null,
    source_payload_hash text not null,
    source_file_name text,
    source_file_row_number integer,
    cik text not null,
    ticker text,
    company_name text,
    exchange text,
    row_hash text not null,
    loaded_at timestamptz not null default timezone('utc', now())
);

create table if not exists raw.sec_company_facts (
    raw_id bigint generated always as identity primary key,
    pipeline_code text not null default 'security_master',
    dataset_code text not null default 'security_master',
    run_id uuid not null,
    business_date date not null,
    ingestion_batch_id uuid not null default gen_random_uuid(),
    source_record_id text not null,
    source_payload jsonb not null,
    source_payload_hash text not null,
    source_file_name text,
    source_file_row_number integer,
    cik text not null,
    fact_name text not null,
    fact_value numeric(24, 6),
    unit text,
    row_hash text not null,
    loaded_at timestamptz not null default timezone('utc', now())
);

create table if not exists raw.holdings_13f (
    raw_id bigint generated always as identity primary key,
    pipeline_code text not null default 'shareholder_holdings',
    dataset_code text not null default 'shareholder_holdings',
    run_id uuid not null,
    business_date date not null,
    ingestion_batch_id uuid not null default gen_random_uuid(),
    source_record_id text not null,
    source_payload jsonb not null,
    source_payload_hash text not null,
    source_file_name text,
    source_file_row_number integer,
    accession_number text not null,
    filer_cik text not null,
    security_identifier text not null,
    cusip text,
    shares_held numeric(24, 6),
    market_value numeric(24, 6),
    row_hash text not null,
    loaded_at timestamptz not null default timezone('utc', now())
);

create table if not exists raw.holdings_13f_filers (
    raw_id bigint generated always as identity primary key,
    pipeline_code text not null default 'shareholder_holdings',
    dataset_code text not null default 'shareholder_holdings',
    run_id uuid not null,
    business_date date not null,
    ingestion_batch_id uuid not null default gen_random_uuid(),
    source_record_id text not null,
    source_payload jsonb not null,
    source_payload_hash text not null,
    source_file_name text,
    source_file_row_number integer,
    accession_number text not null,
    filer_cik text not null,
    filer_name text not null,
    report_period date,
    row_hash text not null,
    loaded_at timestamptz not null default timezone('utc', now())
);

create table if not exists review.security_master_daily (
    review_row_id bigint generated always as identity primary key,
    pipeline_code text not null default 'security_master',
    dataset_code text not null default 'security_master',
    run_id uuid not null,
    business_date date not null,
    origin_mart_row_id bigint,
    origin_mart_row_hash text not null,
    current_row_hash text not null,
    row_version integer not null default 1,
    source_record_id text,
    cik text not null,
    ticker text not null,
    issuer_name text not null,
    exchange text,
    shares_outstanding_raw numeric(24, 6),
    free_float_pct_raw numeric(12, 6),
    investability_factor_raw numeric(12, 6),
    shares_outstanding_override numeric(24, 6),
    free_float_pct_override numeric(12, 6),
    investability_factor_override numeric(12, 6),
    free_float_shares numeric(24, 6),
    investable_shares numeric(24, 6),
    review_materiality_score numeric(24, 6),
    approval_state text not null default 'pending_review'
        check (approval_state in ('pending_review', 'in_review', 'approved', 'rejected', 'reopened', 'exported')),
    edited_cells jsonb not null default '{}'::jsonb,
    manual_edit_count integer not null default 0,
    review_notes text,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now()),
    unique (run_id, cik)
);

create table if not exists review.shareholder_holdings_daily (
    review_row_id bigint generated always as identity primary key,
    pipeline_code text not null default 'shareholder_holdings',
    dataset_code text not null default 'shareholder_holdings',
    run_id uuid not null,
    business_date date not null,
    origin_mart_row_id bigint,
    origin_mart_row_hash text not null,
    current_row_hash text not null,
    row_version integer not null default 1,
    source_record_id text,
    accession_number text not null,
    filer_cik text not null,
    filer_name text not null,
    security_identifier text not null,
    security_name text not null,
    security_review_row_id bigint references review.security_master_daily (review_row_id),
    shares_held_raw numeric(24, 6),
    reviewed_market_value_raw numeric(24, 6),
    source_confidence_raw numeric(12, 6),
    shares_held_override numeric(24, 6),
    reviewed_market_value_override numeric(24, 6),
    source_confidence_override numeric(12, 6),
    holding_pct_of_outstanding numeric(24, 6),
    derived_price_per_share numeric(24, 6),
    portfolio_weight numeric(24, 6),
    approval_state text not null default 'pending_review'
        check (approval_state in ('pending_review', 'in_review', 'approved', 'rejected', 'reopened', 'exported')),
    edited_cells jsonb not null default '{}'::jsonb,
    manual_edit_count integer not null default 0,
    review_notes text,
    created_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now()),
    unique (run_id, accession_number, filer_cik, security_identifier)
);

create table if not exists export.security_master_final (
    export_row_id bigint generated always as identity primary key,
    pipeline_code text not null default 'security_master',
    dataset_code text not null default 'security_master',
    run_id uuid not null,
    business_date date not null,
    origin_review_row_id bigint not null references review.security_master_daily (review_row_id),
    origin_review_row_hash text not null,
    export_batch_id uuid not null default gen_random_uuid(),
    file_id text not null,
    ticker text not null,
    issuer_name text not null,
    investable_shares numeric(24, 6),
    review_materiality_score numeric(24, 6),
    exported_at timestamptz not null default timezone('utc', now()),
    unique (run_id, origin_review_row_id)
);

create table if not exists export.shareholder_holdings_final (
    export_row_id bigint generated always as identity primary key,
    pipeline_code text not null default 'shareholder_holdings',
    dataset_code text not null default 'shareholder_holdings',
    run_id uuid not null,
    business_date date not null,
    origin_review_row_id bigint not null references review.shareholder_holdings_daily (review_row_id),
    origin_review_row_hash text not null,
    export_batch_id uuid not null default gen_random_uuid(),
    file_id text not null,
    filer_name text not null,
    security_name text not null,
    shares_held numeric(24, 6),
    holding_pct_of_outstanding numeric(24, 6),
    exported_at timestamptz not null default timezone('utc', now()),
    unique (run_id, origin_review_row_id)
);

create index if not exists idx_pipeline_registry_enabled
    on control.pipeline_registry (is_enabled, is_schedulable);

create index if not exists idx_pipeline_state_run
    on control.pipeline_state (last_run_id, last_business_date);

create index if not exists idx_dataset_review_state_run
    on workflow.dataset_review_state (run_id, business_date, review_state);

create index if not exists idx_pipeline_runs_lookup
    on observability.pipeline_runs (pipeline_code, dataset_code, business_date desc);

create index if not exists idx_asset_metrics_run
    on observability.pipeline_asset_metrics (run_id, asset_group, asset_name);

create index if not exists idx_dq_results_run
    on observability.data_quality_results (run_id, pipeline_code, dataset_code);

create index if not exists idx_pipeline_failures_run
    on observability.pipeline_failures (run_id, pipeline_code, dataset_code, created_at desc);

create index if not exists idx_failure_rows_failure_id
    on observability.pipeline_failure_rows (failure_id);

create index if not exists idx_change_log_run
    on audit.change_log (run_id, changed_at desc);

create index if not exists idx_change_log_row
    on audit.change_log (review_table, row_pk, changed_at desc);

create index if not exists idx_lineage_row_hash
    on lineage.row_lineage_edges (from_row_hash, to_row_hash);

create index if not exists idx_security_review_run
    on review.security_master_daily (run_id, business_date);

create index if not exists idx_holdings_review_run
    on review.shareholder_holdings_daily (run_id, business_date);

drop trigger if exists trg_dataset_registry_touch_updated_at on control.dataset_registry;
create trigger trg_dataset_registry_touch_updated_at
before update on control.dataset_registry
for each row execute function control.touch_updated_at();

drop trigger if exists trg_pipeline_registry_touch_updated_at on control.pipeline_registry;
create trigger trg_pipeline_registry_touch_updated_at
before update on control.pipeline_registry
for each row execute function control.touch_updated_at();

drop trigger if exists trg_pipeline_state_touch_updated_at on control.pipeline_state;
create trigger trg_pipeline_state_touch_updated_at
before update on control.pipeline_state
for each row execute function control.touch_updated_at();

drop trigger if exists trg_dataset_review_state_touch_updated_at on workflow.dataset_review_state;
create trigger trg_dataset_review_state_touch_updated_at
before update on workflow.dataset_review_state
for each row execute function control.touch_updated_at();

drop trigger if exists trg_security_review_touch_updated_at on review.security_master_daily;
create trigger trg_security_review_touch_updated_at
before update on review.security_master_daily
for each row execute function control.touch_updated_at();

drop trigger if exists trg_holdings_review_touch_updated_at on review.shareholder_holdings_daily;
create trigger trg_holdings_review_touch_updated_at
before update on review.shareholder_holdings_daily
for each row execute function control.touch_updated_at();

grant usage on schema control, raw, staging, intermediate, marts, review, audit, export, observability, lineage, workflow, query_api, calc
    to pipeline_svc, ui_svc, dashboard_ro_svc;

grant select, insert, update, delete on all tables in schema control, raw, review, audit, export, observability, lineage, workflow
    to pipeline_svc;

grant select on all tables in schema control, staging, intermediate, marts, review, audit, export, observability, lineage, workflow
    to dashboard_ro_svc;

grant select on all tables in schema control, review, audit, export, observability, lineage, workflow
    to ui_svc;

grant insert, update on review.security_master_daily, review.shareholder_holdings_daily to ui_svc;
grant insert on audit.change_log, audit.workflow_events to ui_svc;
