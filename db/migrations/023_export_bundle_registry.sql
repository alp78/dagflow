create table if not exists control.export_bundle_registry (
    export_bundle_id bigint generated always as identity primary key,
    pipeline_code text not null,
    dataset_code text not null,
    run_id uuid not null unique,
    business_date date not null,
    baseline_file_path text not null,
    reviewed_file_path text not null,
    audit_file_path text not null,
    manifest_file_path text not null,
    metadata jsonb not null default '{}'::jsonb,
    exported_at timestamptz not null default timezone('utc', now()),
    updated_at timestamptz not null default timezone('utc', now())
);

create index if not exists idx_export_bundle_registry_pipeline_date
    on control.export_bundle_registry (pipeline_code, business_date desc);

drop trigger if exists trg_export_bundle_registry_touch_updated_at on control.export_bundle_registry;
create trigger trg_export_bundle_registry_touch_updated_at
before update on control.export_bundle_registry
for each row execute function control.touch_updated_at();

grant select, insert, update, delete on control.export_bundle_registry
    to pipeline_svc;

grant select on control.export_bundle_registry
    to dashboard_ro_svc, ui_svc;
