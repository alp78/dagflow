create table if not exists control.source_file_registry (
    source_file_id uuid primary key default gen_random_uuid(),
    pipeline_code text not null references control.pipeline_registry (pipeline_code),
    dataset_code text not null references control.dataset_registry (dataset_code),
    source_name text not null,
    business_date date not null,
    storage_backend text not null default 'filesystem'
        check (storage_backend in ('filesystem')),
    storage_path text not null,
    object_key text not null,
    source_url text not null,
    source_format text not null,
    capture_mode text not null,
    content_checksum text not null,
    row_count integer not null default 0,
    file_size_bytes bigint,
    metadata jsonb not null default '{}'::jsonb,
    captured_at timestamptz not null default timezone('utc', now()),
    unique (pipeline_code, source_name, business_date)
);

create index if not exists idx_source_file_registry_lookup
    on control.source_file_registry (pipeline_code, source_name, business_date desc);

create index if not exists idx_source_file_registry_dataset
    on control.source_file_registry (dataset_code, business_date desc);

alter table raw.sec_company_tickers
    add column if not exists source_file_id uuid references control.source_file_registry (source_file_id);

alter table raw.sec_company_facts
    add column if not exists source_file_id uuid references control.source_file_registry (source_file_id);

alter table raw.holdings_13f
    add column if not exists source_file_id uuid references control.source_file_registry (source_file_id);

alter table raw.holdings_13f_filers
    add column if not exists source_file_id uuid references control.source_file_registry (source_file_id);

create index if not exists idx_raw_sec_company_tickers_source_file
    on raw.sec_company_tickers (source_file_id, run_id);

create index if not exists idx_raw_sec_company_facts_source_file
    on raw.sec_company_facts (source_file_id, run_id);

create index if not exists idx_raw_holdings_13f_source_file
    on raw.holdings_13f (source_file_id, run_id);

create index if not exists idx_raw_holdings_13f_filers_source_file
    on raw.holdings_13f_filers (source_file_id, run_id);
