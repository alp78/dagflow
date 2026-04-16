do
$$
begin
    if exists (
        select 1
        from information_schema.tables
        where table_schema = 'export'
          and table_name = 'security_master_final'
    ) and not exists (
        select 1
        from information_schema.columns
        where table_schema = 'export'
          and table_name = 'security_master_final'
          and column_name = 'exported_at'
    ) then
        if to_regclass('export.security_master_preview') is null then
            execute 'alter table export.security_master_final rename to security_master_preview';
        else
            execute 'drop table export.security_master_final';
        end if;
    end if;

    if exists (
        select 1
        from information_schema.tables
        where table_schema = 'export'
          and table_name = 'shareholder_holdings_final'
    ) and not exists (
        select 1
        from information_schema.columns
        where table_schema = 'export'
          and table_name = 'shareholder_holdings_final'
          and column_name = 'exported_at'
    ) then
        if to_regclass('export.shareholder_holdings_preview') is null then
            execute 'alter table export.shareholder_holdings_final rename to shareholder_holdings_preview';
        else
            execute 'drop table export.shareholder_holdings_final';
        end if;
    end if;
end;
$$;

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
