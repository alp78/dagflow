insert into control.dataset_registry (
    dataset_code,
    dataset_name,
    description,
    review_required,
    review_table_name,
    export_table_name,
    export_contract_code,
    calc_package_code,
    approval_workflow_code
)
values
    (
        'security_master',
        'Security Master',
        'Curated security and issuer reference dataset sourced from SEC APIs.',
        true,
        'review.security_master_daily',
        'export.security_master_final',
        'security_master_export_v1',
        'security_master_calc_v1',
        'dataset_approval_v1'
    ),
    (
        'shareholder_holdings',
        'Shareholder Holdings',
        'SEC 13F-based shareholder holdings dataset joined to security master.',
        true,
        'review.shareholder_holdings_daily',
        'export.shareholder_holdings_final',
        'shareholder_holdings_export_v1',
        'shareholder_holdings_calc_v1',
        'dataset_approval_v1'
    )
on conflict (dataset_code) do update
set dataset_name = excluded.dataset_name,
    description = excluded.description,
    review_required = excluded.review_required,
    review_table_name = excluded.review_table_name,
    export_table_name = excluded.export_table_name,
    export_contract_code = excluded.export_contract_code,
    calc_package_code = excluded.calc_package_code,
    approval_workflow_code = excluded.approval_workflow_code,
    updated_at = timezone('utc', now());

insert into control.pipeline_registry (
    pipeline_code,
    pipeline_name,
    dataset_code,
    source_type,
    adapter_type,
    is_enabled,
    is_schedulable,
    default_schedule,
    review_required,
    review_table_name,
    export_contract_code,
    calc_package_code,
    approval_workflow_code,
    storage_prefix,
    owner_team
)
values
    (
        'security_master',
        'Security Master Pipeline',
        'security_master',
        'sec_json_api',
        'sec_json',
        true,
        true,
        '0 6 * * 1-5',
        true,
        'review.security_master_daily',
        'security_master_export_v1',
        'security_master_calc_v1',
        'dataset_approval_v1',
        'security-master/',
        'financial-data-platform'
    ),
    (
        'shareholder_holdings',
        'Shareholder Holdings Pipeline',
        'shareholder_holdings',
        'sec_13f_bulk',
        'sec_13f',
        true,
        true,
        '0 8 * * 1-5',
        true,
        'review.shareholder_holdings_daily',
        'shareholder_holdings_export_v1',
        'shareholder_holdings_calc_v1',
        'dataset_approval_v1',
        'shareholder-holdings/',
        'financial-data-platform'
    )
on conflict (pipeline_code) do update
set pipeline_name = excluded.pipeline_name,
    dataset_code = excluded.dataset_code,
    source_type = excluded.source_type,
    adapter_type = excluded.adapter_type,
    is_enabled = excluded.is_enabled,
    is_schedulable = excluded.is_schedulable,
    default_schedule = excluded.default_schedule,
    review_required = excluded.review_required,
    review_table_name = excluded.review_table_name,
    export_contract_code = excluded.export_contract_code,
    calc_package_code = excluded.calc_package_code,
    approval_workflow_code = excluded.approval_workflow_code,
    storage_prefix = excluded.storage_prefix,
    owner_team = excluded.owner_team,
    updated_at = timezone('utc', now());

insert into control.pipeline_state (
    pipeline_code,
    dataset_code,
    is_active,
    approval_state,
    last_transition_by
)
values
    ('security_master', 'security_master', true, 'pending_review', 'seed'),
    ('shareholder_holdings', 'shareholder_holdings', true, 'pending_review', 'seed')
on conflict (pipeline_code) do update
set dataset_code = excluded.dataset_code,
    is_active = excluded.is_active,
    approval_state = excluded.approval_state,
    last_transition_by = excluded.last_transition_by,
    updated_at = timezone('utc', now());
