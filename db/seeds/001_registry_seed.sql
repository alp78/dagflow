insert into control.dataset_registry (
    dataset_code, dataset_name, description, review_required,
    review_table_name, export_table_name, export_contract_code,
    calc_package_code, approval_workflow_code
)
values
    ('security_master', 'Security Master',
     'Curated security and issuer reference dataset sourced from SEC APIs.',
     true, 'review.security_master_daily', 'export.security_master_final',
     'security_master_export_v1', 'security_master_calc_v1', 'dataset_approval_v1'),
    ('shareholder_holdings', 'Shareholder Holdings',
     'SEC 13F-based shareholder holdings dataset joined to security master.',
     true, 'review.shareholder_holdings_daily', 'export.shareholder_holdings_final',
     'shareholder_holdings_export_v1', 'shareholder_holdings_calc_v1', 'dataset_approval_v1')
on conflict (dataset_code) do nothing;

insert into control.pipeline_registry (
    pipeline_code, pipeline_name, dataset_code, source_type, adapter_type,
    is_enabled, is_schedulable, default_schedule, review_required,
    review_table_name, export_contract_code, calc_package_code,
    approval_workflow_code, storage_prefix, owner_team
)
values (
    'security_shareholder',
    'Securities & Shareholder Holdings',
    'security_master',
    'sec_combined', 'sec_combined',
    true, true, '0 6 * * 1-5', true,
    'review.security_master_daily',
    'security_shareholder_export_v1',
    'security_shareholder_calc_v1',
    'dataset_approval_v1',
    'security-shareholder/',
    'financial-data-platform'
)
on conflict (pipeline_code) do nothing;

insert into control.pipeline_state (
    pipeline_code, dataset_code, is_active, approval_state, last_transition_by
)
values ('security_shareholder', 'security_master', true, 'pending_review', 'seed')
on conflict (pipeline_code) do nothing;
