-- Merge security_master and shareholder_holdings into one unified pipeline.

-- 1. Clean up old pipeline state and review state first (FK deps)
DELETE FROM workflow.dataset_review_state WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
DELETE FROM control.pipeline_state WHERE pipeline_code IN ('security_master', 'shareholder_holdings');

-- 2. Update all data tables that reference old pipeline_codes
UPDATE control.source_file_registry SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
UPDATE control.export_bundle_registry SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
UPDATE observability.pipeline_runs SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
UPDATE observability.pipeline_failures SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
UPDATE observability.data_quality_results SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
UPDATE audit.change_log SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
UPDATE audit.workflow_events SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
UPDATE audit.review_data_issues SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
UPDATE lineage.row_lineage_edges SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
UPDATE lineage.entity_lineage_summary SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');
UPDATE lineage.export_file_lineage SET pipeline_code = 'security_shareholder' WHERE pipeline_code IN ('security_master', 'shareholder_holdings');

-- 3. Remove old pipeline registry entries
DELETE FROM control.pipeline_registry WHERE pipeline_code IN ('security_master', 'shareholder_holdings');

-- 3b. Ensure dataset_registry entries exist (may not if running before seeds)
INSERT INTO control.dataset_registry (dataset_code, dataset_name, description, review_required, review_table_name, export_table_name, export_contract_code, calc_package_code, approval_workflow_code)
VALUES
    ('security_master', 'Security Master', 'Curated security reference dataset.', true, 'review.security_master_daily', 'export.security_master_final', 'security_master_export_v1', 'security_master_calc_v1', 'dataset_approval_v1'),
    ('shareholder_holdings', 'Shareholder Holdings', 'SEC 13F shareholder holdings dataset.', true, 'review.shareholder_holdings_daily', 'export.shareholder_holdings_final', 'shareholder_holdings_export_v1', 'shareholder_holdings_calc_v1', 'dataset_approval_v1')
ON CONFLICT (dataset_code) DO NOTHING;

-- 4. Insert the unified pipeline
INSERT INTO control.pipeline_registry (
    pipeline_code, pipeline_name, dataset_code, source_type, adapter_type,
    is_enabled, is_schedulable, default_schedule, review_required,
    review_table_name, export_contract_code, calc_package_code,
    approval_workflow_code, storage_prefix, owner_team
) VALUES (
    'security_shareholder',
    'Securities & Shareholder Holdings',
    'security_master',
    'sec_combined',
    'sec_combined',
    true, true, '0 6 * * 1-5', true,
    'review.security_master_daily',
    'security_shareholder_export_v1',
    'security_shareholder_calc_v1',
    'dataset_approval_v1',
    'security-shareholder/',
    'financial-data-platform'
);

-- 5. Add unified pipeline_state row
INSERT INTO control.pipeline_state (pipeline_code, dataset_code, approval_state, last_transition_by)
VALUES ('security_shareholder', 'security_master', 'pending_review', 'migration');
