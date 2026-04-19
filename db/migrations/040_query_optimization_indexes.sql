-- raw tables: run_id is the primary filter for every pipeline run and teardown
create index if not exists idx_raw_tickers_run
    on raw.sec_company_tickers (run_id);

create index if not exists idx_raw_facts_run_fact
    on raw.sec_company_facts (run_id, fact_name);

create index if not exists idx_raw_holdings_run
    on raw.holdings_13f (run_id);

create index if not exists idx_raw_filers_run
    on raw.holdings_13f_filers (run_id, filer_cik, report_period desc);

-- review tables: approval_state filtering for export queries
create index if not exists idx_security_review_approval
    on review.security_master_daily (run_id, approval_state);

create index if not exists idx_holdings_review_approval
    on review.shareholder_holdings_daily (run_id, approval_state);

-- review tables: FK from holdings to security (rollup recalcs, dashboard breakdowns)
create index if not exists idx_holdings_security_fk
    on review.shareholder_holdings_daily (security_review_row_id);

-- review tables: publish_review_snapshot joins on (business_date, ticker)
create index if not exists idx_security_review_bdate_ticker
    on review.security_master_daily (business_date, ticker);

-- audit: edit summary queries filter on (pipeline_code, dataset_code, run_id)
create index if not exists idx_change_log_pipeline
    on audit.change_log (pipeline_code, dataset_code, run_id);

-- audit: workflow_events had zero indexes
create index if not exists idx_workflow_events_pipeline
    on audit.workflow_events (pipeline_code, dataset_code, created_at desc);

-- lineage: reverse-direction lookup for fn_row_lineage_trace OR clause
create index if not exists idx_lineage_to_row_hash
    on lineage.row_lineage_edges (to_row_hash);

-- lineage: cleanup/teardown filters by run_id
create index if not exists idx_lineage_edges_run
    on lineage.row_lineage_edges (run_id);
