# Dagflow

Dagflow is a financial data operations platform for SEC reference data and 13F holdings review.
It lands regulated source data, normalizes it into canonical source artifacts, loads one shared
operational workspace into PostgreSQL for a selected business date, lets reviewers correct
factual inputs with immediate cross-dataset recalculation, and exports immutable delivery bundles
once review is complete.

This repository still contains some legacy migrations, scripts, and naming from the earlier
historized architecture. The active runtime used by the current API, React UI, and Dagster jobs is
the model described in this README.

## Current Runtime Model

The platform now follows these rules:

- Raw source files are kept on disk.
- Each landed source also gets a canonical Parquet copy and a `manifest.json`.
- Available business dates are discovered from landed source artifacts on disk, not from review
  history stored in PostgreSQL.
- PostgreSQL is an operational store for the currently loaded workspace, not the long-term history
  store for daily runs.
- Loading a date brings up both `security_master` and `shareholder_holdings` for the same business
  date in one shared live workspace.
- The workspace stays loaded after validation and export until an operator explicitly resets it.
- Another date cannot be loaded until the current workspace is reset.
- Historical retention is expected to live in landed source artifacts and exported bundles, not in
  operational review tables.

## Pipelines

Dagflow currently runs two linked pipelines:

- `security_master`
  - Sources: `sec_company_tickers`, `sec_company_facts`
  - Review table: `review.security_master_daily`
  - Export preview: `export.security_master_preview`
- `shareholder_holdings`
  - Sources: `holdings_13f`, `holdings_13f_filers`
  - Review table: `review.shareholder_holdings_daily`
  - Export preview: `export.shareholder_holdings_preview`

These pipelines are reviewed together because holdings edits affect security-level estimates, and
security edits affect holdings percentages.

## End-To-End Flow

1. Capture or register landed source files for a business date.
2. Persist three source artifacts for each source/date:
   - the landed raw CSV
   - a canonical Parquet file
   - a `manifest.json`
3. Discover ready dates from the landing folders and manifests.
4. Load a selected date into the shared operational workspace.
   - This loads both `security_master` and `shareholder_holdings` for the same date.
5. Review and edit the live workspace in the UI.
6. Approve the `security_master` stage when the master data review is complete.
7. Validate `shareholder_holdings` to launch final workspace export.
   - This export step writes bundles for both datasets.
8. Keep the workspace loaded for inspection until an operator clicks `Reset workbench`.

## Source Ingestion Contract

For every source and business date, Dagflow writes:

- the raw landed file under `source_landing/...`
- a canonical Parquet file for the same source/date
- a manifest file describing the landed artifacts

The canonical Parquet layer is the normalized source contract used for reloadable operational
ingestion. It carries common metadata columns such as:

- `_pipeline_code`
- `_dataset_code`
- `_source_name`
- `_business_date`
- `_extracted_at`
- `_record_id`
- `_row_hash`
- `_source_uri`
- `_schema_version`

The manifest includes:

- pipeline and dataset codes
- source name and business date
- raw and canonical formats
- checksums
- row count
- capture timestamp
- schema version
- availability status

The active runtime discovers available dates by scanning landing folders and manifests rather than
depending on database-persisted date history.

## Storage Model

### Filesystem

The filesystem is the durable boundary for source and export artifacts.

- Source landing root: raw CSVs, canonical Parquet, and manifests
- Export root: immutable dataset export bundles

### PostgreSQL

PostgreSQL is split conceptually into control-plane metadata and operational workspace state.

Important schemas:

- `control`
  - pipeline registry
  - pipeline state
  - source file registry
  - export bundle registry
- `raw`, `staging`, `intermediate`, `marts`
  - runtime ingestion and transform relations
- `review`
  - editable review tables for the active workspace
- `audit`
  - edit log and workflow events
- `observability`
  - pipeline runs, step runs, failures, and quality results
- `workflow`
  - run-level review state
- `query_api`
  - SQL rowsets used by the UI
- `export`
  - dbt export preview relations

Operational PostgreSQL data is intentionally short-lived. The authoritative long-term artifacts are
landed source files and generated export bundles.

## Operational Review Workspace

The review workbench is a shared live workspace across both datasets for one business date.

### What loads together

When a user loads a date:

- `security_master` is prepared and run
- `shareholder_holdings` is prepared and run
- the holdings rows are relinked to the current security review rows
- the UI receives a Dagster-like execution graph for both runs

Only one business date can occupy the workspace at a time. While a workspace is loaded:

- loading another date is blocked
- the workbench shows a reset requirement
- data remains visible after validation and export

### Reset behavior

`Reset workbench` is the explicit cleanup boundary.

It clears the active workspace for both pipelines, including:

- operational review rows
- pipeline state for the active workspace
- source registry rows that belong to the active operational workspace
- related operational metadata

It then vacuums and re-analyzes the workspace relations so the next load starts cleanly.

## Review And Editing Semantics

Dagflow currently supports factual input correction, not arbitrary free-form editing of all fields.

### Editable columns

Current editable inputs are:

- `security_master`
  - `reported_shares_outstanding` (`shares_outstanding_raw`)
- `shareholder_holdings`
  - `reported_shares_held` (`shares_held_raw`)

The same holdings field is also editable from the `Security Master` analysis view
`Shareholder breakdown`.

Fields such as estimated free-float, investability, derived share counts, and other calculated
outputs are intentionally not directly editable.

### Edit capture

Committed edits:

- update the underlying review row
- append structured detail to `edited_cells`
- increment `manual_edit_count`
- increment `row_version`
- recompute `current_row_hash`
- write a row to `audit.change_log`

The UI also exposes operational views for:

- `Bad data rows`
- `Edited cells`
- `Row diff`
- `Lineage trace`

### Recalculation cascade

Edits propagate immediately inside the live workspace.

If `reported_shares_outstanding` is edited on `security_master`, Dagflow recomputes:

- `estimated_free_float_pct`
- `estimated_investability_factor`
- `free_float_shares`
- `investable_shares`
- `review_materiality_score`
- dependent `holding_pct_of_outstanding` values in linked holdings rows

If `reported_shares_held` is edited on `shareholder_holdings`, Dagflow recomputes:

- `holding_pct_of_outstanding`
- `implied_price_per_share` (`derived_price_per_share`)
- linked security estimates derived from holdings evidence

`portfolio_weight` is intentionally value-based off reported market value and filer totals, so it
does not change on share-only edits.

## Review Stages And Validation

The workspace is shared and live, but validation still has two milestones:

- `Approve Security Master`
  - marks the security review stage complete
  - does not export files
  - does not wipe the workspace
- `Validate and export workspace`
  - launched from the `shareholder_holdings` side
  - requires the security stage to be approved
  - triggers export for both datasets in the loaded workspace

This preserves live bidirectional propagation while still making the security stage an explicit
review milestone.

## Export Bundles

Each dataset writes an immutable export bundle under `generated_exports/`.

The current bundle contract is:

- `baseline.csv`
  - pre-review dataset state for the run
- `reviewed.csv`
  - final approved dataset state for the run
- `audit_events.parquet`
  - row/column-level audit trail for the run
- `manifest.json`
  - bundle metadata, checksums, counts, and lineage summary

Bundles are registered in `control.export_bundle_registry` so the UI and API can open them after
export.

Example layout:

```text
generated_exports/
  security-master/
    2026-01-09/
      <run_id>/
        baseline.csv
        reviewed.csv
        audit_events.parquet
        manifest.json
```

## Dagster Execution Pattern

Dagster orchestrates both the loading path and the export path.

### Load path

The operational load follows this shape:

```text
source capture
  -> contract/raw load
  -> dbt transform build
  -> publish review snapshot
  -> pending_review
```

### Export path

After final validation:

```text
review approval
  -> dbt export build
  -> write export bundle
  -> finalize export
  -> register export artifacts
```

The UI polls `execution-status` and renders this as the mini Dagster-style pipeline monitor in the
review tab.

## User Interface

The React app exposes three main surfaces:

- `Pipeline`
  - control-plane view for pipeline state and orchestration context
- `Review / CRUD`
  - business-date picker driven by landed source dates
  - shared live review workspace
  - mini Dagster-style pipeline monitor
  - inline editing and commit flow
  - analysis rowsets
  - operational rowsets
  - validation and reset actions
- `Dashboard`
  - current-dataset-only metrics and charts
  - missing-value indicators
  - distribution and histogram views
  - shareholder distribution view for a selected security

### Analysis views

For `security_master`:

- `Shareholder breakdown`
- `Holder pricing`

For `shareholder_holdings`:

- `Peer holders`
- `Filer portfolio`
- `Weight bands`

### Operational views

- `Bad data rows`
- `Edited cells`
- `Row diff`
- `Lineage trace`

## Observability And Audit

Important runtime audit tables include:

- `audit.change_log`
  - who changed what, before/after values, reason, hashes
- `audit.workflow_events`
  - review and export lifecycle events
- `observability.pipeline_runs`
  - run-level status
- `observability.pipeline_step_runs`
  - step-by-step execution state used by the UI monitor
- `observability.data_quality_results`
  - dbt and other quality signals
- `observability.pipeline_failures`
  - pipeline-level failure capture
- `observability.pipeline_failure_rows`
  - row-level failure context where available

## Data Quality

Dagflow still uses dbt tests and warning-style integrity checks against the active run.

Representative checks include:

- uniqueness and not-null constraints on curated identifiers
- security share consistency and required/bounds checks
- holdings required/bounds checks
- weight-bounds checks for holdings

These checks write results into `observability.data_quality_results` and support the bad-data row
workflows in the UI.

## What Is Intentionally Not In The Active Runtime

The current operational model does not use PostgreSQL as the historical store for reviewer-facing
daily runs.

Specifically:

- review history is not the source of truth for date availability
- the operational UI does not center on historical dimension-history browsing
- the workspace is not wiped automatically immediately after export
- loading another day is not allowed until the operator resets the workbench

If you see legacy scripts or migrations referring to dbt snapshots, dimension history, or
historical backfill utilities, treat those as legacy or sidecar utilities rather than the primary
runtime contract described here.

## Local Validation

Useful validation commands:

```bash
UV_PROJECT_ENVIRONMENT=/tmp/dagflow-venv uv run pytest
UV_PROJECT_ENVIRONMENT=/tmp/dagflow-venv uv run ruff check .
pnpm --filter @dagflow/ui-react typecheck
pnpm --filter @dagflow/ui-react build
```

## Summary

Dagflow is currently a source-driven, shared-workspace review platform with:

- landed raw sources retained on disk
- canonical Parquet source artifacts and manifests
- one active operational workspace in PostgreSQL
- live cross-dataset recalculation during review
- explicit reset instead of automatic wipe
- immutable export bundles for downstream historization

That is the active architecture the codebase implements today.
