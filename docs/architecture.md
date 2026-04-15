# Dagflow Architecture Notes

## Runtime shape

- Dagster handles orchestration and control-plane concerns.
- dbt owns persisted transforms across `staging`, `intermediate`, `marts`, and `export`.
- PostgreSQL owns review/runtime logic through stored procedures and audit tables.
- FastAPI is the only browser-facing write surface.
- React hosts the reviewer shell, dashboard, and Dagster embedding.

## Control tables

- `control.pipeline_registry` stores pipeline registration metadata and adapter bindings.
- `control.dataset_registry` binds datasets to review/export contracts.
- `control.pipeline_state` stores enablement and top-level approval state.
- `workflow.dataset_review_state` stores run-aware dataset review state.

## Review flow

1. Land source records into `raw.*`.
2. Transform through dbt into persisted marts.
3. Publish reviewer-facing rows into `review.*`.
4. Reviewers edit via API-backed stored procedures.
5. Audit records persist to `audit.change_log`.
6. Approval state moves through `workflow.*`.
7. Post-review export models read from `review.*` into `export.*`.

## Failure and lineage

- `observability.pipeline_failures` and `observability.pipeline_failure_rows` persist queryable failures.
- `lineage.row_lineage_edges` tracks row-level movement across layers.
- API endpoints expose failure rows and lineage traces to the UI.
