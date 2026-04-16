# Dagflow

Dagflow is a financial data operations platform for acquiring, governing, reviewing, and
publishing regulated market datasets. It manages the end-to-end lifecycle for security master and
shareholder holdings data, including source ingestion, identifier resolution, transformation,
analyst review, validation, audit capture, lineage, and governed export delivery.

The platform combines Dagster orchestration, dbt transformation models, PostgreSQL control-plane
state, FastAPI services, React-based operational tooling, and Terraform-managed infrastructure.
The result is a repeatable operating model for daily financial data workflows with explicit review
gates, traceable state transitions, and production-grade export controls.

## Product Capabilities

- Live SEC/EDGAR ingestion for issuer reference data and 13F holdings disclosures
- Identifier normalization and enrichment with OpenFIGI and Finnhub fallback resolution
- Control-table driven pipeline state, review workflow, and export handoff
- dbt-managed staging, intermediate, mart, snapshot, and export layers in PostgreSQL
- Reviewer operations for row-level edits, recalculation, approval-state management, and dataset validation
- Row-level audit history, failure capture, and lineage across raw, transformed, review, and export layers
- Automated validated export generation for downstream delivery contracts
- Historical run tracking and backfill support for operational continuity

## Managed Datasets

- `security_master`: issuer reference data derived from SEC company tickers and company facts
- `shareholder_holdings`: 13F filing data normalized into filer, holding, and export-ready views

## Operating Model

1. Source data is ingested into raw PostgreSQL landing tables.
2. dbt builds persisted transformation layers and history-aware snapshots.
3. Dagster publishes dataset-specific review snapshots for analyst workflows.
4. Reviewers inspect rows, apply controlled edits, trigger recalculation, and validate datasets.
5. Approved datasets are exported through governed delivery tables and file artifacts.
6. Operational metadata, lineage edges, and failure records remain available for support,
   compliance, and downstream troubleshooting.

## Repository Layout

- `apps/api`: FastAPI application exposing typed command and query endpoints for workflow, review,
  dashboard, and operational access
- `apps/dagster`: Dagster jobs, sensors, assets, resources, ingestion services, and dbt topology
- `apps/ui-react`: React and TypeScript operations console for pipeline monitoring, review, and
  dashboard analysis
- `packages/pipeline-framework`: shared Python contracts for pipeline registration and runtime
- `packages/source-adapters`: source adapter interfaces plus SEC and 13F implementations
- `packages/shared-types`: shared frontend contracts for API-backed dashboard, review, and workflow
  models
- `db`: SQL migrations, stored procedures, triggers, roles, and seed data for the control plane
- `dbt`: dbt project containing staging, intermediate, mart, snapshot, and export models
- `infra/terraform`: infrastructure modules and environment definitions for cloud deployment
- `ops`: operational scripts, including historical backfill utilities

## Development Environment

The repository includes a containerized development environment that mirrors the platform's core
service boundaries:

- `postgres`: primary operational datastore for raw, transformed, review, lineage, and workflow schemas
- `pgbouncer`: pooled database access layer for application and orchestration services
- `minio`: S3-compatible local object storage service
- `api`: FastAPI backend for workflow and query access
- `ui`: React operations console
- `dagster-webserver`: Dagster web interface
- `dagster-daemon`: Dagster sensors and background orchestration
- `dagster-user-code`: Dagster asset and job definitions

## Quick Start

1. Copy `.env.example` to `.env`, set a valid `EDGAR_IDENTITY`, and add optional values for
   `FINNHUB_API_KEY` and `OPENFIGI_API_KEY`.
2. Install local toolchains: Python 3.13, `uv`, Node 20+, and `pnpm`.
3. Sync Python dependencies with `uv sync --all-packages --dev`.
4. Install frontend dependencies with `pnpm install`.
5. Start the stack with `make up`.
6. Apply schema and seed control-plane data with `make db-init`.
7. Open:
   - UI: `http://localhost:3000`
   - API docs: `http://localhost:8000/docs`
   - Dagster: `http://localhost:3001`
   - MinIO console: `http://localhost:9001`

## Common Commands

- `make up`: build and start the containerized stack
- `make down`: stop the stack
- `make logs`: tail service logs
- `make db-migrate`: apply SQL migrations
- `make seed`: load pipeline registry and control-plane seed data
- `make lint`: run Python and frontend linting
- `make test`: run Python and frontend tests
