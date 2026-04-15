# Dagflow

Dagflow is a local-first, production-shaped financial data curation platform POC built as a
container-first monorepo. The platform combines Dagster, dbt, PostgreSQL, FastAPI, React, and
Terraform to support controlled ingestion, review, approval, auditability, lineage, and export of
financial datasets.

## Architecture

- `apps/api`: FastAPI backend that exposes typed command/query endpoints.
- `apps/dagster`: Dagster control plane and user code definitions.
- `apps/ui-react`: React + TypeScript reviewer and operational UI.
- `packages/pipeline-framework`: Shared Python contracts for pipeline registration and runtime.
- `packages/source-adapters`: Source adapter protocol plus SEC/13F adapter implementations.
- `packages/shared-types`: Shared frontend types for review, pipeline, and dashboard views.
- `db`: SQL migrations, runtime functions, triggers, roles, and seed data.
- `dbt`: dbt project for persisted staging, intermediate, marts, and export transforms.
- `infra/terraform`: GCP module and environment skeletons.
- `ops`: Docker and developer helper scripts.

## Local Runtime

The local platform is designed for WSL2 + Docker Desktop and runs these containerized services:

- `postgres`
- `pgbouncer`
- `minio`
- `api`
- `ui`
- `dagster-webserver`
- `dagster-daemon`
- `dagster-user-code`

## Quick Start

1. Copy `.env.example` to `.env` and adjust values if needed.
2. Install local toolchains: Python 3.13, `uv`, Node 20+, and `pnpm`.
3. Sync Python dependencies with `uv sync --all-packages --dev`.
4. Install frontend dependencies with `pnpm install`.
5. Start the stack with `make up`.
6. Apply schema and seed data with `make db-init`.
7. Open:
   - UI: `http://localhost:3000`
   - API docs: `http://localhost:8000/docs`
   - Dagster: `http://localhost:3001`
   - MinIO console: `http://localhost:9001`

## Common Commands

- `make up`: Build and start the local stack.
- `make down`: Stop the stack.
- `make logs`: Tail container logs.
- `make db-migrate`: Apply SQL migrations.
- `make seed`: Load registry and sample raw data.
- `make lint`: Run Python and frontend linting.
- `make test`: Run Python and frontend tests.

## Implementation Priorities

The repository is organized around these backbone concerns:

- deterministic SQL migrations and runtime DB APIs
- persisted observability, failure capture, and lineage tables
- source adapter and pipeline registration contracts
- dbt-managed transformation layers
- Dagster orchestration that honors control-table state
- reviewer workflow through API-backed stored procedures
- deployable GCP infrastructure skeletons via Terraform
