ifneq (,$(wildcard .env))
include .env
export
endif

COMPOSE ?= docker compose
POSTGRES_DSN ?= postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)

.PHONY: up down logs ps db-init db-migrate seed test lint typecheck format py-sync ui-install

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down --remove-orphans

logs:
	$(COMPOSE) logs -f --tail=200

ps:
	$(COMPOSE) ps

db-init: db-migrate seed

db-migrate:
	uv run python ops/scripts/manage_db.py migrate --dsn "$(POSTGRES_DSN)"

seed:
	uv run python ops/scripts/manage_db.py seed --dsn "$(POSTGRES_DSN)"

py-sync:
	uv sync --all-packages --dev

ui-install:
	pnpm install

lint:
	uv run ruff check .
	uv run mypy apps/api/src apps/dagster/src packages/pipeline-framework/src packages/source-adapters/src
	pnpm lint

typecheck:
	uv run mypy apps/api/src apps/dagster/src packages/pipeline-framework/src packages/source-adapters/src
	pnpm typecheck

test:
	uv run pytest
	pnpm test

format:
	uv run ruff format .
	pnpm format
