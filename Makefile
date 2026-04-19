ifneq (,$(wildcard .env))
include .env
export
endif

COMPOSE ?= docker compose
POSTGRES_DSN ?= postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)

UV := UV_PROJECT_ENVIRONMENT=.dagflow uv
WORKFLOW_SRC = $(shell UV_PROJECT_ENVIRONMENT=.dagflow uv run python -c "import json; c=json.load(open('config.json')); print(' '.join(w['path']+'/src' for w in c['workflows']))")

.PHONY: up down logs ps db-init db-migrate seed test lint typecheck format py-sync ui-install sync-config

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
	$(UV) run python ops/scripts/manage_db.py migrate --dsn "$(POSTGRES_DSN)"

seed:
	$(UV) run python ops/scripts/manage_db.py seed --dsn "$(POSTGRES_DSN)"

py-sync:
	$(UV) sync --all-packages --dev

ui-install:
	pnpm install

lint:
	$(UV) run ruff check .
	$(UV) run mypy apps/api/src apps/dagster/src packages/pipeline-framework/src packages/source-adapters/src $(WORKFLOW_SRC)
	pnpm lint

typecheck:
	$(UV) run mypy apps/api/src apps/dagster/src packages/pipeline-framework/src packages/source-adapters/src $(WORKFLOW_SRC)
	pnpm typecheck

test:
	$(UV) run pytest
	pnpm test

format:
	$(UV) run ruff format .
	pnpm format

sync-config:
	$(UV) run python ops/scripts/sync_config.py
