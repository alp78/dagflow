FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

WORKDIR /workspace

RUN pip install --no-cache-dir uv \
    && mkdir -p /opt/dagster/dagster_home

COPY pyproject.toml README.md ./
COPY apps/dagster/pyproject.toml apps/dagster/pyproject.toml
COPY packages/pipeline-framework/pyproject.toml packages/pipeline-framework/pyproject.toml
COPY packages/source-adapters/pyproject.toml packages/source-adapters/pyproject.toml
COPY . .

RUN uv sync --package dagflow-dagster --no-dev \
    && cp apps/dagster/dagster.yaml /opt/dagster/dagster_home/dagster.yaml

EXPOSE 3000 4000
