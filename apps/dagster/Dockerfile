FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

WORKDIR /workspace

RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends graphviz \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv \
    && mkdir -p /opt/dagster/dagster_home

COPY . .
RUN uv sync --all-packages --no-dev \
    && cp apps/dagster/dagster.yaml /opt/dagster/dagster_home/dagster.yaml

EXPOSE 3000 4000
