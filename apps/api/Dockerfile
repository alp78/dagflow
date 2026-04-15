FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

WORKDIR /workspace

RUN pip install --no-cache-dir uv

COPY pyproject.toml README.md ./
COPY apps/api/pyproject.toml apps/api/pyproject.toml
COPY packages/pipeline-framework/pyproject.toml packages/pipeline-framework/pyproject.toml
COPY packages/source-adapters/pyproject.toml packages/source-adapters/pyproject.toml
COPY . .

RUN uv sync --package dagflow-api --no-dev

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "dagflow_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
