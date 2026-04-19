FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

WORKDIR /workspace

RUN pip install --no-cache-dir uv

COPY . .
RUN uv sync --all-packages --no-dev

EXPOSE 8000

CMD ["uvicorn", "dagflow_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
