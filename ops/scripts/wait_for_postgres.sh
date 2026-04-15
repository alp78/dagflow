#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-localhost}"
PORT="${2:-5432}"
USER_NAME="${3:-postgres}"
DATABASE_NAME="${4:-dagflow}"

until pg_isready -h "${HOST}" -p "${PORT}" -U "${USER_NAME}" -d "${DATABASE_NAME}" >/dev/null 2>&1; do
  echo "Waiting for PostgreSQL at ${HOST}:${PORT}..."
  sleep 2
done

echo "PostgreSQL is ready."
