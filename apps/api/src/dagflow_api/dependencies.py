from __future__ import annotations

from fastapi import Request

from dagflow_api.repository import DagflowRepository


def get_repository(request: Request) -> DagflowRepository:
    return DagflowRepository(request.app.state.database)
