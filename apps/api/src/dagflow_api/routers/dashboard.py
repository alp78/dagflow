from __future__ import annotations

from fastapi import APIRouter, Depends

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository
from dagflow_api.schemas import DashboardOverview, DashboardSnapshot

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/overview", response_model=DashboardOverview)
def get_dashboard_overview(
    repository: DagflowRepository = Depends(get_repository),
) -> DashboardOverview:
    return repository.get_dashboard_overview()


@router.get("/snapshot", response_model=DashboardSnapshot)
def get_dashboard_snapshot(
    repository: DagflowRepository = Depends(get_repository),
) -> DashboardSnapshot:
    return repository.get_dashboard_snapshot()


@router.get("/securities")
def get_security_dimension(
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.list_security_dimension()
