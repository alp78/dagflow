from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository
from dagflow_api.schemas import DashboardDatasetSnapshot

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/current/{dataset_code}", response_model=DashboardDatasetSnapshot)
def get_dashboard_snapshot(
    dataset_code: Literal["security_master", "shareholder_holdings"],
    repository: DagflowRepository = Depends(get_repository),
) -> DashboardDatasetSnapshot:
    return repository.get_dashboard_dataset_snapshot(dataset_code)
