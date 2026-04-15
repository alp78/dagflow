from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository

router = APIRouter(prefix="/failures", tags=["failures"])


@router.get("/runs/{run_id}")
def get_run_failure_rows(
    run_id: UUID,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_failure_rows(run_id)
