from __future__ import annotations

from fastapi import APIRouter, Depends

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository

router = APIRouter(prefix="/lineage", tags=["lineage"])


@router.get("/{row_hash}")
def get_row_lineage(
    row_hash: str,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_lineage_trace(row_hash)
