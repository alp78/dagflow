from __future__ import annotations

from fastapi import APIRouter, Depends

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository
from dagflow_api.schemas import PipelineToggleRequest, PipelineView

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.get("", response_model=list[PipelineView])
def list_pipelines(repository: DagflowRepository = Depends(get_repository)) -> list[PipelineView]:
    return repository.list_pipelines()


@router.post("/{pipeline_code}/activate")
def activate_pipeline(
    pipeline_code: str,
    request: PipelineToggleRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.set_pipeline_enabled(pipeline_code, True, request.changed_by)


@router.post("/{pipeline_code}/deactivate")
def deactivate_pipeline(
    pipeline_code: str,
    request: PipelineToggleRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.set_pipeline_enabled(pipeline_code, False, request.changed_by)
