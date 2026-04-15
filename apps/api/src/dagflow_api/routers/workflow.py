from __future__ import annotations

from fastapi import APIRouter, Depends

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository
from dagflow_api.schemas import WorkflowActionRequest

router = APIRouter(prefix="/workflow", tags=["workflow"])


@router.post("/publish")
def publish_review_snapshot(
    request: WorkflowActionRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.call_workflow_action("publish_review_snapshot", request.model_dump())


@router.post("/approve")
def approve_dataset(
    request: WorkflowActionRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.call_workflow_action("approve_dataset", request.model_dump())


@router.post("/reject")
def reject_dataset(
    request: WorkflowActionRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.call_workflow_action("reject_dataset", request.model_dump())


@router.post("/reopen")
def reopen_dataset(
    request: WorkflowActionRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.call_workflow_action("reopen_dataset", request.model_dump())


@router.post("/finalize-export")
def finalize_export(
    request: WorkflowActionRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.call_workflow_action("finalize_export", request.model_dump())
