from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository
from dagflow_api.schemas import DemoResetRequest, WorkflowActionRequest

router = APIRouter(prefix="/workflow", tags=["workflow"])


@router.post("/validate")
def validate_dataset(
    request: WorkflowActionRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    try:
        return repository.call_workflow_action("validate_dataset", request.model_dump())
    except PermissionError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post("/reset-demo")
def reset_demo_runs(
    request: DemoResetRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.reset_demo_runs(request.actor, request.notes)
