from __future__ import annotations

from uuid import UUID

from dagflow_dagster.runner import (
    launch_pipeline_for_date,
    launch_workspace_export,
    reset_workbench,
    run_pipeline_for_date,
)
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository
from dagflow_api.schemas import (
    DemoResetRequest,
    LoadDateLaunchResponse,
    LoadDateRequest,
    PipelineExecutionStatusResponse,
    WorkbenchResetRequest,
    WorkflowActionRequest,
    WorkflowValidationResponse,
)

router = APIRouter(prefix="/workflow", tags=["workflow"])


@router.post("/validate", response_model=WorkflowValidationResponse)
def validate_dataset(
    request: WorkflowActionRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    try:
        workflow_payload = repository.call_workflow_action(
            "validate_dataset", request.model_dump()
        )
    except PermissionError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    if request.dataset_code == "security_master":
        return {
            "workflow": workflow_payload,
            "export_launch": None,
        }
    try:
        export_launch = launch_workspace_export(
            request.pipeline_code,
            request.business_date,
        )
    except RuntimeError as error:
        return {
            "workflow": workflow_payload,
            "export_error": str(error),
        }
    return {
        "workflow": workflow_payload,
        "export_launch": export_launch,
    }


@router.post("/load-date")
def load_pipeline_for_date(request: LoadDateRequest) -> dict[str, object]:
    try:
        return run_pipeline_for_date(request.pipeline_code, request.business_date)
    except RuntimeError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post("/load-date/launch", response_model=LoadDateLaunchResponse)
def launch_pipeline_load_for_date(request: LoadDateRequest) -> dict[str, object]:
    try:
        return launch_pipeline_for_date(request.pipeline_code, request.business_date)
    except RuntimeError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.get("/execution-status", response_model=PipelineExecutionStatusResponse)
def get_pipeline_execution_status(
    run_ids: str,
    relations: str | None = None,
    repository: DagflowRepository = Depends(get_repository),
) -> PipelineExecutionStatusResponse:
    parsed_run_ids = [UUID(item) for item in run_ids.split(",") if item]
    relation_map: dict[UUID, str] = {}
    if relations:
        for relation_entry in relations.split(","):
            if not relation_entry or ":" not in relation_entry:
                continue
            run_id_raw, relation = relation_entry.split(":", 1)
            relation_map[UUID(run_id_raw)] = relation
    return repository.get_pipeline_execution_status(parsed_run_ids, relation_map)


@router.get("/export-artifacts/{run_id}/{artifact_name:path}")
def open_export_artifact(
    run_id: UUID,
    artifact_name: str,
    repository: DagflowRepository = Depends(get_repository),
) -> FileResponse:
    try:
        artifact_path = repository.get_export_artifact_path(run_id, artifact_name)
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except FileNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    return FileResponse(path=artifact_path, filename=artifact_path.name)


@router.post("/reset-workbench")
def reset_reviewer_workbench(request: WorkbenchResetRequest) -> dict[str, object]:
    try:
        return reset_workbench(actor=request.actor, notes=request.notes)
    except RuntimeError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post("/reset-demo")
def reset_demo_runs(
    request: DemoResetRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.reset_demo_runs(request.actor, request.notes)
