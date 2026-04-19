from __future__ import annotations

from uuid import UUID

from dagflow_dagster.runner import (
    launch_pipeline_for_date,
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
    WorkspaceApprovalRequest,
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
    return {
        "workflow": workflow_payload,
        "export_launch": None,
    }


@router.post("/validate-workspace")
def validate_full_workspace(
    request: WorkspaceApprovalRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    run_id = request.run_id
    if not run_id:
        state = repository.get_current_pipeline_run("security_shareholder")
        run_id = state["last_run_id"]
    try:
        for dataset_code in ("security_master", "shareholder_holdings"):
            result = repository.call_workflow_action(
                "validate_dataset",
                {
                    "pipeline_code": "security_shareholder",
                    "dataset_code": dataset_code,
                    "run_id": run_id,
                    "business_date": request.business_date,
                    "actor": request.actor,
                    "notes": request.notes or "Workspace approved from unified review",
                },
            )
    except PermissionError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return {"workflow": result, "export_launch": None}


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


@router.post("/capture-sources")
def capture_sources_for_date(
    request: LoadDateRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    from dagflow_dagster.resources import build_resources

    bdate = request.business_date
    resources = build_resources()
    control_plane = resources["control_plane"]
    results = []
    for source_name in ("sec_company_tickers", "sec_company_facts", "holdings_13f", "holdings_13f_filers"):
        try:
            record = control_plane.capture_source_file(source_name, bdate)
            results.append({"source_name": source_name, "status": "captured", "row_count": record.get("row_count", 0)})
        except Exception as err:
            results.append({"source_name": source_name, "status": "error", "error": str(err)})
    return {"business_date": request.business_date, "sources": results}


@router.post("/delete-source-files")
def delete_source_files(
    request: LoadDateRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    from dagflow_dagster.resources import build_resources

    resources = build_resources()
    control_plane = resources["control_plane"]
    result = control_plane.delete_source_files_for_date("security_shareholder", request.business_date)
    return result


@router.post("/reset-workbench")
def reset_reviewer_workbench(request: WorkbenchResetRequest) -> dict[str, object]:
    try:
        return reset_workbench(actor=request.actor, notes=request.notes)
    except RuntimeError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.get("/calendar/{calendar_code}")
def get_calendar_days(
    calendar_code: str,
    year: int | None = None,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    from datetime import date as d
    target_year = year or d.today().year
    return repository.get_calendar_days(calendar_code, target_year)


@router.post("/reset-demo")
def reset_demo_runs(
    request: DemoResetRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.reset_demo_runs(request.actor, request.notes)
