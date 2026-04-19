from __future__ import annotations

from datetime import date
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository
from dagflow_api.schemas import (
    AvailableSourceDate,
    CellEditRequest,
    RecalcRequest,
    ReviewRowsResponse,
    ReviewSnapshotSummary,
    RowEditRequest,
)

router = APIRouter(prefix="/review", tags=["review"])


@router.get("/{dataset_code}/rows", response_model=ReviewRowsResponse)
def list_review_rows(
    dataset_code: str,
    run_id: UUID | None = Query(default=None),
    business_date: date | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=5000),
    repository: DagflowRepository = Depends(get_repository),
) -> ReviewRowsResponse:
    review_table = repository.resolve_review_table(dataset_code)
    rows = repository.list_review_rows(dataset_code, run_id, business_date, limit)
    return ReviewRowsResponse(dataset_code=dataset_code, review_table=review_table, rows=rows)


@router.get("/{dataset_code}/snapshots", response_model=list[ReviewSnapshotSummary])
def list_review_snapshots(
    dataset_code: str,
    limit: int = Query(default=5000, ge=1, le=5000),
    repository: DagflowRepository = Depends(get_repository),
) -> list[ReviewSnapshotSummary]:
    return repository.list_review_snapshots(dataset_code, limit)


@router.get("/{dataset_code}/available-dates", response_model=list[AvailableSourceDate])
def list_available_source_dates(
    dataset_code: str,
    repository: DagflowRepository = Depends(get_repository),
) -> list[AvailableSourceDate]:
    return [
        AvailableSourceDate.model_validate(row)
        for row in repository.list_available_source_dates(dataset_code)
    ]


@router.post("/cell-edit")
def apply_cell_edit(
    request: CellEditRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    try:
        return repository.apply_cell_edit(request.model_dump())
    except PermissionError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post("/row-edit")
def apply_row_edit(
    request: RowEditRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    try:
        return repository.apply_row_edit(request.model_dump())
    except PermissionError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.get("/edited-cells")
def get_edited_cells(
    review_table: str,
    row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.get_edited_cells(review_table, row_id)


@router.get("/row-diff")
def get_row_diff(
    review_table: str,
    row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.get_row_diff(review_table, row_id)


@router.get("/row-meta")
def get_row_meta(
    review_table: str,
    row_id: int,
    row_hash: str | None = Query(default=None),
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    diff_result = repository.get_row_diff(review_table, row_id)
    lineage_result = repository.get_lineage_trace(row_hash) if row_hash is not None else []
    return {"diff": diff_result, "lineage": lineage_result}


@router.post("/recalculate/security")
def recalc_security(
    request: RecalcRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    try:
        return repository.recalc_security(request.row_id, request.changed_by)
    except PermissionError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post("/recalculate/holding")
def recalc_holding(
    request: RecalcRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    try:
        return repository.recalc_holding(request.row_id, request.changed_by)
    except PermissionError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
