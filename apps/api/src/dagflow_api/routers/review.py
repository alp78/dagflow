from __future__ import annotations

from datetime import date
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository
from dagflow_api.schemas import CellEditRequest, RecalcRequest, ReviewRowsResponse, RowEditRequest

router = APIRouter(prefix="/review", tags=["review"])


@router.get("/{dataset_code}/rows", response_model=ReviewRowsResponse)
def list_review_rows(
    dataset_code: str,
    run_id: UUID | None = Query(default=None),
    business_date: date | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    repository: DagflowRepository = Depends(get_repository),
) -> ReviewRowsResponse:
    review_table = repository.resolve_review_table(dataset_code)
    rows = repository.list_review_rows(dataset_code, run_id, business_date, limit)
    return ReviewRowsResponse(dataset_code=dataset_code, review_table=review_table, rows=rows)


@router.post("/cell-edit")
def apply_cell_edit(
    request: CellEditRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.apply_cell_edit(request.model_dump())


@router.post("/row-edit")
def apply_row_edit(
    request: RowEditRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.apply_row_edit(request.model_dump())


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


@router.post("/recalculate/security")
def recalc_security(
    request: RecalcRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.recalc_security(request.row_id, request.changed_by)


@router.post("/recalculate/holding")
def recalc_holding(
    request: RecalcRequest,
    repository: DagflowRepository = Depends(get_repository),
) -> dict[str, object]:
    return repository.recalc_holding(request.row_id, request.changed_by)
