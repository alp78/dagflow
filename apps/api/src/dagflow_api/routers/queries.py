from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository

router = APIRouter(prefix="/queries", tags=["queries"])


@router.get("/review/{dataset_code}/{run_id}/bad-data-rows")
def get_review_data_issues(
    dataset_code: str,
    run_id: UUID,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_review_data_issues(dataset_code, run_id)


@router.get("/review/{dataset_code}/{run_id}/edited-cells")
def get_review_edited_cells(
    dataset_code: str,
    run_id: UUID,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_review_edited_cells(dataset_code, run_id)


@router.get("/security/{security_review_row_id}/shareholder-breakdown")
def get_security_shareholder_breakdown(
    security_review_row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_security_shareholder_breakdown(security_review_row_id)


@router.get("/security/{security_review_row_id}/holder-valuations")
def get_security_holder_valuations(
    security_review_row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_security_holder_valuations(security_review_row_id)


@router.get("/holding/{holding_review_row_id}/peer-holders")
def get_holding_peer_holders(
    holding_review_row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_holding_peer_holders(holding_review_row_id)


@router.get("/holding/{holding_review_row_id}/filer-portfolio")
def get_filer_portfolio_snapshot(
    holding_review_row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_filer_portfolio_snapshot(holding_review_row_id)


@router.get("/holding/{holding_review_row_id}/filer-weight-bands")
def get_filer_weight_bands(
    holding_review_row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_filer_weight_bands(holding_review_row_id)
