from __future__ import annotations

from fastapi import APIRouter, Depends

from dagflow_api.dependencies import get_repository
from dagflow_api.repository import DagflowRepository

router = APIRouter(prefix="/queries", tags=["queries"])


@router.get("/security/{security_review_row_id}/shareholder-breakdown")
def get_security_shareholder_breakdown(
    security_review_row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_security_shareholder_breakdown(security_review_row_id)


@router.get("/security/{security_review_row_id}/holder-concentration")
def get_security_holder_concentration(
    security_review_row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_security_holder_concentration(security_review_row_id)


@router.get("/security/{security_review_row_id}/holder-approval-mix")
def get_security_holder_approval_mix(
    security_review_row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_security_holder_approval_mix(security_review_row_id)


@router.get("/security/{security_review_row_id}/history")
def get_security_history(
    security_review_row_id: int,
    repository: DagflowRepository = Depends(get_repository),
) -> list[dict[str, object]]:
    return repository.get_security_history(security_review_row_id)


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
