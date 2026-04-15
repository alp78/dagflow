from __future__ import annotations

from datetime import date
from typing import Any
from uuid import UUID

from dagflow_pipeline_framework.contracts import PipelineRegistration
from pydantic import BaseModel, Field


class PipelineView(PipelineRegistration):
    is_active: bool
    approval_state: str
    last_run_id: UUID | None = None
    last_business_date: date | None = None


class PipelineToggleRequest(BaseModel):
    changed_by: str = "ui"


class CellEditRequest(BaseModel):
    review_table: str
    row_id: int
    column_name: str
    new_value: str
    changed_by: str = "reviewer"
    change_reason: str | None = None


class RowEditRequest(BaseModel):
    review_table: str
    row_id: int
    updates: dict[str, str]
    changed_by: str = "reviewer"
    change_reason: str | None = None


class RecalcRequest(BaseModel):
    row_id: int
    changed_by: str = "reviewer"


class ReviewRowsResponse(BaseModel):
    dataset_code: str
    review_table: str
    rows: list[dict[str, Any]]


class WorkflowActionRequest(BaseModel):
    pipeline_code: str
    dataset_code: str
    run_id: UUID
    business_date: date
    actor: str = "reviewer"
    notes: str | None = None


class DashboardOverview(BaseModel):
    pipeline_count: int
    enabled_pipeline_count: int
    pending_reviews: int
    failure_count: int
    recent_runs: list[dict[str, Any]] = Field(default_factory=list)
