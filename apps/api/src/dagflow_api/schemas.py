from __future__ import annotations

from datetime import date, datetime
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
    new_value: str | None = None
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


class ReviewSnapshotSummary(BaseModel):
    run_id: UUID
    business_date: date
    review_state: str
    row_count: int
    updated_at: datetime | None = None
    is_current: bool = False


class AvailableSourceFile(BaseModel):
    source_name: str
    source_file_id: str
    row_count: int
    captured_at: str
    raw_exists: bool
    manifest_exists: bool


class AvailableSourceDate(BaseModel):
    pipeline_code: str
    business_date: date
    available_source_count: int
    expected_source_count: int
    is_ready: bool
    missing_sources: list[str] = Field(default_factory=list)
    sources: list[AvailableSourceFile] = Field(default_factory=list)


class WorkflowActionRequest(BaseModel):
    pipeline_code: str
    dataset_code: str
    run_id: UUID
    business_date: date
    actor: str = "reviewer"
    notes: str | None = None


class DemoResetRequest(BaseModel):
    actor: str = "reviewer"
    notes: str | None = None


class WorkbenchResetRequest(BaseModel):
    actor: str = "reviewer"
    notes: str | None = None


class LoadDateRequest(BaseModel):
    pipeline_code: str
    dataset_code: str
    business_date: date
    actor: str = "reviewer"


class PipelineExecutionStep(BaseModel):
    step_key: str
    step_label: str
    step_group: str
    sort_order: int
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PipelineExecutionRun(BaseModel):
    pipeline_code: str
    dataset_code: str
    run_id: UUID
    business_date: date
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    relation: str = "primary"
    metadata: dict[str, Any] = Field(default_factory=dict)
    current_step_label: str | None = None
    completed_step_count: int = 0
    total_step_count: int = 0
    steps: list[PipelineExecutionStep] = Field(default_factory=list)


class PipelineExecutionStatusResponse(BaseModel):
    run_ids: list[UUID] = Field(default_factory=list)
    is_complete: bool
    runs: list[PipelineExecutionRun] = Field(default_factory=list)


class LoadDateLaunchResponse(BaseModel):
    requested_pipeline_code: str
    dataset_code: str
    business_date: date
    requested_run_id: UUID
    run_ids: list[UUID] = Field(default_factory=list)
    runs: list[PipelineExecutionRun] = Field(default_factory=list)
    already_running: bool = False


class WorkflowValidationResponse(BaseModel):
    workflow: dict[str, Any]
    export_launch: LoadDateLaunchResponse | None = None
    export_error: str | None = None


class DashboardMetric(BaseModel):
    label: str
    value: float
    display_value: str
    note: str | None = None


class DashboardChartPoint(BaseModel):
    label: str
    value: float
    display_value: str
    percentage: float | None = None
    color: str | None = None


class DashboardMissingField(BaseModel):
    field_name: str
    label: str
    missing_count: int
    present_count: int
    total_count: int
    missing_percentage: float
    completeness_percentage: float


class DashboardFocusSecurity(BaseModel):
    review_row_id: int
    ticker: str
    issuer_name: str
    holder_count: int


class DashboardDatasetSnapshot(BaseModel):
    dataset_code: str
    dataset_label: str
    business_date: date | None = None
    run_id: UUID | None = None
    metrics: list[DashboardMetric] = Field(default_factory=list)
    missing_fields: list[DashboardMissingField] = Field(default_factory=list)
    distribution_title: str
    distribution_subtitle: str
    distribution_points: list[DashboardChartPoint] = Field(default_factory=list)
    shares_histogram_title: str
    shares_histogram_subtitle: str
    shares_histogram_points: list[DashboardChartPoint] = Field(default_factory=list)
    value_histogram_title: str
    value_histogram_subtitle: str
    value_histogram_points: list[DashboardChartPoint] = Field(default_factory=list)
    focus_security_options: list[DashboardFocusSecurity] = Field(default_factory=list)
    default_focus_security_review_row_id: int | None = None
