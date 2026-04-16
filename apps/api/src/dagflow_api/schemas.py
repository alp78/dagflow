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


class DashboardOverview(BaseModel):
    pipeline_count: int
    enabled_pipeline_count: int
    pending_reviews: int
    failure_count: int
    recent_runs: list[dict[str, Any]] = Field(default_factory=list)


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


class DashboardSecurityLeader(BaseModel):
    ticker: str
    issuer_name: str
    exchange: str
    materiality_score: float
    investable_shares: float
    holder_count: int
    total_market_value: float


class DashboardFilerLeader(BaseModel):
    filer_name: str
    position_count: int
    distinct_securities: int
    total_market_value: float


class DashboardSnapshot(BaseModel):
    overview: DashboardOverview
    latest_business_date: date | None = None
    security_run_id: UUID | None = None
    holdings_run_id: UUID | None = None
    security_metrics: list[DashboardMetric] = Field(default_factory=list)
    holdings_metrics: list[DashboardMetric] = Field(default_factory=list)
    exchange_distribution: list[DashboardChartPoint] = Field(default_factory=list)
    materiality_histogram: list[DashboardChartPoint] = Field(default_factory=list)
    security_approval_distribution: list[DashboardChartPoint] = Field(default_factory=list)
    holdings_approval_distribution: list[DashboardChartPoint] = Field(default_factory=list)
    holders_per_security_histogram: list[DashboardChartPoint] = Field(default_factory=list)
    portfolio_weight_histogram: list[DashboardChartPoint] = Field(default_factory=list)
    top_materiality_securities: list[DashboardSecurityLeader] = Field(default_factory=list)
    top_securities_by_holders: list[DashboardSecurityLeader] = Field(default_factory=list)
    top_filers: list[DashboardFilerLeader] = Field(default_factory=list)
