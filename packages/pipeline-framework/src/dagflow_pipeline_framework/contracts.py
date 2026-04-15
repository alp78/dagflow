from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ReviewState(StrEnum):
    PENDING_REVIEW = "pending_review"
    IN_REVIEW = "in_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    REOPENED = "reopened"
    EXPORTED = "exported"


class PipelineRegistration(BaseModel):
    model_config = ConfigDict(extra="ignore")

    pipeline_code: str
    pipeline_name: str
    dataset_code: str
    source_type: str
    adapter_type: str
    is_enabled: bool
    is_schedulable: bool
    default_schedule: str
    review_required: bool
    review_table_name: str
    export_contract_code: str
    calc_package_code: str
    approval_workflow_code: str
    storage_prefix: str
    owner_team: str = "platform"


class PipelineRunContext(BaseModel):
    pipeline_code: str
    dataset_code: str
    run_id: UUID
    business_date: date


class FailureRow(BaseModel):
    source_record_id: str | None = None
    source_file_name: str | None = None
    source_file_row_number: int | None = None
    row_hash: str | None = None
    row_context: dict[str, Any] = Field(default_factory=dict)


class FailureCapture(BaseModel):
    pipeline_code: str
    dataset_code: str
    run_id: UUID | None = None
    step_name: str
    stage: str
    business_date: date | None = None
    source_context: dict[str, Any] = Field(default_factory=dict)
    error_class: str
    error_message: str
    diagnostic_metadata: dict[str, Any] = Field(default_factory=dict)
    failure_rows: list[FailureRow] = Field(default_factory=list)
