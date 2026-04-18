from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class SourceDescriptor(BaseModel):
    pipeline_code: str
    dataset_code: str
    source_name: str
    landing_table: str
    source_url: str
    object_key: str
    file_name: str
    business_date: date
    ingestion_mode: str
    expected_format: str
    canonical_object_key: str | None = None
    canonical_file_name: str | None = None
    canonical_format: str = "parquet"
    manifest_object_key: str | None = None
    manifest_file_name: str | None = None
    schema_version: str = "v1"
