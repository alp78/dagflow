from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class SourceDescriptor(BaseModel):
    source_name: str
    landing_table: str
    source_url: str
    object_key: str
    business_date: date
    ingestion_mode: str
    expected_format: str
