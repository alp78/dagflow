from __future__ import annotations

from datetime import date
from typing import Protocol

from dagflow_source_adapters.contracts import SourceDescriptor


class SourceAdapter(Protocol):
    adapter_type: str

    def build_sources(self, business_date: date) -> list[SourceDescriptor]: ...
