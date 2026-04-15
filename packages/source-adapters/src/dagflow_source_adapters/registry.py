from __future__ import annotations

from typing import cast

from dagflow_source_adapters.base import SourceAdapter
from dagflow_source_adapters.sec import SecJsonAdapter
from dagflow_source_adapters.thirteenf import Sec13FBulkAdapter

_ADAPTERS: dict[str, SourceAdapter] = {
    "sec_json": cast(SourceAdapter, SecJsonAdapter()),
    "sec_13f": cast(SourceAdapter, Sec13FBulkAdapter()),
}


def get_adapters() -> dict[str, SourceAdapter]:
    return _ADAPTERS.copy()


def get_adapter(adapter_type: str) -> SourceAdapter:
    return _ADAPTERS[adapter_type]
