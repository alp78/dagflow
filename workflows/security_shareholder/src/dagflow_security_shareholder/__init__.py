from __future__ import annotations

from typing import cast

from dagflow_source_adapters.base import SourceAdapter
from dagflow_source_adapters.registry import register_adapter
from dagflow_security_shareholder.adapters.sec import SecJsonAdapter
from dagflow_security_shareholder.adapters.thirteenf import Sec13FBulkAdapter

register_adapter("sec_json", cast(SourceAdapter, SecJsonAdapter()))
register_adapter("sec_13f", cast(SourceAdapter, Sec13FBulkAdapter()))
