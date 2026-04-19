from __future__ import annotations

from dagflow_source_adapters.base import SourceAdapter

_ADAPTERS: dict[str, SourceAdapter] = {}


def register_adapter(name: str, adapter: SourceAdapter) -> None:
    _ADAPTERS[name] = adapter


def get_adapters() -> dict[str, SourceAdapter]:
    return _ADAPTERS.copy()


def get_adapter(adapter_type: str) -> SourceAdapter:
    if adapter_type not in _ADAPTERS:
        raise KeyError(
            f"Adapter '{adapter_type}' not registered. "
            "Ensure the workflow package is imported before calling get_adapter()."
        )
    return _ADAPTERS[adapter_type]
