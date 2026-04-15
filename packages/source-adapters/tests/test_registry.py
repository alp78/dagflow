from dagflow_source_adapters.registry import get_adapter


def test_known_adapter_is_registered() -> None:
    assert get_adapter("sec_json").adapter_type == "sec_json"
