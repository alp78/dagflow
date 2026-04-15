from dagflow_dagster.definitions import defs


def test_definitions_load_assets() -> None:
    assert defs is not None
