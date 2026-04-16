from dagflow_dagster.definitions import defs


def test_definitions_load_assets() -> None:
    assert defs is not None
    asset_graph = defs.get_repository_def().asset_graph
    assert asset_graph.asset_check_keys
