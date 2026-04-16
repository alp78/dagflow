from dagflow_dagster.dbt_topology import (
    DagflowDbtTranslator,
    group_for_resource_path,
    pipeline_for_resource,
)


def test_group_for_resource_path_maps_layers() -> None:
    assert (
        group_for_resource_path(
            "models/staging/security_master/stg_sec_company_tickers.sql"
        )
        == "silver"
    )
    assert (
        group_for_resource_path("models/intermediate/shareholder_holdings/int_holding_with_security.sql")
        == "silver"
    )
    assert group_for_resource_path("models/marts/security_master/dim_security.sql") == "gold"
    assert (
        group_for_resource_path("models/exports/shareholder_holdings/shareholder_holdings_final.sql")
        == "gold"
    )


def test_pipeline_for_resource_prefers_tags_and_path() -> None:
    assert (
        pipeline_for_resource({"tags": ["security_master"], "original_file_path": ""})
        == "security_master"
    )
    assert (
        pipeline_for_resource({"tags": ["shareholder_holdings"], "original_file_path": ""})
        == "shareholder_holdings"
    )
    assert (
        pipeline_for_resource(
            {
                "tags": [],
                "original_file_path": (
                    "models/intermediate/shareholder_holdings/"
                    "int_holding_with_security.sql"
                ),
            }
        )
        == "shareholder_holdings"
    )


def test_translator_prefixes_model_asset_keys_by_pipeline() -> None:
    translator = DagflowDbtTranslator()

    security_key = translator.get_asset_key(
        {
            "name": "dim_security",
            "resource_type": "model",
            "tags": ["security_master"],
            "original_file_path": "models/marts/security_master/dim_security.sql",
        }
    )
    holdings_key = translator.get_asset_key(
        {
            "name": "fact_shareholder_holding",
            "resource_type": "model",
            "tags": ["shareholder_holdings"],
            "original_file_path": "models/marts/shareholder_holdings/fact_shareholder_holding.sql",
        }
    )

    assert security_key.path == ["security_master__dim_security"]
    assert holdings_key.path == ["shareholder_holdings__fact_shareholder_holding"]
