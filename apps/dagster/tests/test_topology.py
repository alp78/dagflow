from dagflow_security_shareholder.topology import (
    SecurityShareholderDbtTranslator,
    group_for_resource_path,
    pipeline_for_resource,
)


def test_group_for_resource_path_maps_layers() -> None:
    assert (
        group_for_resource_path(
            "staging/security_master/stg_sec_company_tickers.sql"
        )
        == "warehouse_transform"
    )
    assert (
        group_for_resource_path(
            "intermediate/shareholder_holdings/int_holding_with_security.sql"
        )
        == "warehouse_transform"
    )
    assert (
        group_for_resource_path("marts/security_master/dim_security.sql")
        == "warehouse_transform"
    )
    assert (
        group_for_resource_path(
            "exports/shareholder_holdings/shareholder_holdings_final.sql"
        )
        == "validated_export"
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
                    "intermediate/shareholder_holdings/int_holding_with_security.sql"
                ),
            }
        )
        == "shareholder_holdings"
    )


def test_translator_prefixes_model_asset_keys_by_pipeline() -> None:
    translator = SecurityShareholderDbtTranslator()

    security_key = translator.get_asset_key(
        {
            "name": "dim_security",
            "resource_type": "model",
            "tags": ["security_master"],
            "original_file_path": "marts/security_master/dim_security.sql",
        }
    )
    holdings_key = translator.get_asset_key(
        {
            "name": "fact_shareholder_holding",
            "resource_type": "model",
            "tags": ["shareholder_holdings"],
            "original_file_path": "marts/shareholder_holdings/fact_shareholder_holding.sql",
        }
    )

    assert security_key.path == ["security_master__daily_security_master"]
    assert holdings_key.path == ["shareholder_holdings__daily_holdings"]


def test_translator_adds_business_context_to_assets() -> None:
    translator = SecurityShareholderDbtTranslator()

    resource_props = {
        "name": "int_security_attributes",
        "resource_type": "model",
        "tags": ["security_master"],
        "original_file_path": "intermediate/security_master/int_security_attributes.sql",
        "alias": "int_security_attributes",
        "config": {"materialized": "table"},
    }

    description = translator.get_description(resource_props)
    metadata = translator.get_metadata(resource_props)

    assert "Float metrics." in description
    assert "Creates the key business metrics" in description
    assert "This warehouse transform asset is built with dbt" in description
    assert metadata["dagflow_business_name"] == "Float metrics"
    assert metadata["dagflow_business_significance"].startswith(
        "Creates the key business metrics"
    )
