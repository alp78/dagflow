from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings

from dagflow_dagster.dbt_topology import (
    AssetNarrative,
    asset_description,
    asset_metadata,
    raw_asset_key,
)

# ── Manual asset narratives (4 unified pipeline steps) ─────────────────────────

MANUAL_ASSET_NARRATIVES: dict[str, AssetNarrative] = {
    "source_capture": AssetNarrative(
        key_name="security_shareholder__source_capture",
        title="Capture sources",
        purpose=(
            "Captures all four daily source files — SEC company tickers, SEC company "
            "facts, 13F holdings, and 13F filer roster — as immutable landing artifacts."
        ),
        significance=(
            "Anchors the entire pipeline run; every downstream load, transform, and "
            "review step depends on these captured files being present."
        ),
    ),
    "contract_load": AssetNarrative(
        key_name="security_shareholder__contract_load",
        title="Load to raw",
        purpose=(
            "Loads all four captured source files into their respective typed raw "
            "contract tables in Postgres."
        ),
        significance=(
            "Creates the replayable warehouse boundary for SEC issuer identity, "
            "outstanding-share facts, 13F positions, and filer identity."
        ),
    ),
    "review_snapshot": AssetNarrative(
        key_name="security_shareholder__review_snapshot",
        title="Publish review",
        purpose=(
            "Publishes the curated securities and holdings mart rows into the "
            "reviewer workbenches for both datasets."
        ),
        significance=(
            "This is the human approval boundary; reviewed rows must be approved "
            "before either dataset can be exported."
        ),
    ),
    "csv_export": AssetNarrative(
        key_name="security_shareholder__csv_export",
        title="Write CSVs",
        purpose=(
            "Writes the validated security master and shareholder holdings datasets "
            "into the outbound CSV artifacts."
        ),
        significance=(
            "Delivers the reviewed daily security universe and ownership file that "
            "downstream index, analytics, and publishing consumers can load without "
            "reinterpreting review logic."
        ),
    ),
}

# ── dbt asset narratives (4 flat models) ──────────────────────────────────────

DBT_ASSET_NARRATIVES: dict[tuple[str, str], AssetNarrative] = {
    ("security_shareholder", "securities"): AssetNarrative(
        key_name="security_shareholder__securities",
        title="Securities",
        purpose=(
            "Builds the daily security master by joining SEC ticker identity with "
            "outstanding-share facts and deriving free-float and investability metrics."
        ),
        significance=(
            "Serves as the governed security dimension used by review, holdings "
            "enrichment, and export."
        ),
    ),
    ("security_shareholder", "holdings"): AssetNarrative(
        key_name="security_shareholder__holdings",
        title="Holdings",
        purpose=(
            "Builds the daily shareholder holdings fact table by enriching 13F "
            "positions with filer identity, security reference, and portfolio weights."
        ),
        significance=(
            "Serves as the governed ownership fact used in review, audit, and "
            "delivery."
        ),
    ),
    ("security_shareholder", "securities_export"): AssetNarrative(
        key_name="security_shareholder__securities_export",
        title="Securities Export",
        purpose=(
            "Shapes approved security master review rows into the final outbound "
            "schema."
        ),
        significance=(
            "This is the last warehouse state before the reviewed security master is "
            "written to file."
        ),
    ),
    ("security_shareholder", "holdings_export"): AssetNarrative(
        key_name="security_shareholder__holdings_export",
        title="Holdings Export",
        purpose=(
            "Shapes approved shareholder holdings review rows into the final outbound "
            "schema."
        ),
        significance=(
            "This is the last warehouse state before the reviewed holdings dataset is "
            "written to file."
        ),
    ),
}


def manual_asset_narrative(asset_code: str) -> AssetNarrative:
    return MANUAL_ASSET_NARRATIVES[asset_code]


def manual_asset_key(asset_code: str) -> dg.AssetKey:
    return raw_asset_key(manual_asset_narrative(asset_code).key_name)


def dbt_asset_narrative(pipeline_code: str, model_name: str) -> AssetNarrative:
    fallback_title = model_name.replace("_", " ").title()
    return DBT_ASSET_NARRATIVES.get(
        (pipeline_code, model_name),
        AssetNarrative(
            key_name=f"{pipeline_code}__{model_name}",
            title=fallback_title,
            purpose="Transforms the landed warehouse contract into a downstream-ready dataset.",
            significance="Supports the reviewed daily dataset and its downstream export path.",
        ),
    )


def dbt_asset_key(pipeline_code: str, model_name: str) -> dg.AssetKey:
    return dg.AssetKey([dbt_asset_narrative(pipeline_code, model_name).key_name])


# ── dbt translator ─────────────────────────────────────────────────────────────

_SCHEMA_FOR_TAG = {
    "exports": "export",
}


def _schema_for_resource(resource_props: Mapping[str, Any]) -> str:
    tags = set(resource_props.get("tags", []))
    if "exports" in tags:
        return "export"
    config_schema = resource_props.get("config", {}).get("schema")
    if config_schema:
        return config_schema
    return resource_props.get("schema", "marts")


def group_for_resource(resource_props: Mapping[str, Any]) -> str:
    tags = set(resource_props.get("tags", []))
    if "exports" in tags:
        return "validated_export"
    return "warehouse_transform"


class SecurityShareholderDbtTranslator(DagsterDbtTranslator):
    def __init__(self) -> None:
        super().__init__(
            settings=DagsterDbtTranslatorSettings(
                enable_asset_checks=True,
                enable_source_tests_as_checks=True,
            )
        )

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props.get("resource_type") == "source":
            return super().get_asset_key(dbt_resource_props)
        return dbt_asset_key("security_shareholder", str(dbt_resource_props["name"]))

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        return group_for_resource(dbt_resource_props)

    def get_description(self, dbt_resource_props: Mapping[str, Any]) -> str:
        group_name = self.get_group_name(dbt_resource_props) or "warehouse_transform"
        relation = (
            f"{_schema_for_resource(dbt_resource_props)}."
            f"{dbt_resource_props.get('alias') or dbt_resource_props['name']}"
        )
        narrative = dbt_asset_narrative("security_shareholder", str(dbt_resource_props["name"]))
        base_description = super().get_description(dbt_resource_props)
        return asset_description(
            narrative,
            layer=group_name,
            tool="dbt",
            database="Postgres",
            relation=relation,
            materialization=dbt_resource_props.get("config", {}).get("materialized", "table"),
            base_description=base_description,
        )

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> dict[str, Any]:
        metadata = dict(super().get_metadata(dbt_resource_props))
        relation_name = (
            f"{_schema_for_resource(dbt_resource_props)}."
            f"{dbt_resource_props.get('alias') or dbt_resource_props['name']}"
        )
        metadata.update(
            asset_metadata(
                dbt_asset_narrative("security_shareholder", str(dbt_resource_props["name"])),
                pipeline_code="security_shareholder",
                layer=self.get_group_name(dbt_resource_props) or "warehouse_transform",
                tool="dbt",
                database="postgres",
                relation=relation_name,
                materialization=dbt_resource_props.get("config", {}).get(
                    "materialized", "table"
                ),
            )
        )
        return metadata

    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> dict[str, str]:
        tags = dict(super().get_tags(dbt_resource_props))
        tags.update(
            {
                "dagflow_pipeline": "security_shareholder",
                "dagflow_layer": self.get_group_name(dbt_resource_props)
                or "warehouse_transform",
                "dagflow_tool": "dbt",
            }
        )
        return tags

    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: Any | None,
    ) -> dg.AssetSpec:
        base_spec = super().get_asset_spec(manifest, unique_id, project)
        resource_props = self.get_resource_props(manifest, unique_id)
        return base_spec.replace_attributes(
            group_name=self.get_group_name(resource_props),
            kinds={"dbt", "postgres"},
        )


DAGFLOW_DBT_TRANSLATOR = SecurityShareholderDbtTranslator()
