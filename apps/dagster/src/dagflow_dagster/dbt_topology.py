from __future__ import annotations

import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings, DbtProject

from dagflow_dagster.config import get_settings


@dataclass(frozen=True)
class AssetNarrative:
    key_name: str
    title: str
    purpose: str
    significance: str


MANUAL_ASSET_NARRATIVES: dict[str, AssetNarrative] = {
    "security_master_ticker_capture": AssetNarrative(
        key_name="security_master__ticker_file",
        title="Ticker file",
        purpose=(
            "Captures the daily SEC ticker universe as the immutable source file for "
            "the security master run."
        ),
        significance=(
            "Defines which issuers are in scope for the day and anchors every "
            "downstream security reference record."
        ),
    ),
    "security_master_ticker_contract": AssetNarrative(
        key_name="security_master__ticker_contract",
        title="Ticker contract",
        purpose=(
            "Loads the landed ticker file into the typed raw contract table for "
            "issuer identity and exchange coverage."
        ),
        significance=(
            "Creates the replayable warehouse boundary for SEC issuer identity data."
        ),
    ),
    "security_master_facts_capture": AssetNarrative(
        key_name="security_master__issuer_facts_file",
        title="Issuer facts file",
        purpose=(
            "Captures SEC company facts for the landed ticker universe as the "
            "immutable source file."
        ),
        significance=(
            "Brings in authoritative outstanding-share facts needed to compute free "
            "float and investable shares."
        ),
    ),
    "security_master_facts_contract": AssetNarrative(
        key_name="security_master__issuer_facts_contract",
        title="Issuer facts contract",
        purpose=(
            "Loads the landed issuer facts file into the typed raw contract table."
        ),
        significance=(
            "Makes the SEC facts replayable and queryable for every historical run."
        ),
    ),
    "shareholder_holdings_capture": AssetNarrative(
        key_name="shareholder_holdings__holding_file",
        title="Holding file",
        purpose=(
            "Captures the resolved daily 13F holding rows as the immutable source "
            "file for shareholder analytics."
        ),
        significance=(
            "Provides the daily positions that drive ownership, breadth, and "
            "concentration analytics."
        ),
    ),
    "shareholder_holdings_contract": AssetNarrative(
        key_name="shareholder_holdings__holding_contract",
        title="Holding contract",
        purpose=(
            "Loads the landed holdings file into the typed raw holdings contract "
            "table."
        ),
        significance=(
            "Creates the replayable daily position boundary for downstream review "
            "and export."
        ),
    ),
    "shareholder_filers_capture": AssetNarrative(
        key_name="shareholder_holdings__filer_file",
        title="Filer file",
        purpose=(
            "Captures the daily 13F filer roster associated with the landed holdings "
            "file."
        ),
        significance=(
            "Preserves manager identity and filing context needed to group holdings "
            "by reporting shareholder."
        ),
    ),
    "shareholder_filers_contract": AssetNarrative(
        key_name="shareholder_holdings__filer_contract",
        title="Filer contract",
        purpose=(
            "Loads the landed filer roster into the typed raw filer contract table."
        ),
        significance=(
            "Makes filer identity replayable and joinable across historical holding "
            "runs."
        ),
    ),
    "security_master_review": AssetNarrative(
        key_name="security_master__review_queue",
        title="Security review queue",
        purpose=(
            "Publishes the curated security master rows into the reviewer workbench."
        ),
        significance=(
            "This is the human approval boundary before validated securities can be "
            "exported."
        ),
    ),
    "shareholder_holdings_review": AssetNarrative(
        key_name="shareholder_holdings__review_queue",
        title="Holdings review queue",
        purpose=(
            "Publishes the curated shareholder holdings rows into the reviewer "
            "workbench."
        ),
        significance=(
            "This is the human approval boundary before validated holdings can be "
            "exported."
        ),
    ),
    "security_master_export": AssetNarrative(
        key_name="security_master__daily_csv",
        title="Security export file",
        purpose=(
            "Writes the validated daily security master into the outbound CSV "
            "artifact."
        ),
        significance=(
            "Delivers the reviewed daily security universe that downstream index, "
            "analytics, and publishing consumers can load without reinterpreting "
            "review logic."
        ),
    ),
    "shareholder_holdings_export": AssetNarrative(
        key_name="shareholder_holdings__daily_csv",
        title="Holdings export file",
        purpose=(
            "Writes the validated daily shareholder holdings dataset into the "
            "outbound CSV artifact."
        ),
        significance=(
            "Delivers the reviewed daily ownership file used by downstream "
            "reporting and distribution processes as the official end-of-run "
            "positions output."
        ),
    ),
}

DBT_ASSET_NARRATIVES: dict[tuple[str, str], AssetNarrative] = {
    ("security_master", "stg_sec_company_tickers"): AssetNarrative(
        key_name="security_master__ticker_reference",
        title="Ticker reference",
        purpose=(
            "Standardizes the landed SEC ticker contract into clean issuer reference "
            "rows."
        ),
        significance=(
            "Normalizes ticker, CIK, issuer, and exchange fields before any "
            "downstream calculations depend on them."
        ),
    ),
    ("security_master", "stg_sec_company_facts"): AssetNarrative(
        key_name="security_master__shares_outstanding",
        title="Shares outstanding",
        purpose=(
            "Standardizes the authoritative SEC outstanding-share facts into daily "
            "issuer facts rows."
        ),
        significance=(
            "Provides the denominator for free-float and investability calculations."
        ),
    ),
    ("security_master", "int_security_base"): AssetNarrative(
        key_name="security_master__float_inputs",
        title="Float inputs",
        purpose=(
            "Combines issuer reference data with outstanding-share facts into the "
            "daily inputs for float calculations."
        ),
        significance=(
            "Aligns identity and share-count data at one daily grain before metrics "
            "are derived."
        ),
    ),
    ("security_master", "int_security_attributes"): AssetNarrative(
        key_name="security_master__float_metrics",
        title="Float metrics",
        purpose=(
            "Calculates free float, investability, and review materiality for each "
            "security."
        ),
        significance=(
            "Creates the key business metrics used by reviewers and downstream "
            "holdings enrichment."
        ),
    ),
    ("security_master", "dim_security"): AssetNarrative(
        key_name="security_master__daily_security_master",
        title="Daily security master",
        purpose=(
            "Publishes the final curated security master at daily grain for the run."
        ),
        significance=(
            "Serves as the governed security dimension used by review, holdings "
            "enrichment, and export."
        ),
    ),
    ("security_master", "security_master_final"): AssetNarrative(
        key_name="security_master__export_ready",
        title="Security export ready",
        purpose=(
            "Shapes the validated security master into the final outbound schema."
        ),
        significance=(
            "This is the last warehouse state before the reviewed security master is "
            "written to file."
        ),
    ),
    ("shareholder_holdings", "stg_13f_holdings"): AssetNarrative(
        key_name="shareholder_holdings__holding_positions",
        title="Holding positions",
        purpose=(
            "Standardizes the landed 13F holding contract into clean daily position "
            "rows."
        ),
        significance=(
            "Normalizes source holding records before they are joined to filer and "
            "security context."
        ),
    ),
    ("shareholder_holdings", "stg_13f_filers"): AssetNarrative(
        key_name="shareholder_holdings__filer_reference",
        title="Filer reference",
        purpose=(
            "Standardizes the landed 13F filer contract into clean filer identity "
            "rows."
        ),
        significance=(
            "Preserves manager identity and filing context for grouping holdings by "
            "reporting entity."
        ),
    ),
    ("shareholder_holdings", "int_shareholder_base"): AssetNarrative(
        key_name="shareholder_holdings__shareholder_reference",
        title="Shareholder reference",
        purpose=(
            "Builds the daily shareholder reference by selecting the active filer "
            "record for each reporting manager."
        ),
        significance=(
            "Creates the daily shareholder dimension that every holding record rolls "
            "up to."
        ),
    ),
    ("shareholder_holdings", "int_holding_with_security"): AssetNarrative(
        key_name="shareholder_holdings__positions_enriched",
        title="Positions enriched",
        purpose=(
            "Enriches daily holdings with shareholder identity, security reference, "
            "and confidence metrics."
        ),
        significance=(
            "This is where raw 13F positions become analytically useful ownership "
            "records."
        ),
    ),
    ("shareholder_holdings", "dim_shareholder"): AssetNarrative(
        key_name="shareholder_holdings__shareholder_dimension",
        title="Shareholder dimension",
        purpose=(
            "Publishes the curated daily shareholder dimension for the run."
        ),
        significance=(
            "Provides the governed reporting-entity dimension used by holdings "
            "review and historical SCD2 tracking."
        ),
    ),
    ("shareholder_holdings", "fact_shareholder_holding"): AssetNarrative(
        key_name="shareholder_holdings__daily_holdings",
        title="Daily holdings",
        purpose=(
            "Publishes the curated daily shareholder holdings fact table for review "
            "and export."
        ),
        significance=(
            "Serves as the governed ownership fact used in review, audit, and "
            "delivery."
        ),
    ),
    ("shareholder_holdings", "shareholder_holdings_final"): AssetNarrative(
        key_name="shareholder_holdings__export_ready",
        title="Holdings export ready",
        purpose=(
            "Shapes the validated shareholder holdings dataset into the final "
            "outbound schema."
        ),
        significance=(
            "This is the last warehouse state before the reviewed holdings dataset "
            "is written to file."
        ),
    ),
}


def raw_asset_key(name: str) -> dg.AssetKey:
    return dg.AssetKey([name])


def manual_asset_narrative(asset_code: str) -> AssetNarrative:
    return MANUAL_ASSET_NARRATIVES[asset_code]


def manual_asset_key(asset_code: str) -> dg.AssetKey:
    return raw_asset_key(manual_asset_narrative(asset_code).key_name)


def _humanize_layer(layer: str) -> str:
    return layer.replace("_", " ")


def asset_description(
    narrative: AssetNarrative,
    *,
    layer: str,
    tool: str,
    database: str,
    relation: str | None = None,
    materialization: str | None = None,
    base_description: str | None = None,
) -> str:
    operational_bits = [
        f"This { _humanize_layer(layer) } asset is built with {tool} and stored in {database}."
    ]
    if relation:
        operational_bits.append(f"It lands in `{relation}`.")
    if materialization:
        operational_bits.append(f"It materializes as `{materialization}`.")
    summary = (
        f"{narrative.title}. {narrative.purpose}\n\n"
        f"{narrative.significance}\n\n"
        f"{' '.join(operational_bits)}"
    )
    if base_description:
        return f"{summary}\n\n{base_description}"
    return summary


def asset_metadata(
    narrative: AssetNarrative,
    *,
    pipeline_code: str,
    layer: str,
    tool: str,
    database: str,
    relation: str | None = None,
    materialization: str | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "dagflow_pipeline": pipeline_code,
        "dagflow_layer": layer,
        "dagflow_tool": tool,
        "dagflow_database": database,
        "dagflow_business_name": narrative.title,
        "dagflow_business_purpose": narrative.purpose,
        "dagflow_business_significance": narrative.significance,
    }
    if relation:
        metadata["dagflow_relation"] = relation
    if materialization:
        metadata["dagflow_materialization"] = materialization
    return metadata


def dbt_asset_narrative(pipeline_code: str, model_name: str) -> AssetNarrative:
    fallback_title = model_name.removeprefix("stg_").removeprefix("int_").replace("_", " ")
    return DBT_ASSET_NARRATIVES.get(
        (pipeline_code, model_name),
        AssetNarrative(
            key_name=f"{pipeline_code}__{model_name}",
            title=fallback_title.title(),
            purpose="Transforms the landed warehouse contract into a downstream-ready dataset.",
            significance="Supports the reviewed daily dataset and its downstream export path.",
        ),
    )


def dbt_asset_key(pipeline_code: str, model_name: str) -> dg.AssetKey:
    return dg.AssetKey([dbt_asset_narrative(pipeline_code, model_name).key_name])


SECURITY_MASTER_TICKERS_ASSET_KEY = manual_asset_key("security_master_ticker_contract")
SECURITY_MASTER_FACTS_ASSET_KEY = manual_asset_key("security_master_facts_contract")
SHAREHOLDER_HOLDINGS_ASSET_KEY = manual_asset_key("shareholder_holdings_contract")
SHAREHOLDER_FILERS_ASSET_KEY = manual_asset_key("shareholder_filers_contract")
SECURITY_MASTER_REVIEW_ASSET_KEY = manual_asset_key("security_master_review")
SHAREHOLDER_HOLDINGS_REVIEW_ASSET_KEY = manual_asset_key("shareholder_holdings_review")


def _schema_for_resource(resource_props: Mapping[str, Any]) -> str:
    path = resource_props.get("original_file_path", "")
    if path.startswith("models/staging/"):
        return "staging"
    if path.startswith("models/intermediate/"):
        return "intermediate"
    if path.startswith("models/marts/"):
        return "marts"
    if path.startswith("models/exports/"):
        return "export"
    return resource_props.get("schema", "public")


def group_for_resource_path(path: str) -> str:
    if (
        path.startswith("models/staging/")
        or path.startswith("models/intermediate/")
        or path.startswith("models/marts/")
    ):
        return "warehouse_transform"
    if path.startswith("models/exports/"):
        return "validated_export"
    return "warehouse_transform"


def pipeline_for_resource(dbt_resource_props: Mapping[str, Any]) -> str:
    tags = set(dbt_resource_props.get("tags", []))
    if "shareholder_holdings" in tags:
        return "shareholder_holdings"
    if "security_master" in tags:
        return "security_master"

    path = dbt_resource_props.get("original_file_path", "")
    if "/shareholder_holdings/" in path:
        return "shareholder_holdings"
    return "security_master"


def _dbt_source_paths(project_dir: Path, profiles_dir: Path) -> list[Path]:
    candidate_roots = [
        project_dir / "models",
        project_dir / "macros",
        project_dir / "snapshots",
        project_dir / "tests",
    ]
    paths = [project_dir / "dbt_project.yml", profiles_dir / "profiles.yml"]
    for root in candidate_roots:
        if not root.exists():
            continue
        for pattern in ("*.sql", "*.yml", "*.yaml"):
            paths.extend(root.rglob(pattern))
    return [path for path in paths if path.exists()]


def _needs_manifest_refresh(project: DbtProject) -> bool:
    manifest_path = project.manifest_path
    if not manifest_path.exists():
        return True

    source_paths = _dbt_source_paths(project.project_dir, project.profiles_dir)
    if not source_paths:
        return False

    manifest_mtime = manifest_path.stat().st_mtime
    return any(path.stat().st_mtime > manifest_mtime for path in source_paths)


def _ensure_manifest(project: DbtProject) -> None:
    if not _needs_manifest_refresh(project):
        return

    dbt_executable = Path(sys.executable).with_name("dbt")
    command = [
        str(dbt_executable),
        "parse",
        "--project-dir",
        str(project.project_dir),
        "--profiles-dir",
        str(project.profiles_dir),
        "--target-path",
        str(project.target_path),
    ]
    if project.target:
        command.extend(["--target", project.target])
    subprocess.run(
        command,
        check=True,
        cwd=project.project_dir.parent,
    )


@lru_cache(maxsize=1)
def get_dbt_project() -> DbtProject:
    settings = get_settings()
    project = DbtProject(
        project_dir=settings.dbt_project_dir,
        profiles_dir=settings.resolved_dbt_profiles_dir,
        target="local",
        target_path=Path("/tmp/dagflow-dbt-target"),
    )
    project.prepare_if_dev()
    _ensure_manifest(project)
    return project


class DagflowDbtTranslator(DagsterDbtTranslator):
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
        pipeline_code = pipeline_for_resource(dbt_resource_props)
        return dbt_asset_key(pipeline_code, str(dbt_resource_props["name"]))

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        return group_for_resource_path(dbt_resource_props.get("original_file_path", ""))

    def get_description(self, dbt_resource_props: Mapping[str, Any]) -> str:
        pipeline_code = pipeline_for_resource(dbt_resource_props)
        group_name = self.get_group_name(dbt_resource_props) or "warehouse_transform"
        relation = (
            f"{_schema_for_resource(dbt_resource_props)}."
            f"{dbt_resource_props.get('alias') or dbt_resource_props['name']}"
        )
        narrative = dbt_asset_narrative(pipeline_code, str(dbt_resource_props["name"]))
        base_description = super().get_description(dbt_resource_props)
        return asset_description(
            narrative,
            layer=group_name,
            tool="dbt",
            database="Postgres",
            relation=relation,
            materialization=dbt_resource_props.get("config", {}).get(
                "materialized", "table"
            ),
            base_description=base_description,
        )

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> dict[str, Any]:
        pipeline_code = pipeline_for_resource(dbt_resource_props)
        metadata = dict(super().get_metadata(dbt_resource_props))
        relation_name = (
            f"{_schema_for_resource(dbt_resource_props)}."
            f"{dbt_resource_props.get('alias') or dbt_resource_props['name']}"
        )
        metadata.update(
            asset_metadata(
                dbt_asset_narrative(pipeline_code, str(dbt_resource_props["name"])),
                pipeline_code=pipeline_code,
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
                "dagflow_pipeline": pipeline_for_resource(dbt_resource_props),
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
        project: DbtProject | None,
    ) -> dg.AssetSpec:
        base_spec = super().get_asset_spec(manifest, unique_id, project)
        resource_props = self.get_resource_props(manifest, unique_id)
        return base_spec.replace_attributes(
            group_name=self.get_group_name(resource_props),
            kinds={"dbt", "postgres"},
        )
