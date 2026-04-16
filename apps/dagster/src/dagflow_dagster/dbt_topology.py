from __future__ import annotations

import subprocess
import sys
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings, DbtProject

from dagflow_dagster.config import get_settings


def raw_asset_key(name: str) -> dg.AssetKey:
    return dg.AssetKey([name])


def dbt_asset_key(pipeline_code: str, model_name: str) -> dg.AssetKey:
    return dg.AssetKey([f"{pipeline_code}__{model_name}"])


SECURITY_MASTER_TICKERS_ASSET_KEY = raw_asset_key("security_master__sec_company_tickers_raw")
SECURITY_MASTER_FACTS_ASSET_KEY = raw_asset_key("security_master__sec_company_facts_raw")
SHAREHOLDER_HOLDINGS_ASSET_KEY = raw_asset_key("shareholder_holdings__holdings_13f_raw")
SHAREHOLDER_FILERS_ASSET_KEY = raw_asset_key("shareholder_holdings__holdings_13f_filers_raw")
SECURITY_MASTER_REVIEW_ASSET_KEY = raw_asset_key("security_master__review_snapshot")
SHAREHOLDER_HOLDINGS_REVIEW_ASSET_KEY = raw_asset_key(
    "shareholder_holdings__review_snapshot"
)


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
    if path.startswith("models/staging/") or path.startswith("models/intermediate/"):
        return "silver"
    if path.startswith("models/marts/") or path.startswith("models/exports/"):
        return "gold"
    return "silver"


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
        return dbt_asset_key(
            pipeline_for_resource(dbt_resource_props), str(dbt_resource_props["name"])
        )

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        return group_for_resource_path(dbt_resource_props.get("original_file_path", ""))

    def get_description(self, dbt_resource_props: Mapping[str, Any]) -> str:
        group_name = self.get_group_name(dbt_resource_props) or "silver"
        relation = (
            f"{_schema_for_resource(dbt_resource_props)}."
            f"{dbt_resource_props.get('alias') or dbt_resource_props['name']}"
        )
        summary = (
            f"{group_name.title()} layer dbt "
            f"{dbt_resource_props['resource_type']} in Postgres relation `{relation}`."
        )
        base_description = super().get_description(dbt_resource_props)
        if base_description:
            return f"{summary}\n\n{base_description}"
        return summary

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> dict[str, Any]:
        metadata = dict(super().get_metadata(dbt_resource_props))
        relation_name = (
            f"{_schema_for_resource(dbt_resource_props)}."
            f"{dbt_resource_props.get('alias') or dbt_resource_props['name']}"
        )
        metadata.update(
            {
                "dagflow_pipeline": pipeline_for_resource(dbt_resource_props),
                "dagflow_layer": self.get_group_name(dbt_resource_props) or "silver",
                "dagflow_tool": "dbt",
                "dagflow_database": "postgres",
                "dagflow_relation": relation_name,
                "dagflow_materialization": dbt_resource_props.get("config", {}).get(
                    "materialized", "table"
                ),
            }
        )
        return metadata

    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> dict[str, str]:
        tags = dict(super().get_tags(dbt_resource_props))
        tags.update(
            {
                "dagflow_pipeline": pipeline_for_resource(dbt_resource_props),
                "dagflow_layer": self.get_group_name(dbt_resource_props) or "silver",
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
