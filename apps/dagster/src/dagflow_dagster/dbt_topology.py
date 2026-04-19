from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from dagster_dbt import DbtProject

from dagflow_dagster.config import get_settings


@dataclass(frozen=True)
class AssetNarrative:
    key_name: str
    title: str
    purpose: str
    significance: str


import dagster as dg


def raw_asset_key(name: str) -> dg.AssetKey:
    return dg.AssetKey([name])


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
        f"This {_humanize_layer(layer)} asset is built with {tool} and stored in {database}."
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
