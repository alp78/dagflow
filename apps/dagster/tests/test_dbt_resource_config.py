from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

from dagflow_dagster import resources as resources_module
from dagster_dbt import DbtProject


def test_build_resources_clears_process_dbt_target_cache(
    tmp_path: Path, monkeypatch
) -> None:
    project_dir = tmp_path / "dbt"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text(
        "name: dagflow_test\nversion: '1.0'\nconfig-version: 2\nprofile: dagflow\n",
        encoding="utf-8",
    )
    (project_dir / "profiles.yml").write_text(
        (
            "dagflow:\n"
            "  target: local\n"
            "  outputs:\n"
            "    local:\n"
            "      type: postgres\n"
            "      host: localhost\n"
            "      user: postgres\n"
            "      password: postgres\n"
            "      port: 5432\n"
            "      dbname: dagflow\n"
            "      schema: public\n"
        ),
        encoding="utf-8",
    )
    dbt_project = DbtProject(
        project_dir=project_dir,
        profiles_dir=project_dir,
        target="local",
        target_path=tmp_path / "dbt-target",
    )
    settings = SimpleNamespace(
        direct_database_url="postgresql://unused",
        export_root_dir=str(tmp_path / "exports"),
        resolved_landing_root_dir=tmp_path / "landing",
        edgar_identity="Dagflow test <test@example.com>",
        sec_13f_lookback_days=14,
        sec_13f_filing_limit=25,
        sec_security_focus_limit=10,
        openfigi_api_key=None,
        finnhub_api_key=None,
    )

    monkeypatch.setattr(resources_module, "get_settings", lambda: settings)
    monkeypatch.setattr(resources_module, "get_dbt_project", lambda: dbt_project)
    monkeypatch.setenv("DBT_TARGET_PATH", "/tmp/poisoned-dbt-target")
    monkeypatch.setenv("DBT_LOG_PATH", "/tmp/poisoned-dbt-logs")
    partial_parse_path = dbt_project.target_path / "partial_parse.msgpack"
    partial_parse_path.parent.mkdir(parents=True, exist_ok=True)
    partial_parse_path.write_text("stale-cache", encoding="utf-8")

    resources = resources_module.build_resources()

    assert os.environ.get("DBT_TARGET_PATH") is None
    assert os.environ.get("DBT_LOG_PATH") is None
    assert partial_parse_path.exists() is False
    assert "--no-partial-parse" in resources["dbt"].global_config_flags
