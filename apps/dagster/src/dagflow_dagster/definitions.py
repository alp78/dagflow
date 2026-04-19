from __future__ import annotations

import importlib
import json
from pathlib import Path

from dagster import Definitions

from dagflow_dagster.resources import build_resources


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "config.json").exists():
            return parent
    raise FileNotFoundError("config.json not found in any parent directory")


_all_assets: list = []
_all_jobs: list = []
_all_sensors: list = []

for _wf in json.loads((_repo_root() / "config.json").read_text())["workflows"]:
    _mod = importlib.import_module(f"{_wf['package']}.definitions")
    _all_assets.extend(_mod.ASSETS)
    _all_jobs.extend(_mod.JOBS)
    _all_sensors.extend(_mod.SENSORS)

defs = Definitions(
    assets=_all_assets,
    jobs=_all_jobs,
    sensors=_all_sensors,
    resources=build_resources(),
)
