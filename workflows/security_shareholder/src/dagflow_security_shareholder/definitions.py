from dagster import AssetSelection, Definitions, define_asset_job, in_process_executor
from dagster_dbt import build_dbt_asset_selection

from dagflow_dagster.resources import build_resources
from dagflow_dagster.schedules import build_pipeline_sensor, build_review_validation_sensor
from dagflow_security_shareholder.pipelines.security_master.assets import (
    capture_sources,
    csv_export,
    export_assets,
    load_to_raw,
    review_snapshot,
    transform_assets,
)

PIPELINE_CODE = "security_shareholder"

_main_job = define_asset_job(
    name="security_shareholder_job",
    executor_def=in_process_executor,
    selection=AssetSelection.assets(capture_sources, load_to_raw, review_snapshot)
    | build_dbt_asset_selection([transform_assets]),
)

_export_job = define_asset_job(
    name="security_shareholder_export_job",
    executor_def=in_process_executor,
    selection=AssetSelection.assets(csv_export)
    | build_dbt_asset_selection([export_assets]),
)

ASSETS = [capture_sources, load_to_raw, transform_assets, review_snapshot, export_assets, csv_export]
JOBS = [_main_job, _export_job]
SENSORS = [
    build_pipeline_sensor(_main_job, PIPELINE_CODE),
    build_review_validation_sensor(_export_job, PIPELINE_CODE),
]

defs = Definitions(assets=ASSETS, jobs=JOBS, sensors=SENSORS, resources=build_resources())
