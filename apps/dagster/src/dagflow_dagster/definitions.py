from __future__ import annotations

from dagster import AssetSelection, Definitions, define_asset_job, in_process_executor
from dagster_dbt import build_dbt_asset_selection

from dagflow_dagster.assets import (
    holdings_13f_filers_raw,
    holdings_13f_raw,
    sec_company_facts_raw,
    sec_company_tickers_raw,
    security_master_csv_export,
    security_master_export_assets,
    security_master_review_snapshot,
    security_master_transform_assets,
    shareholder_holdings_csv_export,
    shareholder_holdings_export_assets,
    shareholder_holdings_review_snapshot,
    shareholder_holdings_transform_assets,
)
from dagflow_dagster.resources import build_resources
from dagflow_dagster.schedules import build_pipeline_sensor, build_review_validation_sensor

security_master_job = define_asset_job(
    name="security_master_job",
    executor_def=in_process_executor,
    selection=AssetSelection.assets(
        sec_company_tickers_raw,
        sec_company_facts_raw,
        security_master_review_snapshot,
    )
    | build_dbt_asset_selection([security_master_transform_assets]),
)

shareholder_holdings_job = define_asset_job(
    name="shareholder_holdings_job",
    executor_def=in_process_executor,
    selection=AssetSelection.assets(
        holdings_13f_raw,
        holdings_13f_filers_raw,
        shareholder_holdings_review_snapshot,
    )
    | build_dbt_asset_selection([shareholder_holdings_transform_assets]),
)

security_master_export_job = define_asset_job(
    name="security_master_export_job",
    executor_def=in_process_executor,
    selection=AssetSelection.assets(security_master_csv_export)
    | build_dbt_asset_selection([security_master_export_assets]),
)

shareholder_holdings_export_job = define_asset_job(
    name="shareholder_holdings_export_job",
    executor_def=in_process_executor,
    selection=AssetSelection.assets(shareholder_holdings_csv_export)
    | build_dbt_asset_selection([shareholder_holdings_export_assets]),
)

defs = Definitions(
    assets=[
        sec_company_tickers_raw,
        sec_company_facts_raw,
        holdings_13f_raw,
        holdings_13f_filers_raw,
        security_master_transform_assets,
        security_master_export_assets,
        security_master_csv_export,
        security_master_review_snapshot,
        shareholder_holdings_transform_assets,
        shareholder_holdings_export_assets,
        shareholder_holdings_csv_export,
        shareholder_holdings_review_snapshot,
    ],
    jobs=[
        security_master_job,
        shareholder_holdings_job,
        security_master_export_job,
        shareholder_holdings_export_job,
    ],
    sensors=[
        build_pipeline_sensor(security_master_job, "security_master"),
        build_pipeline_sensor(shareholder_holdings_job, "shareholder_holdings"),
        build_review_validation_sensor(
            security_master_export_job, "security_master"
        ),
        build_review_validation_sensor(
            shareholder_holdings_export_job, "shareholder_holdings"
        ),
    ],
    resources=build_resources(),
)
