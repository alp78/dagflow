from __future__ import annotations

from dagster import Definitions, define_asset_job

from dagflow_dagster.assets import (
    holdings_13f_filers_raw,
    holdings_13f_raw,
    sec_company_facts_raw,
    sec_company_tickers_raw,
    security_master_exports,
    security_master_intermediate,
    security_master_marts,
    security_master_staging,
    shareholder_holdings_exports,
    shareholder_holdings_intermediate,
    shareholder_holdings_marts,
    shareholder_holdings_staging,
)
from dagflow_dagster.resources import build_resources
from dagflow_dagster.schedules import build_pipeline_sensor

security_master_job = define_asset_job(
    name="security_master_job",
    selection=[
        sec_company_tickers_raw.key,
        sec_company_facts_raw.key,
        security_master_staging.key,
        security_master_intermediate.key,
        security_master_marts.key,
        security_master_exports.key,
    ],
)

shareholder_holdings_job = define_asset_job(
    name="shareholder_holdings_job",
    selection=[
        holdings_13f_raw.key,
        holdings_13f_filers_raw.key,
        shareholder_holdings_staging.key,
        shareholder_holdings_intermediate.key,
        shareholder_holdings_marts.key,
        shareholder_holdings_exports.key,
    ],
)

defs = Definitions(
    assets=[
        sec_company_tickers_raw,
        sec_company_facts_raw,
        holdings_13f_raw,
        holdings_13f_filers_raw,
        security_master_staging,
        security_master_intermediate,
        security_master_marts,
        security_master_exports,
        shareholder_holdings_staging,
        shareholder_holdings_intermediate,
        shareholder_holdings_marts,
        shareholder_holdings_exports,
    ],
    jobs=[security_master_job, shareholder_holdings_job],
    sensors=[
        build_pipeline_sensor(security_master_job, "security_master"),
        build_pipeline_sensor(shareholder_holdings_job, "shareholder_holdings"),
    ],
    resources=build_resources(),
)
