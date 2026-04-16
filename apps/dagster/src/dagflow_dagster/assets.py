from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from uuid import UUID

from dagflow_source_adapters.registry import get_adapter
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    MaterializeResult,
    asset,
)
from dagster_dbt import DbtCliResource, dbt_assets

from dagflow_dagster.dbt_topology import (
    SECURITY_MASTER_FACTS_ASSET_KEY,
    SECURITY_MASTER_REVIEW_ASSET_KEY,
    SECURITY_MASTER_TICKERS_ASSET_KEY,
    SHAREHOLDER_FILERS_ASSET_KEY,
    SHAREHOLDER_HOLDINGS_ASSET_KEY,
    SHAREHOLDER_HOLDINGS_REVIEW_ASSET_KEY,
    DagflowDbtTranslator,
    dbt_asset_key,
    get_dbt_project,
    raw_asset_key,
)
from dagflow_dagster.resources import ControlPlaneResource

DBT_PROJECT = get_dbt_project()
DAGFLOW_DBT_TRANSLATOR = DagflowDbtTranslator()
SECURITY_MASTER_CSV_EXPORT_ASSET_KEY = raw_asset_key("security_master__csv_export")
SHAREHOLDER_HOLDINGS_CSV_EXPORT_ASSET_KEY = raw_asset_key(
    "shareholder_holdings__csv_export"
)


def _orchestrator_run_id(context: AssetExecutionContext) -> UUID:
    return UUID(context.run_id)


def _pipeline_run_id(context: AssetExecutionContext, pipeline_code: str) -> UUID:
    tagged_run_id = context.run_tags.get("dagflow_run_id")
    tagged_pipeline_code = context.run_tags.get("dagflow_pipeline_code")
    if tagged_run_id and tagged_pipeline_code == pipeline_code:
        return UUID(tagged_run_id)
    return _orchestrator_run_id(context)


def _business_date(
    context: AssetExecutionContext, control_plane: ControlPlaneResource
) -> date:
    for tag_name in ("dagflow_business_date", "validated_business_date", "business_date"):
        tag_value = context.run_tags.get(tag_name)
        if tag_value:
            return date.fromisoformat(tag_value)
    return control_plane.current_business_date()


def _rows_loaded_check(row_count: int) -> AssetCheckResult:
    return AssetCheckResult(
        check_name="rows_loaded",
        passed=row_count > 0,
        metadata={"row_count": row_count},
        description="Verifies that the ingestion asset loaded at least one row.",
    )


def _review_rows_check(row_count: int) -> AssetCheckResult:
    return AssetCheckResult(
        check_name="review_rows_published",
        passed=row_count > 0,
        metadata={"review_row_count": row_count},
        description="Verifies that the review snapshot published rows for analysts.",
    )


def _export_rows_check(row_count: int, file_path: str) -> AssetCheckResult:
    return AssetCheckResult(
        check_name="csv_written",
        passed=row_count > 0,
        metadata={"export_row_count": row_count, "file_path": file_path},
        description="Verifies that the validated review run was written to a CSV export.",
    )


def _dbt_vars(
    context: AssetExecutionContext,
    control_plane: ControlPlaneResource,
    pipeline_code: str,
) -> dict[str, str]:
    business_date = _business_date(context, control_plane)
    vars_payload = {
        "dagflow_run_id": str(_pipeline_run_id(context, pipeline_code)),
        "dagflow_pipeline_code": pipeline_code,
        "dagflow_business_date": business_date.isoformat(),
    }
    if pipeline_code == "shareholder_holdings":
        security_run_id = control_plane.last_pipeline_run_id("security_master")
        if security_run_id is not None:
            vars_payload["dagflow_security_run_id"] = str(security_run_id)
    return vars_payload


def _stream_dbt_build(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    dataset_code: str,
    stage: str,
) -> Iterator:
    business_date = _business_date(context, control_plane)
    pipeline_run_id = _pipeline_run_id(context, pipeline_code)
    command = [
        "build",
        "--vars",
        json.dumps(_dbt_vars(context, control_plane, pipeline_code)),
    ]
    try:
        yield from dbt.cli(command, context=context).stream()
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=pipeline_run_id,
            step_name=context.asset_key.to_user_string(),
            stage=stage,
            business_date=business_date,
            error=error,
            diagnostic_metadata={"dbt_command": command},
        )
        raise


def _stream_dbt_snapshot(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    dataset_code: str,
    snapshot_name: str,
) -> Iterator:
    business_date = _business_date(context, control_plane)
    pipeline_run_id = _pipeline_run_id(context, pipeline_code)
    command = [
        "snapshot",
        "--select",
        snapshot_name,
        "--vars",
        json.dumps(_dbt_vars(context, control_plane, pipeline_code)),
    ]
    try:
        yield from dbt.cli(command, context=context).stream()
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=pipeline_run_id,
            step_name=context.asset_key.to_user_string(),
            stage="dbt_snapshot",
            business_date=business_date,
            error=error,
            diagnostic_metadata={"dbt_command": command, "snapshot_name": snapshot_name},
        )
        raise


def _publish_snapshot(
    context: AssetExecutionContext,
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    dataset_code: str,
) -> MaterializeResult:
    run_id = _pipeline_run_id(context, pipeline_code)
    business_date = _business_date(context, control_plane)
    try:
        payload = control_plane.publish_review_snapshot(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=business_date,
            actor="dagster",
            notes="Review snapshot published by Dagster live SEC pipeline",
        )
        review_row_count = control_plane.review_row_count(dataset_code, run_id)
        control_plane.complete_pipeline_run(
            run_id=run_id,
            status="pending_review",
            metadata={"review_state": payload["review_state"]},
        )
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            step_name=context.asset_key.to_user_string(),
            stage="publish_review_snapshot",
            business_date=business_date,
            error=error,
        )
        raise
    return MaterializeResult(
        metadata={**payload, "review_row_count": review_row_count},
        check_results=[_review_rows_check(review_row_count)],
    )


@asset(
    key=SECURITY_MASTER_TICKERS_ASSET_KEY,
    group_name="bronze",
    kinds={"python", "postgres", "edgar"},
    description="Bronze EDGAR ticker landing asset written into raw.sec_company_tickers.",
    check_specs=[AssetCheckSpec("rows_loaded", asset=SECURITY_MASTER_TICKERS_ASSET_KEY)],
)
def sec_company_tickers_raw(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    business_date = _business_date(context, control_plane)
    run_id = _pipeline_run_id(context, "security_master")
    descriptor = get_adapter("sec_json").build_sources(business_date)[0]
    control_plane.ensure_pipeline_run(
        pipeline_code="security_master",
        dataset_code="security_master",
        run_id=run_id,
        business_date=business_date,
        orchestrator_run_id=context.run_id,
    )
    inserted_rows = control_plane.load_security_master_tickers(run_id, business_date)
    context.log.info("Inserted live SEC ticker records into raw.sec_company_tickers")
    return MaterializeResult(
        metadata={
            "run_id": str(run_id),
            "business_date": business_date.isoformat(),
            "landing_table": descriptor.landing_table,
            "source_url": descriptor.source_url,
            "inserted_rows": inserted_rows,
            "dagflow_layer": "bronze",
            "dagflow_tool": "python",
            "dagflow_database": "postgres",
        },
        check_results=[_rows_loaded_check(inserted_rows)],
    )


@asset(
    key=SECURITY_MASTER_FACTS_ASSET_KEY,
    deps=[SECURITY_MASTER_TICKERS_ASSET_KEY],
    group_name="bronze",
    kinds={"python", "postgres", "edgar"},
    description="Bronze EDGAR company facts landing asset written into raw.sec_company_facts.",
    check_specs=[AssetCheckSpec("rows_loaded", asset=SECURITY_MASTER_FACTS_ASSET_KEY)],
)
def sec_company_facts_raw(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    business_date = _business_date(context, control_plane)
    run_id = _pipeline_run_id(context, "security_master")
    descriptor = get_adapter("sec_json").build_sources(business_date)[1]
    control_plane.ensure_pipeline_run(
        pipeline_code="security_master",
        dataset_code="security_master",
        run_id=run_id,
        business_date=business_date,
        orchestrator_run_id=context.run_id,
    )
    inserted_rows = control_plane.load_security_master_facts(run_id, business_date)
    context.log.info("Inserted live SEC facts records into raw.sec_company_facts")
    return MaterializeResult(
        metadata={
            "run_id": str(run_id),
            "business_date": business_date.isoformat(),
            "landing_table": descriptor.landing_table,
            "source_url": descriptor.source_url,
            "inserted_rows": inserted_rows,
            "dagflow_layer": "bronze",
            "dagflow_tool": "python",
            "dagflow_database": "postgres",
        },
        check_results=[_rows_loaded_check(inserted_rows)],
    )


@asset(
    key=SHAREHOLDER_HOLDINGS_ASSET_KEY,
    group_name="bronze",
    kinds={"python", "postgres", "edgar"},
    description="Bronze EDGAR 13F holdings landing asset written into raw.holdings_13f.",
    check_specs=[AssetCheckSpec("rows_loaded", asset=SHAREHOLDER_HOLDINGS_ASSET_KEY)],
)
def holdings_13f_raw(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    business_date = _business_date(context, control_plane)
    run_id = _pipeline_run_id(context, "shareholder_holdings")
    descriptor = get_adapter("sec_13f").build_sources(business_date)[0]
    control_plane.ensure_pipeline_run(
        pipeline_code="shareholder_holdings",
        dataset_code="shareholder_holdings",
        run_id=run_id,
        business_date=business_date,
        orchestrator_run_id=context.run_id,
    )
    inserted_rows = control_plane.load_shareholder_holdings(run_id, business_date)
    context.log.info("Inserted live 13F holdings records into raw.holdings_13f")
    return MaterializeResult(
        metadata={
            "run_id": str(run_id),
            "business_date": business_date.isoformat(),
            "landing_table": descriptor.landing_table,
            "source_url": descriptor.source_url,
            "inserted_rows": inserted_rows,
            "dagflow_layer": "bronze",
            "dagflow_tool": "python",
            "dagflow_database": "postgres",
        },
        check_results=[_rows_loaded_check(inserted_rows)],
    )


@asset(
    key=SHAREHOLDER_FILERS_ASSET_KEY,
    deps=[SHAREHOLDER_HOLDINGS_ASSET_KEY],
    group_name="bronze",
    kinds={"python", "postgres", "edgar"},
    description="Bronze EDGAR 13F filer landing asset written into raw.holdings_13f_filers.",
    check_specs=[AssetCheckSpec("rows_loaded", asset=SHAREHOLDER_FILERS_ASSET_KEY)],
)
def holdings_13f_filers_raw(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    business_date = _business_date(context, control_plane)
    run_id = _pipeline_run_id(context, "shareholder_holdings")
    descriptor = get_adapter("sec_13f").build_sources(business_date)[1]
    control_plane.ensure_pipeline_run(
        pipeline_code="shareholder_holdings",
        dataset_code="shareholder_holdings",
        run_id=run_id,
        business_date=business_date,
        orchestrator_run_id=context.run_id,
    )
    inserted_rows = control_plane.load_shareholder_filers(run_id, business_date)
    context.log.info("Inserted live 13F filer records into raw.holdings_13f_filers")
    return MaterializeResult(
        metadata={
            "run_id": str(run_id),
            "business_date": business_date.isoformat(),
            "landing_table": descriptor.landing_table,
            "source_url": descriptor.source_url,
            "inserted_rows": inserted_rows,
            "dagflow_layer": "bronze",
            "dagflow_tool": "python",
            "dagflow_database": "postgres",
        },
        check_results=[_rows_loaded_check(inserted_rows)],
    )


@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    project=DBT_PROJECT,
    name="security_master_transform_assets",
    select="tag:security_master",
    exclude="tag:exports",
    dagster_dbt_translator=DAGFLOW_DBT_TRANSLATOR,
)
def security_master_transform_assets(
    context,
    dbt: DbtCliResource,
    control_plane: ControlPlaneResource,
) -> Iterator:
    yield from _stream_dbt_build(
        context,
        dbt,
        control_plane,
        "security_master",
        "security_master",
        "dbt_build",
    )
    yield from _stream_dbt_snapshot(
        context,
        dbt,
        control_plane,
        "security_master",
        "security_master",
        "dim_security_snapshot",
    )


@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    project=DBT_PROJECT,
    name="security_master_export_assets",
    select="tag:security_master,tag:exports",
    dagster_dbt_translator=DAGFLOW_DBT_TRANSLATOR,
)
def security_master_export_assets(
    context,
    dbt: DbtCliResource,
    control_plane: ControlPlaneResource,
) -> Iterator:
    yield from _stream_dbt_build(
        context,
        dbt,
        control_plane,
        "security_master",
        "security_master",
        "dbt_export_build",
    )


@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    project=DBT_PROJECT,
    name="shareholder_holdings_transform_assets",
    select="tag:shareholder_holdings",
    exclude="tag:exports",
    dagster_dbt_translator=DAGFLOW_DBT_TRANSLATOR,
)
def shareholder_holdings_transform_assets(
    context,
    dbt: DbtCliResource,
    control_plane: ControlPlaneResource,
) -> Iterator:
    yield from _stream_dbt_build(
        context,
        dbt,
        control_plane,
        "shareholder_holdings",
        "shareholder_holdings",
        "dbt_build",
    )
    yield from _stream_dbt_snapshot(
        context,
        dbt,
        control_plane,
        "shareholder_holdings",
        "shareholder_holdings",
        "dim_shareholder_snapshot",
    )


@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    project=DBT_PROJECT,
    name="shareholder_holdings_export_assets",
    select="tag:shareholder_holdings,tag:exports",
    dagster_dbt_translator=DAGFLOW_DBT_TRANSLATOR,
)
def shareholder_holdings_export_assets(
    context,
    dbt: DbtCliResource,
    control_plane: ControlPlaneResource,
) -> Iterator:
    yield from _stream_dbt_build(
        context,
        dbt,
        control_plane,
        "shareholder_holdings",
        "shareholder_holdings",
        "dbt_export_build",
    )


def _write_validated_export_csv(
    context: AssetExecutionContext,
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    dataset_code: str,
) -> MaterializeResult:
    run_id = _pipeline_run_id(context, pipeline_code)
    business_date = _business_date(context, control_plane)
    try:
        file_payload = control_plane.write_export_csv(pipeline_code, run_id, business_date)
        workflow_payload = control_plane.finalize_export(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=business_date,
            actor="dagster",
            notes="Validated dataset exported automatically after review handoff",
        )
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            step_name=context.asset_key.to_user_string(),
            stage="write_export_csv",
            business_date=business_date,
            error=error,
        )
        raise

    return MaterializeResult(
        metadata={**file_payload, **workflow_payload},
        check_results=[
            _export_rows_check(
                int(file_payload["export_row_count"]),
                str(file_payload["file_path"]),
            )
        ],
    )


@asset(
    key=SECURITY_MASTER_CSV_EXPORT_ASSET_KEY,
    deps=[dbt_asset_key("security_master", "security_master_final")],
    group_name="gold",
    kinds={"python", "csv", "filesystem"},
    description="Writes the validated security master daily export to a CSV artifact.",
    check_specs=[AssetCheckSpec("csv_written", asset=SECURITY_MASTER_CSV_EXPORT_ASSET_KEY)],
)
def security_master_csv_export(
    context,
    control_plane: ControlPlaneResource,
) -> MaterializeResult:
    return _write_validated_export_csv(
        context, control_plane, "security_master", "security_master"
    )


@asset(
    key=SHAREHOLDER_HOLDINGS_CSV_EXPORT_ASSET_KEY,
    deps=[dbt_asset_key("shareholder_holdings", "shareholder_holdings_final")],
    group_name="gold",
    kinds={"python", "csv", "filesystem"},
    description="Writes the validated shareholder holdings daily export to a CSV artifact.",
    check_specs=[
        AssetCheckSpec("csv_written", asset=SHAREHOLDER_HOLDINGS_CSV_EXPORT_ASSET_KEY)
    ],
)
def shareholder_holdings_csv_export(
    context,
    control_plane: ControlPlaneResource,
) -> MaterializeResult:
    return _write_validated_export_csv(
        context, control_plane, "shareholder_holdings", "shareholder_holdings"
    )


@asset(
    key=SECURITY_MASTER_REVIEW_ASSET_KEY,
    deps=[dbt_asset_key("security_master", "dim_security")],
    group_name="gold",
    kinds={"python", "postgres"},
    description="Publishes gold security master rows into review.security_master_daily.",
    check_specs=[
        AssetCheckSpec("review_rows_published", asset=SECURITY_MASTER_REVIEW_ASSET_KEY)
    ],
)
def security_master_review_snapshot(
    context,
    control_plane: ControlPlaneResource,
) -> MaterializeResult:
    return _publish_snapshot(context, control_plane, "security_master", "security_master")


@asset(
    key=SHAREHOLDER_HOLDINGS_REVIEW_ASSET_KEY,
    deps=[dbt_asset_key("shareholder_holdings", "fact_shareholder_holding")],
    group_name="gold",
    kinds={"python", "postgres"},
    description="Publishes gold shareholder holdings rows into review.shareholder_holdings_daily.",
    check_specs=[
        AssetCheckSpec(
            "review_rows_published", asset=SHAREHOLDER_HOLDINGS_REVIEW_ASSET_KEY
        )
    ],
)
def shareholder_holdings_review_snapshot(
    context,
    control_plane: ControlPlaneResource,
) -> MaterializeResult:
    return _publish_snapshot(
        context, control_plane, "shareholder_holdings", "shareholder_holdings"
    )
