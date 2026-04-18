from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from uuid import UUID, uuid4

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
    asset_description,
    asset_metadata,
    dbt_asset_key,
    get_dbt_project,
    manual_asset_key,
    manual_asset_narrative,
)
from dagflow_dagster.execution import export_step_key
from dagflow_dagster.resources import ControlPlaneResource

DBT_PROJECT = get_dbt_project()
DAGFLOW_DBT_TRANSLATOR = DagflowDbtTranslator()
SECURITY_MASTER_CSV_EXPORT_ASSET_KEY = manual_asset_key("security_master_export")
SECURITY_MASTER_TICKERS_CAPTURE_ASSET_KEY = manual_asset_key(
    "security_master_ticker_capture"
)
SECURITY_MASTER_FACTS_CAPTURE_ASSET_KEY = manual_asset_key(
    "security_master_facts_capture"
)
SHAREHOLDER_HOLDINGS_CSV_EXPORT_ASSET_KEY = manual_asset_key(
    "shareholder_holdings_export"
)
SHAREHOLDER_HOLDINGS_CAPTURE_ASSET_KEY = manual_asset_key(
    "shareholder_holdings_capture"
)
SHAREHOLDER_FILERS_CAPTURE_ASSET_KEY = manual_asset_key(
    "shareholder_filers_capture"
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


def _review_issue_scan_check(issue_count: int) -> AssetCheckResult:
    return AssetCheckResult(
        check_name="review_data_issues_scanned",
        passed=issue_count == 0,
        metadata={"issue_count": issue_count},
        description=(
            "Scans the published review snapshot for non-blocking bad-data rows and "
            "logs them into audit.review_data_issues."
        ),
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
        "dagflow_dataset_code": pipeline_code,
        "dagflow_business_date": business_date.isoformat(),
    }
    if pipeline_code == "shareholder_holdings":
        security_run_id = control_plane.last_pipeline_run_id("security_master")
        if security_run_id is not None:
            vars_payload["dagflow_security_run_id"] = str(security_run_id)
    return vars_payload


def _dbt_target_path(context: AssetExecutionContext) -> Path:
    unique_id = uuid4().hex[:7]
    return Path(DBT_PROJECT.target_path).joinpath(
        f"{context.op_execution_context.op.name}-{context.run.run_id[:7]}-{unique_id}"
    )


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
    step_name = (
        f"{pipeline_code}_transform_assets"
        if stage == "dbt_build"
        else f"{pipeline_code}_export_assets"
    )
    command = [
        "build",
        "--vars",
        json.dumps(_dbt_vars(context, control_plane, pipeline_code)),
    ]
    control_plane.start_pipeline_step(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=pipeline_run_id,
        business_date=business_date,
        step_key=step_name,
        metadata={"stage": stage, "dbt_command": command},
    )
    target_path = _dbt_target_path(context)
    try:
        yield from dbt.cli(command, context=context, target_path=target_path).stream()
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=pipeline_run_id,
            step_name=step_name,
            stage=stage,
            business_date=business_date,
            error=error,
            diagnostic_metadata={"dbt_command": command},
        )
        raise
    control_plane.complete_pipeline_step(
        run_id=pipeline_run_id,
        step_key=step_name,
        metadata={"stage": stage, "dbt_command": command},
    )


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
    target_path = _dbt_target_path(context)
    try:
        dbt.cli(command, context=context, target_path=target_path).wait()
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=pipeline_run_id,
            step_name=_step_name(context),
            stage="dbt_snapshot",
            business_date=business_date,
            error=error,
            diagnostic_metadata={"dbt_command": command, "snapshot_name": snapshot_name},
        )
        raise
    return iter(())


def _publish_snapshot(
    context: AssetExecutionContext,
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    dataset_code: str,
) -> MaterializeResult:
    run_id = _pipeline_run_id(context, pipeline_code)
    business_date = _business_date(context, control_plane)
    step_name = f"{pipeline_code}_review_snapshot"
    control_plane.start_pipeline_step(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=business_date,
        step_key=step_name,
        metadata={"stage": "publish_review_snapshot"},
    )
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
        issue_payload = control_plane.scan_review_data_issues(
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=business_date,
            actor="dagster",
        )
        control_plane.complete_pipeline_run(
            run_id=run_id,
            status="pending_review",
            metadata={
                "review_state": payload["review_state"],
                "review_issue_count": issue_payload["issue_count"],
            },
        )
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            step_name=step_name,
            stage="publish_review_snapshot",
            business_date=business_date,
            error=error,
        )
        raise
    metadata = {
        **payload,
        **issue_payload,
        "review_row_count": review_row_count,
    }
    control_plane.complete_pipeline_step(
        run_id=run_id,
        step_key=step_name,
        metadata=metadata,
    )
    return MaterializeResult(
        metadata=metadata,
        check_results=[
            _review_rows_check(review_row_count),
            _review_issue_scan_check(int(issue_payload["issue_count"])),
        ],
    )


def _capture_source(
    context: AssetExecutionContext,
    control_plane: ControlPlaneResource,
    *,
    pipeline_code: str,
    dataset_code: str,
    source_name: str,
) -> MaterializeResult:
    business_date = _business_date(context, control_plane)
    run_id = _pipeline_run_id(context, pipeline_code)
    step_name = {
        "sec_company_tickers": "sec_company_tickers_capture",
        "sec_company_facts": "sec_company_facts_capture",
        "holdings_13f": "holdings_13f_capture",
        "holdings_13f_filers": "holdings_13f_filers_capture",
    }[source_name]
    descriptor = next(
        item
        for item in get_adapter(
            "sec_json" if pipeline_code == "security_master" else "sec_13f"
        ).build_sources(business_date)
        if item.source_name == source_name
    )
    control_plane.ensure_pipeline_run(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=business_date,
        orchestrator_run_id=context.run_id,
    )
    control_plane.start_pipeline_step(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=business_date,
        step_key=step_name,
        metadata={"source_name": source_name},
    )
    try:
        if source_name == "sec_company_facts":
            file_record = control_plane.capture_security_master_facts_from_landed_tickers(
                business_date
            )
        elif source_name == "holdings_13f_filers":
            file_record = control_plane.capture_shareholder_filers_from_landed_holdings(
                business_date
            )
        else:
            file_record = control_plane.capture_source_file(source_name, business_date)
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            step_name=step_name,
            stage="source_capture",
            business_date=business_date,
            error=error,
            diagnostic_metadata={"source_name": source_name},
        )
        raise
    record_metadata = file_record.get("metadata") or {}
    metadata = {
        "run_id": str(run_id),
        "business_date": business_date.isoformat(),
        "source_name": source_name,
        "source_file_id": str(file_record["source_file_id"]),
        "storage_path": str(file_record["storage_path"]),
        "object_key": str(file_record["object_key"]),
        "source_url": descriptor.source_url,
        "row_count": int(file_record["row_count"]),
        "derived_from_source_name": record_metadata.get("derived_from_source_name"),
        "derived_from_source_file_id": record_metadata.get("derived_from_source_file_id"),
        "dagflow_layer": "source_capture",
        "dagflow_tool": "python",
        "dagflow_database": "filesystem",
    }
    control_plane.complete_pipeline_step(
        run_id=run_id,
        step_key=step_name,
        metadata=metadata,
    )
    return MaterializeResult(
        metadata=metadata,
        check_results=[_rows_loaded_check(int(file_record["row_count"]))],
    )


def _step_name(context: AssetExecutionContext) -> str:
    op_name = getattr(getattr(context, "op", None), "name", None)
    if isinstance(op_name, str) and op_name:
        return op_name
    op_def_name = getattr(getattr(context, "op_def", None), "name", None)
    if isinstance(op_def_name, str) and op_def_name:
        return op_def_name
    try:
        return context.asset_key.to_user_string()
    except Exception:
        return "dagster_asset_step"


@asset(
    key=SECURITY_MASTER_TICKERS_CAPTURE_ASSET_KEY,
    group_name="source_capture",
    kinds={"python", "csv", "filesystem", "edgar"},
    description=asset_description(
        manual_asset_narrative("security_master_ticker_capture"),
        layer="source_capture",
        tool="python",
        database="filesystem",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("security_master_ticker_capture"),
        pipeline_code="security_master",
        layer="source_capture",
        tool="python",
        database="filesystem",
    ),
    check_specs=[
        AssetCheckSpec("rows_loaded", asset=SECURITY_MASTER_TICKERS_CAPTURE_ASSET_KEY)
    ],
)
def sec_company_tickers_capture(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    return _capture_source(
        context,
        control_plane,
        pipeline_code="security_master",
        dataset_code="security_master",
        source_name="sec_company_tickers",
    )


@asset(
    key=SECURITY_MASTER_TICKERS_ASSET_KEY,
    deps=[SECURITY_MASTER_TICKERS_CAPTURE_ASSET_KEY],
    group_name="contract_load",
    kinds={"python", "postgres"},
    description=asset_description(
        manual_asset_narrative("security_master_ticker_contract"),
        layer="contract_load",
        tool="python",
        database="postgres",
        relation="raw.sec_company_tickers",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("security_master_ticker_contract"),
        pipeline_code="security_master",
        layer="contract_load",
        tool="python",
        database="postgres",
        relation="raw.sec_company_tickers",
    ),
    check_specs=[AssetCheckSpec("rows_loaded", asset=SECURITY_MASTER_TICKERS_ASSET_KEY)],
)
def sec_company_tickers_raw(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    business_date = _business_date(context, control_plane)
    run_id = _pipeline_run_id(context, "security_master")
    step_name = "sec_company_tickers_raw"
    control_plane.start_pipeline_step(
        pipeline_code="security_master",
        dataset_code="security_master",
        run_id=run_id,
        business_date=business_date,
        step_key=step_name,
        metadata={"stage": "contract_load", "source_name": "sec_company_tickers"},
    )
    descriptor = get_adapter("sec_json").build_sources(business_date)[0]
    try:
        source_file = control_plane.latest_source_file("sec_company_tickers", business_date)
        inserted_rows = control_plane.load_security_master_tickers(run_id, business_date)
        context.log.info("Loaded landed SEC ticker rows into raw.sec_company_tickers")
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code="security_master",
            dataset_code="security_master",
            run_id=run_id,
            step_name=step_name,
            stage="contract_load",
            business_date=business_date,
            error=error,
            diagnostic_metadata={"source_name": "sec_company_tickers"},
        )
        raise
    metadata = {
        "run_id": str(run_id),
        "business_date": business_date.isoformat(),
        "landing_table": descriptor.landing_table,
        "source_url": descriptor.source_url,
        "source_file_id": str(source_file["source_file_id"]),
        "storage_path": str(source_file["storage_path"]),
        "inserted_rows": inserted_rows,
        "dagflow_layer": "contract_load",
        "dagflow_tool": "python",
        "dagflow_database": "postgres",
    }
    control_plane.complete_pipeline_step(run_id=run_id, step_key=step_name, metadata=metadata)
    return MaterializeResult(
        metadata=metadata,
        check_results=[_rows_loaded_check(inserted_rows)],
    )


@asset(
    key=SECURITY_MASTER_FACTS_CAPTURE_ASSET_KEY,
    deps=[SECURITY_MASTER_TICKERS_CAPTURE_ASSET_KEY],
    group_name="source_capture",
    kinds={"python", "csv", "filesystem", "edgar"},
    description=asset_description(
        manual_asset_narrative("security_master_facts_capture"),
        layer="source_capture",
        tool="python",
        database="filesystem",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("security_master_facts_capture"),
        pipeline_code="security_master",
        layer="source_capture",
        tool="python",
        database="filesystem",
    ),
    check_specs=[AssetCheckSpec("rows_loaded", asset=SECURITY_MASTER_FACTS_CAPTURE_ASSET_KEY)],
)
def sec_company_facts_capture(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    return _capture_source(
        context,
        control_plane,
        pipeline_code="security_master",
        dataset_code="security_master",
        source_name="sec_company_facts",
    )


@asset(
    key=SECURITY_MASTER_FACTS_ASSET_KEY,
    deps=[SECURITY_MASTER_FACTS_CAPTURE_ASSET_KEY],
    group_name="contract_load",
    kinds={"python", "postgres"},
    description=asset_description(
        manual_asset_narrative("security_master_facts_contract"),
        layer="contract_load",
        tool="python",
        database="postgres",
        relation="raw.sec_company_facts",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("security_master_facts_contract"),
        pipeline_code="security_master",
        layer="contract_load",
        tool="python",
        database="postgres",
        relation="raw.sec_company_facts",
    ),
    check_specs=[AssetCheckSpec("rows_loaded", asset=SECURITY_MASTER_FACTS_ASSET_KEY)],
)
def sec_company_facts_raw(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    business_date = _business_date(context, control_plane)
    run_id = _pipeline_run_id(context, "security_master")
    step_name = "sec_company_facts_raw"
    control_plane.start_pipeline_step(
        pipeline_code="security_master",
        dataset_code="security_master",
        run_id=run_id,
        business_date=business_date,
        step_key=step_name,
        metadata={"stage": "contract_load", "source_name": "sec_company_facts"},
    )
    descriptor = get_adapter("sec_json").build_sources(business_date)[1]
    try:
        source_file = control_plane.latest_source_file("sec_company_facts", business_date)
        inserted_rows = control_plane.load_security_master_facts(run_id, business_date)
        context.log.info("Loaded landed SEC facts rows into raw.sec_company_facts")
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code="security_master",
            dataset_code="security_master",
            run_id=run_id,
            step_name=step_name,
            stage="contract_load",
            business_date=business_date,
            error=error,
            diagnostic_metadata={"source_name": "sec_company_facts"},
        )
        raise
    metadata = {
        "run_id": str(run_id),
        "business_date": business_date.isoformat(),
        "landing_table": descriptor.landing_table,
        "source_url": descriptor.source_url,
        "source_file_id": str(source_file["source_file_id"]),
        "storage_path": str(source_file["storage_path"]),
        "inserted_rows": inserted_rows,
        "dagflow_layer": "contract_load",
        "dagflow_tool": "python",
        "dagflow_database": "postgres",
    }
    control_plane.complete_pipeline_step(run_id=run_id, step_key=step_name, metadata=metadata)
    return MaterializeResult(
        metadata=metadata,
        check_results=[_rows_loaded_check(inserted_rows)],
    )


@asset(
    key=SHAREHOLDER_HOLDINGS_CAPTURE_ASSET_KEY,
    group_name="source_capture",
    kinds={"python", "csv", "filesystem", "edgar"},
    description=asset_description(
        manual_asset_narrative("shareholder_holdings_capture"),
        layer="source_capture",
        tool="python",
        database="filesystem",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("shareholder_holdings_capture"),
        pipeline_code="shareholder_holdings",
        layer="source_capture",
        tool="python",
        database="filesystem",
    ),
    check_specs=[AssetCheckSpec("rows_loaded", asset=SHAREHOLDER_HOLDINGS_CAPTURE_ASSET_KEY)],
)
def holdings_13f_capture(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    return _capture_source(
        context,
        control_plane,
        pipeline_code="shareholder_holdings",
        dataset_code="shareholder_holdings",
        source_name="holdings_13f",
    )


@asset(
    key=SHAREHOLDER_HOLDINGS_ASSET_KEY,
    deps=[SHAREHOLDER_HOLDINGS_CAPTURE_ASSET_KEY],
    group_name="contract_load",
    kinds={"python", "postgres"},
    description=asset_description(
        manual_asset_narrative("shareholder_holdings_contract"),
        layer="contract_load",
        tool="python",
        database="postgres",
        relation="raw.holdings_13f",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("shareholder_holdings_contract"),
        pipeline_code="shareholder_holdings",
        layer="contract_load",
        tool="python",
        database="postgres",
        relation="raw.holdings_13f",
    ),
    check_specs=[AssetCheckSpec("rows_loaded", asset=SHAREHOLDER_HOLDINGS_ASSET_KEY)],
)
def holdings_13f_raw(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    business_date = _business_date(context, control_plane)
    run_id = _pipeline_run_id(context, "shareholder_holdings")
    step_name = "holdings_13f_raw"
    control_plane.start_pipeline_step(
        pipeline_code="shareholder_holdings",
        dataset_code="shareholder_holdings",
        run_id=run_id,
        business_date=business_date,
        step_key=step_name,
        metadata={"stage": "contract_load", "source_name": "holdings_13f"},
    )
    descriptor = get_adapter("sec_13f").build_sources(business_date)[0]
    try:
        source_file = control_plane.latest_source_file("holdings_13f", business_date)
        inserted_rows = control_plane.load_shareholder_holdings(run_id, business_date)
        context.log.info("Loaded landed 13F holdings rows into raw.holdings_13f")
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code="shareholder_holdings",
            dataset_code="shareholder_holdings",
            run_id=run_id,
            step_name=step_name,
            stage="contract_load",
            business_date=business_date,
            error=error,
            diagnostic_metadata={"source_name": "holdings_13f"},
        )
        raise
    metadata = {
        "run_id": str(run_id),
        "business_date": business_date.isoformat(),
        "landing_table": descriptor.landing_table,
        "source_url": descriptor.source_url,
        "source_file_id": str(source_file["source_file_id"]),
        "storage_path": str(source_file["storage_path"]),
        "inserted_rows": inserted_rows,
        "dagflow_layer": "contract_load",
        "dagflow_tool": "python",
        "dagflow_database": "postgres",
    }
    control_plane.complete_pipeline_step(run_id=run_id, step_key=step_name, metadata=metadata)
    return MaterializeResult(
        metadata=metadata,
        check_results=[_rows_loaded_check(inserted_rows)],
    )


@asset(
    key=SHAREHOLDER_FILERS_CAPTURE_ASSET_KEY,
    deps=[SHAREHOLDER_HOLDINGS_CAPTURE_ASSET_KEY],
    group_name="source_capture",
    kinds={"python", "csv", "filesystem", "edgar"},
    description=asset_description(
        manual_asset_narrative("shareholder_filers_capture"),
        layer="source_capture",
        tool="python",
        database="filesystem",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("shareholder_filers_capture"),
        pipeline_code="shareholder_holdings",
        layer="source_capture",
        tool="python",
        database="filesystem",
    ),
    check_specs=[AssetCheckSpec("rows_loaded", asset=SHAREHOLDER_FILERS_CAPTURE_ASSET_KEY)],
)
def holdings_13f_filers_capture(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    return _capture_source(
        context,
        control_plane,
        pipeline_code="shareholder_holdings",
        dataset_code="shareholder_holdings",
        source_name="holdings_13f_filers",
    )


@asset(
    key=SHAREHOLDER_FILERS_ASSET_KEY,
    deps=[SHAREHOLDER_FILERS_CAPTURE_ASSET_KEY],
    group_name="contract_load",
    kinds={"python", "postgres"},
    description=asset_description(
        manual_asset_narrative("shareholder_filers_contract"),
        layer="contract_load",
        tool="python",
        database="postgres",
        relation="raw.holdings_13f_filers",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("shareholder_filers_contract"),
        pipeline_code="shareholder_holdings",
        layer="contract_load",
        tool="python",
        database="postgres",
        relation="raw.holdings_13f_filers",
    ),
    check_specs=[AssetCheckSpec("rows_loaded", asset=SHAREHOLDER_FILERS_ASSET_KEY)],
)
def holdings_13f_filers_raw(
    context, control_plane: ControlPlaneResource
) -> MaterializeResult:
    business_date = _business_date(context, control_plane)
    run_id = _pipeline_run_id(context, "shareholder_holdings")
    step_name = "holdings_13f_filers_raw"
    control_plane.start_pipeline_step(
        pipeline_code="shareholder_holdings",
        dataset_code="shareholder_holdings",
        run_id=run_id,
        business_date=business_date,
        step_key=step_name,
        metadata={"stage": "contract_load", "source_name": "holdings_13f_filers"},
    )
    descriptor = get_adapter("sec_13f").build_sources(business_date)[1]
    try:
        source_file = control_plane.latest_source_file("holdings_13f_filers", business_date)
        inserted_rows = control_plane.load_shareholder_filers(run_id, business_date)
        context.log.info("Loaded landed 13F filer rows into raw.holdings_13f_filers")
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code="shareholder_holdings",
            dataset_code="shareholder_holdings",
            run_id=run_id,
            step_name=step_name,
            stage="contract_load",
            business_date=business_date,
            error=error,
            diagnostic_metadata={"source_name": "holdings_13f_filers"},
        )
        raise
    metadata = {
        "run_id": str(run_id),
        "business_date": business_date.isoformat(),
        "landing_table": descriptor.landing_table,
        "source_url": descriptor.source_url,
        "source_file_id": str(source_file["source_file_id"]),
        "storage_path": str(source_file["storage_path"]),
        "inserted_rows": inserted_rows,
        "dagflow_layer": "contract_load",
        "dagflow_tool": "python",
        "dagflow_database": "postgres",
    }
    control_plane.complete_pipeline_step(run_id=run_id, step_key=step_name, metadata=metadata)
    return MaterializeResult(
        metadata=metadata,
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
    step_name = export_step_key(pipeline_code)
    control_plane.start_pipeline_step(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=business_date,
        step_key=step_name,
        metadata={"stage": "validated_export"},
        mark_run_running=False,
    )
    try:
        file_payload = control_plane.write_export_bundle(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=business_date,
        )
        workflow_payload = control_plane.finalize_export(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=business_date,
            actor="dagster",
            notes="Validated dataset exported automatically after review handoff",
        )
        export_metadata = {**file_payload, **workflow_payload}
        control_plane.complete_pipeline_step(
            run_id=run_id,
            step_key=step_name,
            metadata=export_metadata,
        )
        export_registry_payload = control_plane.record_export_bundle_registry(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=business_date,
            artifact_payload=export_metadata,
        )
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            step_name=step_name,
            stage="write_export_csv",
            business_date=business_date,
            error=error,
        )
        raise
    metadata = {
        **file_payload,
        **workflow_payload,
        "export_registry": export_registry_payload,
    }
    return MaterializeResult(
        metadata=metadata,
        check_results=[
            _export_rows_check(
                int(file_payload["export_row_count"]),
                str(file_payload["reviewed_file_path"]),
            )
        ],
    )


@asset(
    key=SECURITY_MASTER_CSV_EXPORT_ASSET_KEY,
    deps=[dbt_asset_key("security_master", "security_master_final")],
    group_name="validated_export",
    kinds={"python", "csv", "filesystem"},
    description=asset_description(
        manual_asset_narrative("security_master_export"),
        layer="validated_export",
        tool="python",
        database="filesystem",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("security_master_export"),
        pipeline_code="security_master",
        layer="validated_export",
        tool="python",
        database="filesystem",
    ),
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
    group_name="validated_export",
    kinds={"python", "csv", "filesystem"},
    description=asset_description(
        manual_asset_narrative("shareholder_holdings_export"),
        layer="validated_export",
        tool="python",
        database="filesystem",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("shareholder_holdings_export"),
        pipeline_code="shareholder_holdings",
        layer="validated_export",
        tool="python",
        database="filesystem",
    ),
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
    group_name="review",
    kinds={"python", "postgres"},
    description=asset_description(
        manual_asset_narrative("security_master_review"),
        layer="review",
        tool="python",
        database="postgres",
        relation="review.security_master_daily",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("security_master_review"),
        pipeline_code="security_master",
        layer="review",
        tool="python",
        database="postgres",
        relation="review.security_master_daily",
    ),
    check_specs=[
        AssetCheckSpec("review_rows_published", asset=SECURITY_MASTER_REVIEW_ASSET_KEY),
        AssetCheckSpec("review_data_issues_scanned", asset=SECURITY_MASTER_REVIEW_ASSET_KEY),
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
    group_name="review",
    kinds={"python", "postgres"},
    description=asset_description(
        manual_asset_narrative("shareholder_holdings_review"),
        layer="review",
        tool="python",
        database="postgres",
        relation="review.shareholder_holdings_daily",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("shareholder_holdings_review"),
        pipeline_code="shareholder_holdings",
        layer="review",
        tool="python",
        database="postgres",
        relation="review.shareholder_holdings_daily",
    ),
    check_specs=[
        AssetCheckSpec("review_rows_published", asset=SHAREHOLDER_HOLDINGS_REVIEW_ASSET_KEY),
        AssetCheckSpec(
            "review_data_issues_scanned",
            asset=SHAREHOLDER_HOLDINGS_REVIEW_ASSET_KEY,
        ),
    ],
)
def shareholder_holdings_review_snapshot(
    context,
    control_plane: ControlPlaneResource,
) -> MaterializeResult:
    return _publish_snapshot(
        context, control_plane, "shareholder_holdings", "shareholder_holdings"
    )
