from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from uuid import UUID, uuid4

from dagflow_source_adapters.registry import get_adapter
from dagster import AssetCheckResult, AssetExecutionContext, MaterializeResult
from dagster_dbt import DbtCliResource

from dagflow_dagster.dbt_topology import get_dbt_project
from dagflow_dagster.execution import export_step_key
from dagflow_dagster.resources import ControlPlaneResource

DBT_PROJECT = get_dbt_project()


def orchestrator_run_id(context: AssetExecutionContext) -> UUID:
    return UUID(context.run_id)


def pipeline_run_id(context: AssetExecutionContext, pipeline_code: str) -> UUID:
    tagged_run_id = context.run_tags.get("dagflow_run_id")
    tagged_pipeline_code = context.run_tags.get("dagflow_pipeline_code")
    if tagged_run_id and tagged_pipeline_code == pipeline_code:
        return UUID(tagged_run_id)
    return orchestrator_run_id(context)


def business_date(
    context: AssetExecutionContext, control_plane: ControlPlaneResource
) -> date:
    for tag_name in ("dagflow_business_date", "validated_business_date", "business_date"):
        tag_value = context.run_tags.get(tag_name)
        if tag_value:
            return date.fromisoformat(tag_value)
    return control_plane.current_business_date()


def rows_loaded_check(row_count: int) -> AssetCheckResult:
    return AssetCheckResult(
        check_name="rows_loaded",
        passed=row_count > 0,
        metadata={"row_count": row_count},
        description="Verifies that the ingestion asset loaded at least one row.",
    )


def review_rows_check(row_count: int) -> AssetCheckResult:
    return AssetCheckResult(
        check_name="review_rows_published",
        passed=row_count > 0,
        metadata={"review_row_count": row_count},
        description="Verifies that the review snapshot published rows for analysts.",
    )


def review_issue_scan_check(issue_count: int) -> AssetCheckResult:
    return AssetCheckResult(
        check_name="review_data_issues_scanned",
        passed=issue_count == 0,
        metadata={"issue_count": issue_count},
        description=(
            "Scans the published review snapshot for non-blocking bad-data rows and "
            "logs them into audit.review_data_issues."
        ),
    )


def export_rows_check(row_count: int, file_path: str) -> AssetCheckResult:
    return AssetCheckResult(
        check_name="csv_written",
        passed=row_count > 0,
        metadata={"export_row_count": row_count, "file_path": file_path},
        description="Verifies that the validated review run was written to a CSV export.",
    )


def dbt_vars(
    context: AssetExecutionContext,
    control_plane: ControlPlaneResource,
    pipeline_code: str,
) -> dict[str, str]:
    bdate = business_date(context, control_plane)
    payload: dict[str, str] = {
        "dagflow_run_id": str(pipeline_run_id(context, pipeline_code)),
        "dagflow_pipeline_code": pipeline_code,
        "dagflow_dataset_code": pipeline_code,
        "dagflow_business_date": bdate.isoformat(),
    }
    return payload


def dbt_target_path(context: AssetExecutionContext) -> Path:
    unique_id = uuid4().hex[:7]
    return Path(DBT_PROJECT.target_path).joinpath(
        f"{context.op_execution_context.op.name}-{context.run.run_id[:7]}-{unique_id}"
    )


def stream_dbt_build(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    dataset_code: str,
    stage: str,
) -> Iterator:
    bdate = business_date(context, control_plane)
    run_id = pipeline_run_id(context, pipeline_code)
    step_name = (
        f"{pipeline_code}_transform_assets"
        if stage == "dbt_build"
        else f"{pipeline_code}_export_assets"
    )
    command = ["build", "--vars", json.dumps(dbt_vars(context, control_plane, pipeline_code))]
    control_plane.start_pipeline_step(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=bdate,
        step_key=step_name,
        metadata={"stage": stage, "dbt_command": command},
    )
    target = dbt_target_path(context)
    try:
        yield from dbt.cli(command, context=context, target_path=target).stream()
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            step_name=step_name,
            stage=stage,
            business_date=bdate,
            error=error,
            diagnostic_metadata={"dbt_command": command},
        )
        raise
    control_plane.complete_pipeline_step(
        run_id=run_id,
        step_key=step_name,
        metadata={"stage": stage, "dbt_command": command},
    )


def asset_step_name(context: AssetExecutionContext) -> str:
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


def capture_source(
    context: AssetExecutionContext,
    control_plane: ControlPlaneResource,
    *,
    pipeline_code: str,
    dataset_code: str,
    source_name: str,
) -> MaterializeResult:
    bdate = business_date(context, control_plane)
    run_id = pipeline_run_id(context, pipeline_code)
    step_name_map = {
        "sec_company_tickers": "sec_company_tickers_capture",
        "sec_company_facts": "sec_company_facts_capture",
        "holdings_13f": "holdings_13f_capture",
        "holdings_13f_filers": "holdings_13f_filers_capture",
    }
    sname = step_name_map[source_name]
    descriptor = next(
        item
        for item in get_adapter(
            "sec_json" if source_name.startswith("sec_company") else "sec_13f"
        ).build_sources(bdate)
        if item.source_name == source_name
    )
    control_plane.ensure_pipeline_run(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=bdate,
        orchestrator_run_id=context.run_id,
    )
    control_plane.start_pipeline_step(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=bdate,
        step_key=sname,
        metadata={"source_name": source_name},
    )
    try:
        if source_name == "sec_company_facts":
            file_record = control_plane.capture_security_master_facts_from_landed_tickers(bdate)
        elif source_name == "holdings_13f_filers":
            file_record = control_plane.capture_shareholder_filers_from_landed_holdings(bdate)
        else:
            file_record = control_plane.capture_source_file(source_name, bdate)
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            step_name=sname,
            stage="source_capture",
            business_date=bdate,
            error=error,
            diagnostic_metadata={"source_name": source_name},
        )
        raise
    record_meta = file_record.get("metadata") or {}
    metadata = {
        "run_id": str(run_id),
        "business_date": bdate.isoformat(),
        "source_name": source_name,
        "source_file_id": str(file_record["source_file_id"]),
        "storage_path": str(file_record["storage_path"]),
        "object_key": str(file_record["object_key"]),
        "source_url": descriptor.source_url,
        "row_count": int(file_record["row_count"]),
        "derived_from_source_name": record_meta.get("derived_from_source_name"),
        "derived_from_source_file_id": record_meta.get("derived_from_source_file_id"),
        "dagflow_layer": "source_capture",
        "dagflow_tool": "python",
        "dagflow_database": "filesystem",
    }
    control_plane.complete_pipeline_step(run_id=run_id, step_key=sname, metadata=metadata)
    return MaterializeResult(
        metadata=metadata,
        check_results=[rows_loaded_check(int(file_record["row_count"]))],
    )


def publish_snapshot(
    context: AssetExecutionContext,
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    dataset_code: str,
) -> MaterializeResult:
    run_id = pipeline_run_id(context, pipeline_code)
    bdate = business_date(context, control_plane)
    sname = f"{pipeline_code}_review_snapshot"
    control_plane.start_pipeline_step(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=bdate,
        step_key=sname,
        metadata={"stage": "publish_review_snapshot"},
    )
    try:
        payload = control_plane.publish_review_snapshot(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=bdate,
            actor="dagster",
            notes="Review snapshot published by Dagster live SEC pipeline",
        )
        review_row_count = control_plane.review_row_count(dataset_code, run_id)
        issue_payload = control_plane.scan_review_data_issues(
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=bdate,
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
            step_name=sname,
            stage="publish_review_snapshot",
            business_date=bdate,
            error=error,
        )
        raise
    meta = {**payload, **issue_payload, "review_row_count": review_row_count}
    control_plane.complete_pipeline_step(run_id=run_id, step_key=sname, metadata=meta)
    return MaterializeResult(
        metadata=meta,
        check_results=[
            review_rows_check(review_row_count),
            review_issue_scan_check(int(issue_payload["issue_count"])),
        ],
    )


def write_validated_export_csv(
    context: AssetExecutionContext,
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    dataset_code: str,
) -> MaterializeResult:
    run_id = pipeline_run_id(context, pipeline_code)
    bdate = business_date(context, control_plane)
    sname = export_step_key(pipeline_code)
    control_plane.start_pipeline_step(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=bdate,
        step_key=sname,
        metadata={"stage": "validated_export"},
        mark_run_running=False,
    )
    try:
        file_payload = control_plane.write_export_bundle(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=bdate,
        )
        workflow_payload = control_plane.finalize_export(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=bdate,
            actor="dagster",
            notes="Validated dataset exported automatically after review handoff",
        )
        export_meta = {**file_payload, **workflow_payload}
        control_plane.complete_pipeline_step(
            run_id=run_id, step_key=sname, metadata=export_meta
        )
        registry_payload = control_plane.record_export_bundle_registry(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=bdate,
            artifact_payload=export_meta,
        )
    except Exception as error:
        control_plane.capture_failure(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            step_name=sname,
            stage="write_export_csv",
            business_date=bdate,
            error=error,
        )
        raise
    meta = {**file_payload, **workflow_payload, "export_registry": registry_payload}
    return MaterializeResult(
        metadata=meta,
        check_results=[
            export_rows_check(
                int(file_payload["export_row_count"]),
                str(file_payload["reviewed_file_path"]),
            )
        ],
    )
