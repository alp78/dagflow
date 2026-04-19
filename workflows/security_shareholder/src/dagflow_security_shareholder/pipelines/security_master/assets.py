from collections.abc import Iterator

from dagster import AssetCheckSpec, AssetExecutionContext, AssetKey, AssetOut, AssetSpec, MaterializeResult, Output, asset, multi_asset
from dagster_dbt import DbtCliResource, dbt_assets

from dagflow_dagster.asset_helpers import (
    DBT_PROJECT,
    business_date,
    capture_source,
    pipeline_run_id,
    publish_snapshot,
    review_issue_scan_check,
    review_rows_check,
    rows_loaded_check,
    stream_dbt_build,
    write_validated_export_csv,
)
from dagflow_dagster.dbt_topology import asset_description, asset_metadata
from dagflow_dagster.resources import ControlPlaneResource
from dagflow_security_shareholder.topology import (
    DAGFLOW_DBT_TRANSLATOR,
    dbt_asset_key,
    manual_asset_key,
    manual_asset_narrative,
)

# ── Manual asset keys ──────────────────────────────────────────────────────────
CAPTURE_SOURCES_ASSET_KEY = manual_asset_key("source_capture")
LOAD_TO_RAW_ASSET_KEY = manual_asset_key("contract_load")
REVIEW_SNAPSHOT_ASSET_KEY = manual_asset_key("review_snapshot")
CSV_EXPORT_ASSET_KEY = manual_asset_key("csv_export")


# ── Step 1: Capture Sources ────────────────────────────────────────────────────

@asset(
    key=CAPTURE_SOURCES_ASSET_KEY,
    group_name="source_capture",
    kinds={"python", "csv", "filesystem", "edgar"},
    description=asset_description(
        manual_asset_narrative("source_capture"),
        layer="source_capture",
        tool="python",
        database="filesystem",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("source_capture"),
        pipeline_code="security_shareholder",
        layer="source_capture",
        tool="python",
        database="filesystem",
    ),
    check_specs=[AssetCheckSpec("rows_loaded", asset=CAPTURE_SOURCES_ASSET_KEY)],
)
def capture_sources(
    context: AssetExecutionContext, control_plane: ControlPlaneResource
) -> MaterializeResult:
    bdate = business_date(context, control_plane)
    run_id = pipeline_run_id(context, "security_shareholder")
    control_plane.ensure_pipeline_run(
        pipeline_code="security_shareholder", dataset_code="security_master",
        run_id=run_id, business_date=bdate, orchestrator_run_id=context.run_id,
    )
    total_rows = 0

    step_name_map = {
        "sec_company_tickers": "sec_company_tickers_capture",
        "sec_company_facts": "sec_company_facts_capture",
        "holdings_13f": "holdings_13f_capture",
        "holdings_13f_filers": "holdings_13f_filers_capture",
    }
    dataset_map = {
        "sec_company_tickers": "security_master",
        "sec_company_facts": "security_master",
        "holdings_13f": "shareholder_holdings",
        "holdings_13f_filers": "shareholder_holdings",
    }

    for source_name in ("sec_company_tickers", "sec_company_facts", "holdings_13f", "holdings_13f_filers"):
        sname = step_name_map[source_name]
        dcode = dataset_map[source_name]
        control_plane.start_pipeline_step(
            pipeline_code="security_shareholder", dataset_code=dcode,
            run_id=run_id, business_date=bdate, step_key=sname,
            metadata={"source_name": source_name},
        )
        record = control_plane.capture_source_file(source_name, bdate)
        row_count = int(record.get("row_count", 0))
        total_rows += row_count
        record_meta = record.get("metadata") or {}
        step_metadata = {
            "source_name": source_name,
            "source_file_id": str(record.get("source_file_id", "")),
            "storage_path": str(record.get("storage_path", "")),
            "object_key": str(record.get("object_key", "")),
            "row_count": row_count,
            "file_size_bytes": record.get("file_size_bytes"),
            "content_checksum": record.get("content_checksum"),
            "source_format": record.get("source_format"),
            "captured_at": str(record.get("captured_at", "")),
            "derived_from": record_meta.get("derived_from_source_name"),
        }
        control_plane.complete_pipeline_step(run_id=run_id, step_key=sname, metadata=step_metadata)
        context.log.info("Captured %s: %d rows", source_name, row_count)

    return MaterializeResult(
        metadata={"sources_captured": 4, "total_rows": total_rows},
        check_results=[rows_loaded_check(total_rows)],
    )


# ── Step 2: Load to Raw ────────────────────────────────────────────────────────

RAW_TICKERS_KEY = AssetKey(["security_shareholder__raw_tickers"])
RAW_FACTS_KEY = AssetKey(["security_shareholder__raw_facts"])
RAW_HOLDINGS_KEY = AssetKey(["security_shareholder__raw_holdings"])
RAW_FILERS_KEY = AssetKey(["security_shareholder__raw_filers"])


_LOAD_KINDS = {"dagster/kind/python": "", "dagster/kind/postgres": ""}

@multi_asset(
    outs={
        "raw_tickers": AssetOut(key=RAW_TICKERS_KEY, group_name="contract_load", is_required=False, tags=_LOAD_KINDS),
        "raw_facts": AssetOut(key=RAW_FACTS_KEY, group_name="contract_load", is_required=False, tags=_LOAD_KINDS),
        "raw_holdings": AssetOut(key=RAW_HOLDINGS_KEY, group_name="contract_load", is_required=False, tags=_LOAD_KINDS),
        "raw_filers": AssetOut(key=RAW_FILERS_KEY, group_name="contract_load", is_required=False, tags=_LOAD_KINDS),
    },
    deps=[CAPTURE_SOURCES_ASSET_KEY],
    name="load_to_raw",
)
def load_to_raw(
    context: AssetExecutionContext, control_plane: ControlPlaneResource
):
    bdate = business_date(context, control_plane)
    run_id = pipeline_run_id(context, "security_shareholder")

    tickers_rows = control_plane.load_security_master_tickers(run_id, bdate)
    context.log.info("Loaded %d rows into raw.sec_company_tickers", tickers_rows)
    yield Output(value=None, output_name="raw_tickers", metadata={"rows": tickers_rows})

    facts_rows = control_plane.load_security_master_facts(run_id, bdate)
    context.log.info("Loaded %d rows into raw.sec_company_facts", facts_rows)
    yield Output(value=None, output_name="raw_facts", metadata={"rows": facts_rows})

    holdings_rows = control_plane.load_shareholder_holdings(run_id, bdate)
    context.log.info("Loaded %d rows into raw.holdings_13f", holdings_rows)
    yield Output(value=None, output_name="raw_holdings", metadata={"rows": holdings_rows})

    filers_rows = control_plane.load_shareholder_filers(run_id, bdate)
    context.log.info("Loaded %d rows into raw.holdings_13f_filers", filers_rows)
    yield Output(value=None, output_name="raw_filers", metadata={"rows": filers_rows})


# ── Step 3: Transform (dbt) — non-export models ────────────────────────────────

@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    project=DBT_PROJECT,
    name="security_shareholder_transform_assets",
    select="tag:security_shareholder",
    exclude="tag:exports",
    dagster_dbt_translator=DAGFLOW_DBT_TRANSLATOR,
)
def transform_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    control_plane: ControlPlaneResource,
) -> Iterator:
    yield from stream_dbt_build(
        context, dbt, control_plane, "security_shareholder", "security_master", "dbt_build"
    )


# ── Step 4: Publish Review ─────────────────────────────────────────────────────

@asset(
    key=REVIEW_SNAPSHOT_ASSET_KEY,
    deps=[
        dbt_asset_key("security_shareholder", "securities"),
        dbt_asset_key("security_shareholder", "holdings"),
    ],
    group_name="review",
    kinds={"python", "postgres"},
    description=asset_description(
        manual_asset_narrative("review_snapshot"),
        layer="review",
        tool="python",
        database="postgres",
        relation="review.security_master_daily / review.shareholder_holdings_daily",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("review_snapshot"),
        pipeline_code="security_shareholder",
        layer="review",
        tool="python",
        database="postgres",
        relation="review.security_master_daily / review.shareholder_holdings_daily",
    ),
    check_specs=[
        AssetCheckSpec("review_rows_published", asset=REVIEW_SNAPSHOT_ASSET_KEY),
        AssetCheckSpec("review_data_issues_scanned", asset=REVIEW_SNAPSHOT_ASSET_KEY),
    ],
)
def review_snapshot(
    context: AssetExecutionContext, control_plane: ControlPlaneResource
) -> MaterializeResult:
    run_id = pipeline_run_id(context, "security_shareholder")
    bdate = business_date(context, control_plane)

    total_issues = 0
    for dataset_code in ("security_master", "shareholder_holdings"):
        sname = f"security_shareholder_{dataset_code}_review"
        control_plane.start_pipeline_step(
            pipeline_code="security_shareholder", dataset_code=dataset_code,
            run_id=run_id, business_date=bdate, step_key=sname,
            metadata={"stage": "publish_review_snapshot"},
        )
        payload = control_plane.publish_review_snapshot(
            pipeline_code="security_shareholder", dataset_code=dataset_code,
            run_id=run_id, business_date=bdate, actor="dagster",
            notes="Review snapshot published by pipeline",
        )
        row_count = control_plane.review_row_count(dataset_code, run_id)
        issue_payload = control_plane.scan_review_data_issues(
            dataset_code=dataset_code, run_id=run_id, business_date=bdate, actor="dagster",
        )
        total_issues += int(issue_payload.get("issue_count", 0))
        control_plane.complete_pipeline_step(run_id=run_id, step_key=sname, metadata={
            **payload, **issue_payload, "review_row_count": row_count,
        })
        context.log.info("Published %s review: %d rows, %s issues", dataset_code, row_count, issue_payload.get("issue_count", 0))

    total_rows = control_plane.review_row_count("security_master", run_id) + control_plane.review_row_count("shareholder_holdings", run_id)
    control_plane.complete_pipeline_run(run_id=run_id, status="pending_review", metadata={})
    return MaterializeResult(
        metadata={"review_row_count": total_rows, "issue_count": total_issues},
        check_results=[
            review_rows_check(total_rows),
            review_issue_scan_check(total_issues),
        ],
    )


# ── Step 5: Export (dbt) — export-tagged models ────────────────────────────────

@dbt_assets(
    manifest=DBT_PROJECT.manifest_path,
    project=DBT_PROJECT,
    name="security_shareholder_export_assets",
    select="tag:security_shareholder,tag:exports",
    dagster_dbt_translator=DAGFLOW_DBT_TRANSLATOR,
)
def export_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    control_plane: ControlPlaneResource,
) -> Iterator:
    yield from stream_dbt_build(
        context, dbt, control_plane, "security_shareholder", "security_master", "dbt_export_build"
    )


# ── Step 6: Write Export CSVs ──────────────────────────────────────────────────

@asset(
    key=CSV_EXPORT_ASSET_KEY,
    deps=[
        dbt_asset_key("security_shareholder", "securities_export"),
        dbt_asset_key("security_shareholder", "holdings_export"),
    ],
    group_name="validated_export",
    kinds={"python", "csv", "filesystem"},
    description=asset_description(
        manual_asset_narrative("csv_export"),
        layer="validated_export",
        tool="python",
        database="filesystem",
    ),
    metadata=asset_metadata(
        manual_asset_narrative("csv_export"),
        pipeline_code="security_shareholder",
        layer="validated_export",
        tool="python",
        database="filesystem",
    ),
    check_specs=[AssetCheckSpec("csv_written", asset=CSV_EXPORT_ASSET_KEY)],
)
def csv_export(
    context: AssetExecutionContext, control_plane: ControlPlaneResource
) -> MaterializeResult:
    from dagflow_dagster.execution import export_step_key
    run_id = pipeline_run_id(context, "security_shareholder")
    bdate = business_date(context, control_plane)
    csv_step_key = "security_shareholder__csv_export"
    control_plane.start_pipeline_step(
        pipeline_code="security_shareholder", dataset_code="security_shareholder",
        run_id=run_id, business_date=bdate, step_key=csv_step_key,
        metadata={"stage": "csv_export"}, mark_run_running=False,
    )
    total_rows = 0
    all_bundles: dict[str, dict] = {}

    for dataset_code in ("security_master", "shareholder_holdings"):
        sname = export_step_key("security_shareholder") + f"_{dataset_code}"
        control_plane.start_pipeline_step(
            pipeline_code="security_shareholder", dataset_code=dataset_code,
            run_id=run_id, business_date=bdate, step_key=sname,
            metadata={"stage": "validated_export"}, mark_run_running=False,
        )
        bundle = control_plane.write_export_bundle(
            pipeline_code="security_shareholder", dataset_code=dataset_code,
            run_id=run_id, business_date=bdate,
        )
        row_count = int(bundle.get("export_row_count", 0))
        total_rows += row_count
        control_plane.complete_pipeline_step(run_id=run_id, step_key=sname, metadata=bundle)
        all_bundles[dataset_code] = bundle
        context.log.info("Exported %s: %d rows", dataset_code, row_count)

    for dataset_code in ("security_master", "shareholder_holdings"):
        control_plane.finalize_export(
            pipeline_code="security_shareholder", dataset_code=dataset_code,
            run_id=run_id, business_date=bdate,
        )

    # Record csv_export step completion with all bundle metadata
    csv_step_meta: dict[str, object] = {"total_export_rows": total_rows}
    for dc, b in all_bundles.items():
        csv_step_meta[f"{dc}_bundle_dir"] = str(b.get("bundle_dir", ""))
        csv_step_meta[f"{dc}_reviewed_csv"] = str(b.get("reviewed_file_path", ""))
        csv_step_meta[f"{dc}_baseline_csv"] = str(b.get("baseline_file_path", ""))
        csv_step_meta[f"{dc}_audit_parquet"] = str(b.get("audit_file_path", ""))
        csv_step_meta[f"{dc}_manifest"] = str(b.get("manifest_file_path", ""))
        csv_step_meta[f"{dc}_export_rows"] = int(b.get("export_row_count", 0))
        csv_step_meta[f"{dc}_changed_rows"] = int(b.get("changed_row_count", 0))
    control_plane.complete_pipeline_step(run_id=run_id, step_key=csv_step_key, metadata=csv_step_meta)

    control_plane.complete_pipeline_run(
        run_id=run_id, status="exported", metadata={"total_export_rows": total_rows},
    )

    from dagflow_dagster.asset_helpers import export_rows_check
    dagster_meta: dict[str, object] = dict(csv_step_meta)
    for dc, b in all_bundles.items():
        dagster_meta[f"{dc}_bundle_dir"] = str(b.get("bundle_dir", ""))
        dagster_meta[f"{dc}_reviewed_csv"] = str(b.get("reviewed_file_path", ""))
        dagster_meta[f"{dc}_baseline_csv"] = str(b.get("baseline_file_path", ""))
        dagster_meta[f"{dc}_audit_parquet"] = str(b.get("audit_file_path", ""))
        dagster_meta[f"{dc}_manifest"] = str(b.get("manifest_file_path", ""))
        dagster_meta[f"{dc}_export_rows"] = int(b.get("export_row_count", 0))
        dagster_meta[f"{dc}_changed_rows"] = int(b.get("changed_row_count", 0))

    return MaterializeResult(
        metadata=dagster_meta,
        check_results=[export_rows_check(total_rows, f"security_shareholder_{bdate.isoformat()}")],
    )
