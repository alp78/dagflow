from __future__ import annotations

import math
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from dagflow_dagster.config import get_settings as get_dagster_settings
from dagflow_dagster.execution import export_step_key
from dagflow_dagster.resources import ControlPlaneResource
from psycopg.sql import SQL, Identifier
from psycopg.types.json import Jsonb

from dagflow_api.db import Database
from dagflow_api.schemas import (
    DashboardChartPoint,
    DashboardDatasetSnapshot,
    DashboardFocusSecurity,
    DashboardMetric,
    DashboardMissingField,
    PipelineExecutionRun,
    PipelineExecutionStatusResponse,
    PipelineExecutionStep,
    PipelineView,
    ReviewSnapshotSummary,
)

DASHBOARD_COLORS = [
    "#f29e5c",
    "#5ea1f6",
    "#72c7a8",
    "#d78af5",
    "#f0d36c",
    "#7fd1e8",
]

REVIEW_EDIT_BLOCK_MARKERS = (
    "is not editable in review",
    "only factual provider corrections are allowed",
)

ACTIVE_EXECUTION_STATUSES = {"queued", "running", "approved"}
ACTIVE_STEP_STATUSES = {"pending", "running"}
EXPORT_ARTIFACT_PATH_KEYS = {
    "baseline.csv": "baseline_file_path",
    "reviewed.csv": "reviewed_file_path",
    "audit_events.parquet": "audit_file_path",
    "manifest.json": "manifest_file_path",
}


def _artifact_metadata_from_row(row: dict[str, Any]) -> dict[str, str]:
    return {
        "baseline_file_path": str(row["baseline_file_path"]),
        "reviewed_file_path": str(row["reviewed_file_path"]),
        "audit_file_path": str(row["audit_file_path"]),
        "manifest_file_path": str(row["manifest_file_path"]),
    }


def _control_plane() -> ControlPlaneResource:
    settings = get_dagster_settings()
    return ControlPlaneResource(
        direct_database_url=settings.direct_database_url,
        export_root_dir=settings.export_root_dir,
        landing_root_dir=str(settings.resolved_landing_root_dir),
        edgar_identity=settings.edgar_identity,
        sec_13f_lookback_days=settings.sec_13f_lookback_days,
        sec_13f_filing_limit=settings.sec_13f_filing_limit,
        sec_security_focus_limit=settings.sec_security_focus_limit,
        openfigi_api_key=settings.openfigi_api_key,
        finnhub_api_key=settings.finnhub_api_key,
    )


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)


def _compact_number(value: float) -> str:
    magnitude = abs(value)
    if magnitude < 1_000 and math.isclose(value, round(value)):
        return str(int(round(value)))
    if magnitude >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.1f}T"
    if magnitude >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if magnitude >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if magnitude >= 1_000:
        return f"{value / 1_000:.1f}K"
    if magnitude >= 100:
        return f"{value:.0f}"
    if magnitude >= 10:
        return f"{value:.1f}"
    if magnitude >= 1:
        return f"{value:.2f}"
    return f"{value:.4f}".rstrip("0").rstrip(".")


def _format_value(value: float, *, percent: bool = False) -> str:
    if percent:
        return f"{value * 100:.1f}%"
    return _compact_number(value)


def _raise_permission_for_review_edit(error: psycopg.Error) -> None:
    message = str(error).splitlines()[0]
    if any(marker in message.lower() for marker in REVIEW_EDIT_BLOCK_MARKERS):
        raise PermissionError(message) from error


def _range_label(lower: float, upper: float, *, percent: bool = False) -> str:
    return f"{_format_value(lower, percent=percent)}-{_format_value(upper, percent=percent)}"


def _metric(
    label: str,
    value: float,
    note: str | None = None,
    *,
    percent: bool = False,
) -> DashboardMetric:
    return DashboardMetric(
        label=label,
        value=value,
        display_value=_format_value(value, percent=percent),
        note=note,
    )


def _distribution_points(counter: Counter[str]) -> list[DashboardChartPoint]:
    total = sum(counter.values())
    points: list[DashboardChartPoint] = []
    for index, (label, value) in enumerate(counter.most_common()):
        points.append(
            DashboardChartPoint(
                label=label,
                value=float(value),
                display_value=str(value),
                percentage=(value / total * 100) if total else None,
                color=DASHBOARD_COLORS[index % len(DASHBOARD_COLORS)],
            )
        )
    return points


def _top_distribution_points(
    counter: Counter[str],
    *,
    limit: int = 6,
    other_label: str = "Other",
) -> list[DashboardChartPoint]:
    if len(counter) <= limit:
        return _distribution_points(counter)

    top_items = counter.most_common(limit)
    total = sum(counter.values())
    other_count = total - sum(value for _, value in top_items)
    points: list[DashboardChartPoint] = []
    for index, (label, value) in enumerate(top_items):
        points.append(
            DashboardChartPoint(
                label=label,
                value=float(value),
                display_value=str(value),
                percentage=(value / total * 100) if total else None,
                color=DASHBOARD_COLORS[index % len(DASHBOARD_COLORS)],
            )
        )
    if other_count > 0:
        points.append(
            DashboardChartPoint(
                label=other_label,
                value=float(other_count),
                display_value=str(other_count),
                percentage=(other_count / total * 100) if total else None,
                color=DASHBOARD_COLORS[len(top_items) % len(DASHBOARD_COLORS)],
            )
        )
    return points


def _histogram_points(
    values: list[float], *, bins: int = 8, percent: bool = False, log_scale: bool = False
) -> list[DashboardChartPoint]:
    clean_values = [max(0.0, _to_float(value)) for value in values if value is not None]
    if not clean_values:
        return []

    if len(clean_values) == 1 or math.isclose(min(clean_values), max(clean_values)):
        only_value = clean_values[0]
        return [
            DashboardChartPoint(
                label=_range_label(only_value, only_value, percent=percent),
                value=float(len(clean_values)),
                display_value=str(len(clean_values)),
                percentage=100.0,
                color=DASHBOARD_COLORS[0],
            )
        ]

    transformed = [math.log10(value + 1.0) if log_scale else value for value in clean_values]
    lower_bound = min(transformed)
    upper_bound = max(transformed)
    step = (upper_bound - lower_bound) / bins
    counts = [0] * bins

    for transformed_value in transformed:
        bucket = min(bins - 1, int((transformed_value - lower_bound) / step))
        counts[bucket] += 1

    points: list[DashboardChartPoint] = []
    total = len(clean_values)
    for index, count in enumerate(counts):
        start_t = lower_bound + (step * index)
        end_t = lower_bound + (step * (index + 1))
        start = (10**start_t - 1.0) if log_scale else start_t
        end = (10**end_t - 1.0) if log_scale else end_t
        if index == 0:
            start = min(clean_values)
        if index == bins - 1:
            end = max(clean_values)
        points.append(
            DashboardChartPoint(
                label=_range_label(start, end, percent=percent),
                value=float(count),
                display_value=str(count),
                percentage=(count / total * 100) if total else None,
                color=DASHBOARD_COLORS[index % len(DASHBOARD_COLORS)],
            )
        )
    return points


def _missing_field(
    *,
    field_name: str,
    label: str,
    missing_count: int,
    total_count: int,
) -> DashboardMissingField:
    present_count = max(0, total_count - missing_count)
    missing_percentage = (missing_count / total_count * 100) if total_count else 0.0
    completeness_percentage = (present_count / total_count * 100) if total_count else 0.0
    return DashboardMissingField(
        field_name=field_name,
        label=label,
        missing_count=missing_count,
        present_count=present_count,
        total_count=total_count,
        missing_percentage=missing_percentage,
        completeness_percentage=completeness_percentage,
    )


class DagflowRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def list_pipelines(self) -> list[PipelineView]:
        query = """
        select
            pr.pipeline_code,
            pr.pipeline_name,
            pr.dataset_code,
            pr.source_type,
            pr.adapter_type,
            pr.is_enabled,
            pr.is_schedulable,
            pr.default_schedule,
            pr.review_required,
            pr.review_table_name,
            pr.export_contract_code,
            pr.calc_package_code,
            pr.approval_workflow_code,
            pr.storage_prefix,
            pr.owner_team,
            coalesce(ps.is_active, pr.is_enabled) as is_active,
            coalesce(ps.approval_state, 'pending_review') as approval_state,
            ps.last_run_id,
            ps.last_business_date
        from control.pipeline_registry pr
        left join control.pipeline_state ps
            on ps.pipeline_code = pr.pipeline_code
        order by pr.pipeline_code
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        return [PipelineView.model_validate(row) for row in rows]

    def set_pipeline_enabled(
        self, pipeline_code: str, is_enabled: bool, changed_by: str
    ) -> dict[str, Any]:
        query = "select workflow.set_pipeline_enabled(%s, %s, %s) as payload"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (pipeline_code, is_enabled, changed_by))
            row = cursor.fetchone()
            connection.commit()
        return row["payload"]

    def resolve_review_table(self, dataset_code: str) -> str:
        query = """
        select review_table_name
        from control.dataset_registry
        where dataset_code = %s
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (dataset_code,))
            row = cursor.fetchone()
        if row is None:
            raise KeyError(f"Unknown dataset code: {dataset_code}")
        return row["review_table_name"]

    def _resolve_review_order(self, dataset_code: str) -> SQL:
        if dataset_code == "security_master":
            return SQL("ticker, issuer_name, review_row_id")
        if dataset_code == "shareholder_holdings":
            return SQL("security_identifier, filer_name, review_row_id")
        return SQL("review_row_id")

    def _current_review_selection(self, dataset_code: str) -> dict[str, Any] | None:
        query = """
        select pipeline_code, dataset_code, last_run_id, last_business_date, approval_state
        from control.pipeline_state
        where dataset_code = %s
        order by updated_at desc, pipeline_code
        limit 1
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (dataset_code,))
            row = cursor.fetchone()
        if row is None or row["last_run_id"] is None or row["last_business_date"] is None:
            return None
        return row

    def _relation_exists(self, relation_name: str) -> bool:
        query = "select to_regclass(%s) is not null as exists"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (relation_name,))
            row = cursor.fetchone()
        return bool(row and row["exists"])

    def _get_review_calculation_affected_columns(
        self,
        dataset_code: str,
        review_row_ids: list[int],
    ) -> dict[int, list[str]]:
        if not review_row_ids:
            return {}

        if dataset_code == "security_master":
            query = """
            with holding_baseline as (
                select
                    h.security_review_row_id,
                    case
                        when coalesce(h.edited_cells, '{}'::jsonb) ? 'shares_held_raw' then
                            coalesce(
                                nullif(
                                    h.edited_cells -> 'shares_held_raw' ->> 'old',
                                    ''
                                )::numeric,
                                0
                            )
                        else coalesce(h.shares_held_raw, 0)
                    end as baseline_shares_held,
                    coalesce(h.edited_cells, '{}'::jsonb) ? 'shares_held_raw'
                        as has_manual_holding_edit
                from review.shareholder_holdings_daily h
                where h.security_review_row_id = any(%s)
            ),
            holding_rollup as (
                select
                    h.security_review_row_id,
                    count(*) filter (
                        where h.baseline_shares_held > 0
                    )::numeric as baseline_holder_count,
                    coalesce(sum(h.baseline_shares_held), 0)::numeric
                        as baseline_total_held_shares,
                    coalesce(max(h.baseline_shares_held), 0)::numeric
                        as baseline_top_holder_shares,
                    bool_or(h.has_manual_holding_edit) as has_manual_holding_edit
                from holding_baseline h
                group by h.security_review_row_id
            ),
            baseline_input as (
                select
                    s.review_row_id,
                    s.free_float_shares,
                    s.investable_shares,
                    s.review_materiality_score,
                    coalesce(s.shares_outstanding_override, s.shares_outstanding_raw, 0)::numeric
                        as current_outstanding_shares,
                    case
                        when s.shares_outstanding_override is not null then
                            s.shares_outstanding_override::numeric
                        when coalesce(s.edited_cells, '{}'::jsonb) ? 'shares_outstanding_raw' then
                            coalesce(
                                nullif(
                                    s.edited_cells -> 'shares_outstanding_raw' ->> 'old',
                                    ''
                                )::numeric,
                                0
                            )
                        else coalesce(s.shares_outstanding_raw, 0)
                    end as baseline_outstanding_shares,
                    coalesce(h.baseline_holder_count, 0)::numeric as holder_count,
                    coalesce(h.baseline_total_held_shares, 0)::numeric as total_held_shares,
                    coalesce(h.baseline_top_holder_shares, 0)::numeric as top_holder_shares,
                    coalesce(h.has_manual_holding_edit, false) as has_manual_holding_edit,
                    coalesce(s.edited_cells, '{}'::jsonb) ? 'shares_outstanding_raw'
                        as has_manual_security_edit,
                    s.free_float_pct_raw,
                    s.investability_factor_raw,
                    s.free_float_pct_override,
                    s.investability_factor_override
                from review.security_master_daily s
                left join holding_rollup h
                    on h.security_review_row_id = s.review_row_id
                where s.review_row_id = any(%s)
            ),
            baseline_metrics as (
                select
                    review_row_id,
                    free_float_pct_raw,
                    investability_factor_raw,
                    free_float_shares,
                    investable_shares,
                    review_materiality_score,
                    current_outstanding_shares,
                    baseline_outstanding_shares,
                    has_manual_holding_edit,
                    has_manual_security_edit,
                    case
                        when (has_manual_holding_edit or has_manual_security_edit)
                            and baseline_outstanding_shares <= 0
                        then 1::numeric
                        when has_manual_holding_edit or has_manual_security_edit then greatest(
                            0::numeric,
                            least(
                                1::numeric,
                                1::numeric
                                - least(
                                    total_held_shares / nullif(baseline_outstanding_shares, 0),
                                    1::numeric
                                )
                            )
                        )
                        else coalesce(
                            free_float_pct_override,
                            free_float_pct_raw,
                            1
                        )
                    end as baseline_free_float_pct,
                    case
                        when (has_manual_holding_edit or has_manual_security_edit)
                            and baseline_outstanding_shares <= 0
                        then 0.70::numeric
                        when has_manual_holding_edit or has_manual_security_edit then greatest(
                            0.35::numeric,
                            least(
                                1::numeric,
                                0.45::numeric
                                + (least(holder_count, 100::numeric) / 250::numeric)
                                + (
                                    greatest(
                                        0::numeric,
                                        least(
                                            1::numeric,
                                            1::numeric
                                            - least(
                                                total_held_shares
                                                / nullif(baseline_outstanding_shares, 0),
                                                1::numeric
                                            )
                                        )
                                    ) * 0.25::numeric
                                )
                                - (
                                    least(
                                        top_holder_shares / nullif(baseline_outstanding_shares, 0),
                                        1::numeric
                                    )
                                    * 0.50::numeric
                                )
                            )
                        )
                        else coalesce(
                            investability_factor_override,
                            investability_factor_raw,
                            1
                        )
                    end as baseline_investability_factor,
                    free_float_pct_override,
                    investability_factor_override
                from baseline_input
            )
            select
                m.review_row_id,
                array_remove(array[
                    case
                        when (m.has_manual_holding_edit or m.has_manual_security_edit)
                         and m.free_float_pct_raw is distinct from round(
                            m.baseline_free_float_pct,
                            6
                         )
                        then 'free_float_pct_raw'
                    end,
                    case
                        when (m.has_manual_holding_edit or m.has_manual_security_edit)
                         and m.investability_factor_raw is distinct from round(
                            m.baseline_investability_factor,
                            6
                         )
                        then 'investability_factor_raw'
                    end,
                    case
                        when (m.has_manual_holding_edit or m.has_manual_security_edit)
                         and m.free_float_shares is distinct from round(
                            m.baseline_outstanding_shares
                            * coalesce(m.free_float_pct_override, m.baseline_free_float_pct),
                            6
                         )
                        then 'free_float_shares'
                    end,
                    case
                        when (m.has_manual_holding_edit or m.has_manual_security_edit)
                         and m.investable_shares is distinct from round(
                            m.baseline_outstanding_shares
                            * coalesce(m.free_float_pct_override, m.baseline_free_float_pct)
                            * coalesce(
                                m.investability_factor_override,
                                m.baseline_investability_factor
                            ),
                            6
                         )
                        then 'investable_shares'
                    end,
                    case
                        when (m.has_manual_holding_edit or m.has_manual_security_edit)
                         and m.review_materiality_score is distinct from round(
                            (
                                m.baseline_outstanding_shares
                                * coalesce(m.free_float_pct_override, m.baseline_free_float_pct)
                                * coalesce(
                                    m.investability_factor_override,
                                    m.baseline_investability_factor
                                )
                            ) / 1000000.0,
                            6
                         )
                        then 'review_materiality_score'
                    end
                ], null)::text[] as calculation_affected_columns
            from baseline_metrics m
            where m.review_row_id = any(%s)
            """
        elif dataset_code == "shareholder_holdings":
            if not self._relation_exists("marts.fact_shareholder_holding"):
                return {}
            query = """
            select
                h.review_row_id,
                array_remove(array[
                    case
                        when m.holding_id is not null
                         and h.holding_pct_of_outstanding is distinct from
                             m.holding_pct_of_outstanding
                        then 'holding_pct_of_outstanding'
                    end,
                    case
                        when m.holding_id is not null
                         and h.derived_price_per_share is distinct from m.derived_price_per_share
                        then 'derived_price_per_share'
                    end
                ], null)::text[] as calculation_affected_columns
            from review.shareholder_holdings_daily h
            left join marts.fact_shareholder_holding m
                on m.holding_id = h.origin_mart_row_id
            where h.review_row_id = any(%s)
            """
        else:
            return {}

        with self.database.connection() as connection, connection.cursor() as cursor:
            if dataset_code == "security_master":
                cursor.execute(query, (review_row_ids, review_row_ids, review_row_ids))
            else:
                cursor.execute(query, (review_row_ids,))
            rows = cursor.fetchall()
        return {
            int(row["review_row_id"]): list(row["calculation_affected_columns"] or [])
            for row in rows
        }

    def _attach_review_calculation_affected_columns(
        self,
        dataset_code: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        review_row_ids = [
            int(row["review_row_id"])
            for row in rows
            if row.get("review_row_id") is not None
        ]
        affected_columns_by_row_id = self._get_review_calculation_affected_columns(
            dataset_code,
            review_row_ids,
        )
        for row in rows:
            review_row_id = row.get("review_row_id")
            row["calculation_affected_columns"] = affected_columns_by_row_id.get(
                int(review_row_id) if review_row_id is not None else -1,
                [],
            )
        return rows

    def list_review_rows(
        self,
        dataset_code: str,
        run_id: UUID | None,
        business_date: date | None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if run_id is None and business_date is None:
            selection = self._current_review_selection(dataset_code)
            if selection is None:
                return []
            run_id = selection["last_run_id"]
            business_date = selection["last_business_date"]
        review_table = self.resolve_review_table(dataset_code)
        schema_name, table_name = review_table.split(".", maxsplit=1)
        clauses: list[str] = []
        params: list[Any] = []
        if run_id is not None:
            clauses.append("run_id = %s")
            params.append(run_id)
        if business_date is not None:
            clauses.append("business_date = %s")
            params.append(business_date)
        where_sql = f"where {' and '.join(clauses)}" if clauses else ""
        query = SQL("select * from {}.{} {} order by {} limit %s").format(
            Identifier(schema_name),
            Identifier(table_name),
            SQL(where_sql),
            self._resolve_review_order(dataset_code),
        )
        params.append(limit)
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = list(cursor.fetchall())
        return self._attach_review_calculation_affected_columns(dataset_code, rows)

    def list_review_snapshots(
        self,
        dataset_code: str,
        limit: int = 5000,
    ) -> list[ReviewSnapshotSummary]:
        selection = self._current_review_selection(dataset_code)
        if selection is None:
            return []

        review_table = self.resolve_review_table(dataset_code)
        schema_name, table_name = review_table.split(".", maxsplit=1)
        query = SQL(
            """
            select
                rows.run_id,
                rows.business_date,
                coalesce(review.review_state, runs.status, 'pending_review') as review_state,
                count(*)::bigint as row_count,
                max(rows.updated_at) as updated_at,
                true as is_current
            from {}.{} rows
            left join workflow.dataset_review_state review
              on review.dataset_code = %s
             and review.run_id = rows.run_id
            left join observability.pipeline_runs runs
              on runs.dataset_code = %s
             and runs.run_id = rows.run_id
            where rows.run_id = %s
              and rows.business_date = %s
            group by
                rows.run_id,
                rows.business_date,
                coalesce(review.review_state, runs.status, 'pending_review')
            limit %s
            """
        ).format(Identifier(schema_name), Identifier(table_name))
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    dataset_code,
                    dataset_code,
                    selection["last_run_id"],
                    selection["last_business_date"],
                    limit,
                ),
            )
            rows = cursor.fetchall()
        return [ReviewSnapshotSummary.model_validate(row) for row in rows]

    def list_available_source_dates(self, pipeline_code: str) -> list[dict[str, Any]]:
        return _control_plane().list_available_source_dates(pipeline_code)

    def _review_row_context(self, review_table: str, row_id: int) -> dict[str, Any]:
        schema_name, table_name = review_table.split(".", maxsplit=1)
        query = SQL(
            """
            select
                pipeline_code,
                dataset_code,
                run_id,
                business_date
            from {}.{}
            where review_row_id = %s
            """
        ).format(Identifier(schema_name), Identifier(table_name))
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (row_id,))
            row = cursor.fetchone()
        if row is None:
            raise KeyError(f"Review row {row_id} was not found in {review_table}")
        return row

    def ensure_current_review_snapshot(
        self,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
    ) -> None:
        query = """
        select last_run_id, last_business_date
        from control.pipeline_state
        where dataset_code = %s
        order by updated_at desc, pipeline_code
        limit 1
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (dataset_code,))
            row = cursor.fetchone()
        if row is None:
            raise PermissionError(f"No pipeline state exists for {dataset_code}.")
        if row["last_run_id"] is None or row["last_business_date"] is None:
            raise PermissionError(f"No current review snapshot exists for {dataset_code}.")
        if row["last_run_id"] != run_id or row["last_business_date"] != business_date:
            raise PermissionError(
                "Only the current operational run can be edited or validated. "
                "Load the selected source date into the workspace before making changes."
            )

    def ensure_holdings_review_unlocked(self, business_date: date) -> None:
        security_state = self._get_pipeline_state("security_master")
        if security_state is None or security_state.get("last_run_id") is None:
            raise PermissionError(
                "Shareholder Holdings stays locked until Security Master is "
                "validated for the same business date."
            )
        if security_state.get("last_business_date") != business_date:
            raise PermissionError(
                "Shareholder Holdings stays locked until the current Security Master run for "
                f"{business_date.isoformat()} is validated."
            )
        if security_state.get("approval_state") not in {"approved", "exported"}:
            raise PermissionError(
                "Validate Security Master first. Shareholder Holdings only becomes editable "
                "after Security Master is approved."
            )

    def apply_cell_edit(self, payload: dict[str, Any]) -> dict[str, Any]:
        row_context = self._review_row_context(payload["review_table"], payload["row_id"])
        self.ensure_current_review_snapshot(
            row_context["dataset_code"],
            row_context["run_id"],
            row_context["business_date"],
        )
        query = """
        select review.apply_cell_edit(%s::regclass, %s, %s, %s, %s, %s) as payload
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            try:
                cursor.execute(
                    query,
                    (
                        payload["review_table"],
                        payload["row_id"],
                        payload["column_name"],
                        payload["new_value"],
                        payload["changed_by"],
                        payload["change_reason"],
                    ),
                )
                row = cursor.fetchone()
                connection.commit()
            except psycopg.Error as error:
                connection.rollback()
                _raise_permission_for_review_edit(error)
                raise
        return row["payload"]

    def apply_row_edit(self, payload: dict[str, Any]) -> dict[str, Any]:
        row_context = self._review_row_context(payload["review_table"], payload["row_id"])
        self.ensure_current_review_snapshot(
            row_context["dataset_code"],
            row_context["run_id"],
            row_context["business_date"],
        )
        query = """
        select review.apply_row_edit(%s::regclass, %s, %s::jsonb, %s, %s) as payload
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            try:
                cursor.execute(
                    query,
                    (
                        payload["review_table"],
                        payload["row_id"],
                        Jsonb(payload["updates"]),
                        payload["changed_by"],
                        payload["change_reason"],
                    ),
                )
                row = cursor.fetchone()
                connection.commit()
            except psycopg.Error as error:
                connection.rollback()
                _raise_permission_for_review_edit(error)
                raise
        return row["payload"]

    def get_edited_cells(self, review_table: str, row_id: int) -> dict[str, Any]:
        query = "select review.get_edited_cells(%s::regclass, %s) as payload"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (review_table, row_id))
            row = cursor.fetchone()
        return row["payload"]

    def get_row_diff(self, review_table: str, row_id: int) -> dict[str, Any]:
        query = "select review.get_row_diff(%s::regclass, %s) as payload"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (review_table, row_id))
            row = cursor.fetchone()
        return row["payload"]

    def recalc_security(self, row_id: int, changed_by: str) -> dict[str, Any]:
        row_context = self._review_row_context("review.security_master_daily", row_id)
        self.ensure_current_review_snapshot(
            row_context["dataset_code"],
            row_context["run_id"],
            row_context["business_date"],
        )
        query = "select calc.recalc_security_review_fields(%s, %s) as payload"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (row_id, changed_by))
            row = cursor.fetchone()
            connection.commit()
        return row["payload"]

    def recalc_holding(self, row_id: int, changed_by: str) -> dict[str, Any]:
        row_context = self._review_row_context("review.shareholder_holdings_daily", row_id)
        self.ensure_current_review_snapshot(
            row_context["dataset_code"],
            row_context["run_id"],
            row_context["business_date"],
        )
        query = "select calc.recalc_holding_review_fields(%s, %s) as payload"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (row_id, changed_by))
            row = cursor.fetchone()
            connection.commit()
        return row["payload"]

    def call_workflow_action(self, function_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        if function_name == "validate_dataset":
            self.ensure_current_review_snapshot(
                payload["dataset_code"],
                payload["run_id"],
                payload["business_date"],
            )
            if payload["dataset_code"] == "shareholder_holdings":
                self.ensure_holdings_review_unlocked(payload["business_date"])
        query = SQL("select workflow.{}(%s, %s, %s, %s, %s, %s) as payload").format(
            Identifier(function_name)
        )
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    payload["pipeline_code"],
                    payload["dataset_code"],
                    payload["run_id"],
                    payload["business_date"],
                    payload["actor"],
                    payload["notes"],
                ),
            )
            row = cursor.fetchone()
            connection.commit()
        return row["payload"]

    def get_pipeline_execution_status(
        self,
        run_ids: list[UUID],
        relations_by_run_id: dict[UUID, str] | None = None,
    ) -> PipelineExecutionStatusResponse:
        if not run_ids:
            return PipelineExecutionStatusResponse(run_ids=[], is_complete=True, runs=[])

        run_query = """
        select
            run_id,
            pipeline_code,
            dataset_code,
            business_date,
            status,
            started_at,
            ended_at,
            metadata
        from observability.pipeline_runs
        where run_id = any(%s)
        """
        step_query = """
        select
            run_id,
            step_key,
            step_label,
            step_group,
            sort_order,
            status,
            started_at,
            ended_at,
            metadata
        from observability.pipeline_step_runs
        where run_id = any(%s)
        order by run_id, sort_order
        """
        export_registry_query = """
        select
            pipeline_code,
            dataset_code,
            run_id,
            business_date,
            baseline_file_path,
            reviewed_file_path,
            audit_file_path,
            manifest_file_path,
            exported_at,
            metadata
        from control.export_bundle_registry
        where run_id = any(%s)
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(run_query, (run_ids,))
            run_rows = cursor.fetchall()
            cursor.execute(step_query, (run_ids,))
            step_rows = cursor.fetchall()
            cursor.execute(export_registry_query, (run_ids,))
            export_registry_rows = cursor.fetchall()

        steps_by_run_id: dict[UUID, list[PipelineExecutionStep]] = {}
        for row in step_rows:
            run_id = row["run_id"]
            steps_by_run_id.setdefault(run_id, []).append(PipelineExecutionStep.model_validate(row))

        run_row_by_id = {row["run_id"]: row for row in run_rows}
        export_registry_row_by_id = {row["run_id"]: row for row in export_registry_rows}
        runs: list[PipelineExecutionRun] = []
        for run_id in run_ids:
            row = run_row_by_id.get(run_id)
            if row is not None:
                steps = steps_by_run_id.get(run_id, [])
                metadata = row["metadata"] or {}
                run_payload = dict(row)
            else:
                export_registry_row = export_registry_row_by_id.get(run_id)
                if export_registry_row is None:
                    continue
                artifact_metadata = _artifact_metadata_from_row(export_registry_row)
                registry_metadata = export_registry_row["metadata"] or {}
                run_snapshot = registry_metadata.get("observability_run") or {}
                raw_steps = registry_metadata.get("steps") or []
                steps = []
                for raw_step in raw_steps:
                    step_payload = dict(raw_step)
                    if step_payload.get("step_key") == export_step_key(
                        str(export_registry_row["pipeline_code"])
                    ):
                        step_payload["metadata"] = {
                            **artifact_metadata,
                            **(step_payload.get("metadata") or {}),
                        }
                    steps.append(PipelineExecutionStep.model_validate(step_payload))
                if not steps:
                    steps = [
                        PipelineExecutionStep.model_validate(
                            {
                                "step_key": export_step_key(
                                    str(export_registry_row["pipeline_code"])
                                ),
                                "step_label": "Write export bundle",
                                "step_group": "validated_export",
                                "sort_order": 80,
                                "status": "succeeded",
                                "started_at": export_registry_row["exported_at"],
                                "ended_at": export_registry_row["exported_at"],
                                "metadata": artifact_metadata,
                            }
                        )
                    ]
                metadata = {
                    **(run_snapshot.get("metadata") or {}),
                    **artifact_metadata,
                }
                run_payload = {
                    "pipeline_code": export_registry_row["pipeline_code"],
                    "dataset_code": export_registry_row["dataset_code"],
                    "run_id": export_registry_row["run_id"],
                    "business_date": export_registry_row["business_date"],
                    "status": run_snapshot.get("status") or "exported",
                    "started_at": run_snapshot.get("started_at"),
                    "ended_at": run_snapshot.get("ended_at")
                    or export_registry_row["exported_at"],
                    "metadata": metadata,
                }

            if any(step.sort_order < 999 for step in steps):
                steps = [step for step in steps if step.sort_order < 999]
            current_step = next((step for step in steps if step.status == "running"), None)
            if current_step is None:
                current_step = next((step for step in steps if step.status == "failed"), None)
            if current_step is None:
                current_step = next((step for step in steps if step.status == "blocked"), None)
            if current_step is None:
                current_step = next((step for step in steps if step.status == "pending"), None)
            completed_step_count = sum(1 for step in steps if step.status == "succeeded")
            runs.append(
                PipelineExecutionRun.model_validate(
                    {
                        **run_payload,
                        "metadata": metadata,
                        "relation": (relations_by_run_id or {}).get(run_id, "primary"),
                        "steps": steps,
                        "current_step_label": current_step.step_label if current_step else None,
                        "completed_step_count": completed_step_count,
                        "total_step_count": len(steps),
                    }
                )
            )

        is_complete = all(
            not any(step.status in ACTIVE_STEP_STATUSES for step in run.steps)
            and run.status not in ACTIVE_EXECUTION_STATUSES
            for run in runs
        )
        return PipelineExecutionStatusResponse(
            run_ids=run_ids,
            is_complete=is_complete,
            runs=runs,
        )

    def get_export_artifact_path(self, run_id: UUID, artifact_name: str) -> Path:
        metadata_key = EXPORT_ARTIFACT_PATH_KEYS.get(artifact_name)
        if metadata_key is None:
            raise KeyError(f"Unsupported export artifact: {artifact_name}")

        query = """
        select metadata
        from observability.pipeline_step_runs
        where run_id = %s
          and step_group = 'validated_export'
        order by sort_order desc, step_run_id desc
        limit 1
        """
        registry_query = """
        select
            baseline_file_path,
            reviewed_file_path,
            audit_file_path,
            manifest_file_path
        from control.export_bundle_registry
        where run_id = %s
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (run_id,))
            row = cursor.fetchone()
            if row is None:
                cursor.execute(registry_query, (run_id,))
                row = cursor.fetchone()
                if row is None:
                    raise FileNotFoundError(
                        f"No export bundle metadata exists for run {run_id}"
                    )
                artifact_path = row[metadata_key]
            else:
                metadata = row["metadata"] or {}
                artifact_path = metadata.get(metadata_key)
        if not artifact_path:
            raise FileNotFoundError(
                f"Artifact {artifact_name} is not available for run {run_id}"
            )

        path = Path(str(artifact_path))
        if not path.exists():
            raise FileNotFoundError(f"Artifact path does not exist: {path}")
        return path

    def reset_demo_runs(self, actor: str, notes: str | None) -> dict[str, Any]:
        query = "select workflow.reset_demo_runs(%s, %s) as payload"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (actor, notes))
            row = cursor.fetchone()
            connection.commit()
        return row["payload"]

    def _get_pipeline_state(self, pipeline_code: str) -> dict[str, Any] | None:
        query = """
        select pipeline_code, dataset_code, approval_state, last_run_id, last_business_date
        from control.pipeline_state
        where pipeline_code = %s
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (pipeline_code,))
            return cursor.fetchone()

    def _list_focus_securities(self) -> list[DashboardFocusSecurity]:
        holdings_state = self._get_pipeline_state("shareholder_holdings")
        if not holdings_state or holdings_state["last_run_id"] is None:
            return []

        query = """
        select
            h.security_review_row_id as review_row_id,
            h.security_identifier as ticker,
            max(coalesce(s.issuer_name, h.security_name, h.security_identifier)) as issuer_name,
            count(*)::int as holder_count
        from review.shareholder_holdings_daily h
        left join review.security_master_daily s
            on s.review_row_id = h.security_review_row_id
        where h.run_id = %s
          and h.security_review_row_id is not null
        group by h.security_review_row_id, h.security_identifier
        order by count(*) desc, h.security_identifier
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (holdings_state["last_run_id"],))
            rows = cursor.fetchall()
        return [
            DashboardFocusSecurity(
                review_row_id=int(row["review_row_id"]),
                ticker=str(row["ticker"]),
                issuer_name=str(row["issuer_name"]),
                holder_count=int(row["holder_count"]),
            )
            for row in rows
        ]

    def get_dashboard_dataset_snapshot(self, dataset_code: str) -> DashboardDatasetSnapshot:
        focus_securities = self._list_focus_securities()
        default_focus_security_review_row_id = (
            focus_securities[0].review_row_id if focus_securities else None
        )

        if dataset_code == "security_master":
            return self._get_security_dashboard_snapshot(
                focus_securities=focus_securities,
                default_focus_security_review_row_id=default_focus_security_review_row_id,
            )
        if dataset_code == "shareholder_holdings":
            return self._get_holdings_dashboard_snapshot(
                focus_securities=focus_securities,
                default_focus_security_review_row_id=default_focus_security_review_row_id,
            )
        raise ValueError(f"Unsupported dashboard dataset: {dataset_code}")

    def _get_security_dashboard_snapshot(
        self,
        *,
        focus_securities: list[DashboardFocusSecurity],
        default_focus_security_review_row_id: int | None,
    ) -> DashboardDatasetSnapshot:
        state = self._get_pipeline_state("security_master")
        if not state or state["last_run_id"] is None:
            return DashboardDatasetSnapshot(
                dataset_code="security_master",
                dataset_label="Security master",
                business_date=state["last_business_date"] if state else None,
                run_id=state["last_run_id"] if state else None,
                distribution_title="Exchange mix",
                distribution_subtitle=(
                    "Distribution is available once the current security dataset is loaded."
                ),
                shares_histogram_title="Shares outstanding",
                shares_histogram_subtitle=(
                    "Outstanding-share spread is available once the current security dataset "
                    "is loaded."
                ),
                value_histogram_title="Investable shares",
                value_histogram_subtitle=(
                    "Investable-share spread is available once the current security dataset is "
                    "loaded."
                ),
                focus_security_options=focus_securities,
                default_focus_security_review_row_id=default_focus_security_review_row_id,
            )

        query = """
        select
            ticker,
            issuer_name,
            exchange,
            shares_outstanding_raw::double precision as shares_outstanding_raw,
            free_float_pct_raw::double precision as free_float_pct_raw,
            investability_factor_raw::double precision as investability_factor_raw,
            investable_shares::double precision as investable_shares,
            review_materiality_score::double precision as review_materiality_score,
            approval_state,
            coalesce(manual_edit_count, 0) as manual_edit_count
        from review.security_master_daily
        where run_id = %s
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (state["last_run_id"],))
            rows = list(cursor.fetchall())

        total_rows = len(rows)
        exchange_counter = Counter((row["exchange"] or "Unknown") for row in rows)

        missing_shares = sum(
            1
            for row in rows
            if row["shares_outstanding_raw"] in (None, 0)
        )
        missing_free_float = sum(
            1
            for row in rows
            if row["free_float_pct_raw"] in (None, 0)
        )
        missing_investability = sum(
            1
            for row in rows
            if row["investability_factor_raw"] in (None, 0)
        )
        rows_with_any_missing = sum(
            1
            for row in rows
            if row["shares_outstanding_raw"] in (None, 0)
            or row["free_float_pct_raw"] in (None, 0)
            or row["investability_factor_raw"] in (None, 0)
        )
        materiality_scores = [_to_float(row["review_materiality_score"]) for row in rows]
        sorted_scores = sorted(materiality_scores)

        metrics = [
            _metric("Security rows", float(total_rows)),
            _metric("Rows with missing inputs", float(rows_with_any_missing)),
            _metric(
                "Edited rows",
                float(sum(1 for row in rows if int(row["manual_edit_count"]) > 0)),
            ),
            _metric("Distinct exchanges", float(len(exchange_counter))),
            _metric(
                "Total investable shares",
                sum(_to_float(row["investable_shares"]) for row in rows),
            ),
            _metric(
                "Median materiality",
                sorted_scores[len(sorted_scores) // 2] if sorted_scores else 0.0,
            ),
        ]

        missing_fields = [
            _missing_field(
                field_name="shares_outstanding_raw",
                label="Missing outstanding shares",
                missing_count=missing_shares,
                total_count=total_rows,
            ),
            _missing_field(
                field_name="free_float_pct_raw",
                label="Missing estimated free float %",
                missing_count=missing_free_float,
                total_count=total_rows,
            ),
            _missing_field(
                field_name="investability_factor_raw",
                label="Missing estimated investability factor",
                missing_count=missing_investability,
                total_count=total_rows,
            ),
        ]

        return DashboardDatasetSnapshot(
            dataset_code="security_master",
            dataset_label="Security master",
            business_date=state["last_business_date"],
            run_id=state["last_run_id"],
            metrics=metrics,
            missing_fields=missing_fields,
            distribution_title="Exchange mix",
            distribution_subtitle=(
                "How the currently loaded security dataset is distributed across exchanges."
            ),
            distribution_points=_distribution_points(exchange_counter),
            shares_histogram_title="Shares outstanding",
            shares_histogram_subtitle=(
                "Histogram of the raw outstanding-share values currently under review."
            ),
            shares_histogram_points=_histogram_points(
                [_to_float(row["shares_outstanding_raw"]) for row in rows],
                bins=8,
                log_scale=True,
            ),
            value_histogram_title="Investable shares",
            value_histogram_subtitle=(
                "Histogram of the calculated investable-share outputs for the current dataset."
            ),
            value_histogram_points=_histogram_points(
                [_to_float(row["investable_shares"]) for row in rows],
                bins=8,
                log_scale=True,
            ),
            focus_security_options=focus_securities,
            default_focus_security_review_row_id=default_focus_security_review_row_id,
        )

    def _get_holdings_dashboard_snapshot(
        self,
        *,
        focus_securities: list[DashboardFocusSecurity],
        default_focus_security_review_row_id: int | None,
    ) -> DashboardDatasetSnapshot:
        state = self._get_pipeline_state("shareholder_holdings")
        if not state or state["last_run_id"] is None:
            return DashboardDatasetSnapshot(
                dataset_code="shareholder_holdings",
                dataset_label="Shareholder holdings",
                business_date=state["last_business_date"] if state else None,
                run_id=state["last_run_id"] if state else None,
                distribution_title="Security mix",
                distribution_subtitle=(
                    "Distribution is available once the current holdings dataset is loaded."
                ),
                shares_histogram_title="Shares held",
                shares_histogram_subtitle=(
                    "Shares-held spread is available once the current holdings dataset is loaded."
                ),
                value_histogram_title="Price per share",
                value_histogram_subtitle=(
                    "Price-per-share spread is available once the current holdings dataset is "
                    "loaded."
                ),
                focus_security_options=focus_securities,
                default_focus_security_review_row_id=default_focus_security_review_row_id,
            )

        query = """
        select
            filer_name,
            security_identifier,
            security_review_row_id,
            coalesce(shares_held_override, shares_held_raw)::double precision as shares_held,
            coalesce(
                reviewed_market_value_override,
                reviewed_market_value_raw
            )::double precision as market_value,
            derived_price_per_share::double precision as derived_price_per_share,
            holding_pct_of_outstanding::double precision as holding_pct_of_outstanding,
            portfolio_weight::double precision as portfolio_weight,
            approval_state,
            coalesce(manual_edit_count, 0) as manual_edit_count
        from review.shareholder_holdings_daily
        where run_id = %s
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (state["last_run_id"],))
            rows = list(cursor.fetchall())

        total_rows = len(rows)
        securities = {row["security_identifier"] for row in rows if row["security_identifier"]}
        filers = {row["filer_name"] for row in rows if row["filer_name"]}
        security_counter = Counter(
            (row["security_identifier"] or "Unknown") for row in rows
        )

        missing_shares = sum(
            1
            for row in rows
            if row["shares_held"] in (None, 0)
        )
        missing_market_value = sum(
            1
            for row in rows
            if row["market_value"] in (None, 0)
        )
        missing_price = sum(
            1
            for row in rows
            if row["derived_price_per_share"] in (None, 0)
        )
        rows_with_any_missing = sum(
            1
            for row in rows
            if row["shares_held"] in (None, 0)
            or row["market_value"] in (None, 0)
            or row["derived_price_per_share"] in (None, 0)
        )

        metrics = [
            _metric("Holding rows", float(total_rows)),
            _metric("Distinct securities", float(len(securities))),
            _metric("Distinct filers", float(len(filers))),
            _metric("Rows with missing inputs", float(rows_with_any_missing)),
            _metric(
                "Edited rows",
                float(sum(1 for row in rows if int(row["manual_edit_count"]) > 0)),
            ),
            _metric(
                "Total market value",
                sum(_to_float(row["market_value"]) for row in rows),
            ),
        ]

        missing_fields = [
            _missing_field(
                field_name="shares_held",
                label="Missing reported shares held",
                missing_count=missing_shares,
                total_count=total_rows,
            ),
            _missing_field(
                field_name="derived_price_per_share",
                label="Missing implied price per share",
                missing_count=missing_price,
                total_count=total_rows,
            ),
            _missing_field(
                field_name="market_value",
                label="Missing reported market value",
                missing_count=missing_market_value,
                total_count=total_rows,
            ),
        ]

        return DashboardDatasetSnapshot(
            dataset_code="shareholder_holdings",
            dataset_label="Shareholder holdings",
            business_date=state["last_business_date"],
            run_id=state["last_run_id"],
            metrics=metrics,
            missing_fields=missing_fields,
            distribution_title="Security mix",
            distribution_subtitle="Where the current holdings rows concentrate by security ticker.",
            distribution_points=_top_distribution_points(
                security_counter,
                limit=6,
                other_label="Other securities",
            ),
            shares_histogram_title="Shares held",
            shares_histogram_subtitle="Histogram of the currently reviewed holder position sizes.",
            shares_histogram_points=_histogram_points(
                [_to_float(row["shares_held"]) for row in rows],
                bins=8,
                log_scale=True,
            ),
            value_histogram_title="Price per share",
            value_histogram_subtitle="Histogram of the current derived price-per-share values.",
            value_histogram_points=_histogram_points(
                [_to_float(row["derived_price_per_share"]) for row in rows],
                bins=8,
                log_scale=True,
            ),
            focus_security_options=focus_securities,
            default_focus_security_review_row_id=default_focus_security_review_row_id,
        )

    def get_failure_rows(self, run_id: UUID) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_run_failure_rows(%s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (run_id,))
            return list(cursor.fetchall())

    def get_lineage_trace(self, row_hash: str) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_row_lineage_trace(%s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (row_hash,))
            return list(cursor.fetchall())

    def get_review_data_issues(
        self,
        dataset_code: str,
        run_id: UUID,
    ) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_review_data_issues(%s, %s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (dataset_code, run_id))
            return list(cursor.fetchall())

    def get_review_edited_cells(
        self,
        dataset_code: str,
        run_id: UUID,
    ) -> list[dict[str, Any]]:
        if dataset_code == "security_master":
            query = """
            select
                concat(s.review_row_id::text, ':', edit.key) as edit_key,
                s.review_row_id,
                s.ticker,
                s.issuer_name,
                s.exchange,
                edit.key as column_name,
                edit.value ->> 'old' as old_value,
                edit.value ->> 'new' as new_value,
                edit.value ->> 'changed_by' as changed_by,
                edit.value ->> 'changed_at' as changed_at,
                edit.value ->> 'reason' as reason
            from review.security_master_daily s
            cross join lateral jsonb_each(coalesce(s.edited_cells, '{}'::jsonb)) as edit(key, value)
            where s.run_id = %s
            order by
                nullif(edit.value ->> 'changed_at', '')::timestamptz desc nulls last,
                s.ticker asc,
                s.review_row_id asc,
                edit.key asc
            """
        elif dataset_code == "shareholder_holdings":
            query = """
            select
                concat(h.review_row_id::text, ':', edit.key) as edit_key,
                h.review_row_id,
                h.security_identifier,
                h.security_name,
                h.filer_name,
                edit.key as column_name,
                edit.value ->> 'old' as old_value,
                edit.value ->> 'new' as new_value,
                edit.value ->> 'changed_by' as changed_by,
                edit.value ->> 'changed_at' as changed_at,
                edit.value ->> 'reason' as reason
            from review.shareholder_holdings_daily h
            cross join lateral jsonb_each(coalesce(h.edited_cells, '{}'::jsonb)) as edit(key, value)
            where h.run_id = %s
            order by
                nullif(edit.value ->> 'changed_at', '')::timestamptz desc nulls last,
                h.security_identifier asc,
                h.filer_name asc,
                h.review_row_id asc,
                edit.key asc
            """
        else:
            raise KeyError(f"Unknown dataset code: {dataset_code}")

        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (run_id,))
            return list(cursor.fetchall())

    def get_security_shareholder_breakdown(
        self, security_review_row_id: int
    ) -> list[dict[str, Any]]:
        if not self._relation_exists("marts.fact_shareholder_holding"):
            query = "select * from query_api.fn_security_shareholder_breakdown(%s)"
            with self.database.connection() as connection, connection.cursor() as cursor:
                cursor.execute(query, (security_review_row_id,))
                rows = list(cursor.fetchall())
            for row in rows:
                row["calculation_affected_columns"] = []
            return rows

        query = """
        select
            h.review_row_id as holding_review_row_id,
            h.filer_name,
            coalesce(h.shares_held_override, h.shares_held_raw) as shares_held,
            h.holding_pct_of_outstanding,
            h.portfolio_weight,
            h.approval_state,
            coalesce(h.edited_cells ? 'shares_held_raw', false) as shares_held_manually_edited,
            array_remove(array[
                case
                    when m.holding_id is not null
                     and h.holding_pct_of_outstanding is distinct from m.holding_pct_of_outstanding
                    then 'holding_pct_of_outstanding'
                end
            ], null)::text[] as calculation_affected_columns
        from review.shareholder_holdings_daily h
        left join marts.fact_shareholder_holding m
            on m.holding_id = h.origin_mart_row_id
        where h.security_review_row_id = %s
        order by
            h.filer_name asc,
            coalesce(h.shares_held_override, h.shares_held_raw) desc nulls last
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (security_review_row_id,))
            return list(cursor.fetchall())

    def get_security_holder_valuations(
        self, security_review_row_id: int
    ) -> list[dict[str, Any]]:
        if not self._relation_exists("marts.fact_shareholder_holding"):
            query = """
            select
                h.review_row_id as holding_review_row_id,
                h.filer_name,
                coalesce(h.shares_held_override, h.shares_held_raw) as shares_held,
                coalesce(
                    h.reviewed_market_value_override,
                    h.reviewed_market_value_raw
                ) as market_value,
                h.holding_pct_of_outstanding,
                h.derived_price_per_share,
                h.portfolio_weight,
                h.approval_state,
                coalesce(h.edited_cells ? 'shares_held_raw', false) as shares_held_manually_edited,
                array[]::text[] as calculation_affected_columns
            from review.shareholder_holdings_daily h
            where h.security_review_row_id = %s
            order by
                coalesce(
                    h.reviewed_market_value_override,
                    h.reviewed_market_value_raw
                ) desc nulls last,
                h.filer_name
            """
            with self.database.connection() as connection, connection.cursor() as cursor:
                cursor.execute(query, (security_review_row_id,))
                return list(cursor.fetchall())

        query = """
        select
            h.review_row_id as holding_review_row_id,
            h.filer_name,
            coalesce(h.shares_held_override, h.shares_held_raw) as shares_held,
            coalesce(
                h.reviewed_market_value_override,
                h.reviewed_market_value_raw
            ) as market_value,
            h.holding_pct_of_outstanding,
            h.derived_price_per_share,
            h.portfolio_weight,
            h.approval_state,
            coalesce(h.edited_cells ? 'shares_held_raw', false) as shares_held_manually_edited,
            array_remove(array[
                case
                    when m.holding_id is not null
                     and h.holding_pct_of_outstanding is distinct from m.holding_pct_of_outstanding
                    then 'holding_pct_of_outstanding'
                end,
                case
                    when m.holding_id is not null
                     and h.derived_price_per_share is distinct from m.derived_price_per_share
                    then 'derived_price_per_share'
                end
            ], null)::text[] as calculation_affected_columns
        from review.shareholder_holdings_daily h
        left join marts.fact_shareholder_holding m
            on m.holding_id = h.origin_mart_row_id
        where h.security_review_row_id = %s
        order by
            coalesce(
                h.reviewed_market_value_override,
                h.reviewed_market_value_raw
            ) desc nulls last,
            h.filer_name
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (security_review_row_id,))
            return list(cursor.fetchall())

    def get_holding_peer_holders(self, holding_review_row_id: int) -> list[dict[str, Any]]:
        if not self._relation_exists("marts.fact_shareholder_holding"):
            query = "select * from query_api.fn_holding_peer_holders(%s)"
            with self.database.connection() as connection, connection.cursor() as cursor:
                cursor.execute(query, (holding_review_row_id,))
                rows = list(cursor.fetchall())
            for row in rows:
                row["calculation_affected_columns"] = []
            return rows

        query = """
        with selected_holding as (
            select security_review_row_id
            from review.shareholder_holdings_daily
            where review_row_id = %s
        )
        select
            h.review_row_id as holding_review_row_id,
            h.filer_name,
            coalesce(h.shares_held_override, h.shares_held_raw) as shares_held,
            h.holding_pct_of_outstanding,
            h.portfolio_weight,
            h.approval_state,
            array_remove(array[
                case
                    when m.holding_id is not null
                     and h.holding_pct_of_outstanding is distinct from m.holding_pct_of_outstanding
                    then 'holding_pct_of_outstanding'
                end
            ], null)::text[] as calculation_affected_columns
        from review.shareholder_holdings_daily h
        join selected_holding selected
            on selected.security_review_row_id = h.security_review_row_id
        left join marts.fact_shareholder_holding m
            on m.holding_id = h.origin_mart_row_id
        order by coalesce(h.shares_held_override, h.shares_held_raw) desc nulls last, h.filer_name
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (holding_review_row_id,))
            return list(cursor.fetchall())

    def get_filer_portfolio_snapshot(self, holding_review_row_id: int) -> list[dict[str, Any]]:
        if not self._relation_exists("marts.fact_shareholder_holding"):
            query = "select * from query_api.fn_filer_portfolio_snapshot(%s)"
            with self.database.connection() as connection, connection.cursor() as cursor:
                cursor.execute(query, (holding_review_row_id,))
                rows = list(cursor.fetchall())
            for row in rows:
                row["calculation_affected_columns"] = []
            return rows

        query = """
        with selected_holding as (
            select run_id, filer_cik
            from review.shareholder_holdings_daily
            where review_row_id = %s
        )
        select
            h.review_row_id as holding_review_row_id,
            h.security_identifier,
            h.security_name,
            coalesce(h.shares_held_override, h.shares_held_raw) as shares_held,
            coalesce(h.reviewed_market_value_override, h.reviewed_market_value_raw) as market_value,
            h.portfolio_weight,
            h.holding_pct_of_outstanding,
            h.approval_state,
            array_remove(array[
                case
                    when m.holding_id is not null
                     and h.holding_pct_of_outstanding is distinct from m.holding_pct_of_outstanding
                    then 'holding_pct_of_outstanding'
                end
            ], null)::text[] as calculation_affected_columns
        from review.shareholder_holdings_daily h
        join selected_holding selected
            on selected.run_id = h.run_id
           and selected.filer_cik = h.filer_cik
        left join marts.fact_shareholder_holding m
            on m.holding_id = h.origin_mart_row_id
        order by h.portfolio_weight desc nulls last, h.security_identifier
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (holding_review_row_id,))
            return list(cursor.fetchall())

    def get_filer_weight_bands(self, holding_review_row_id: int) -> list[dict[str, Any]]:
        if not self._relation_exists("marts.fact_shareholder_holding"):
            query = "select * from query_api.fn_filer_weight_bands(%s)"
            with self.database.connection() as connection, connection.cursor() as cursor:
                cursor.execute(query, (holding_review_row_id,))
                rows = list(cursor.fetchall())
            for row in rows:
                row["calculation_affected_columns"] = []
            return rows

        query = """
        with selected_holding as (
            select run_id, filer_cik
            from review.shareholder_holdings_daily
            where review_row_id = %s
        ),
        review_bucketed as (
            select
                case
                    when coalesce(h.portfolio_weight, 0) >= 0.15 then 'Core positions'
                    when coalesce(h.portfolio_weight, 0) >= 0.05 then 'Conviction positions'
                    else 'Satellite positions'
                end as weight_band,
                coalesce(
                    h.reviewed_market_value_override,
                    h.reviewed_market_value_raw,
                    0
                ) as market_value,
                h.portfolio_weight,
                case
                    when coalesce(h.portfolio_weight, 0) >= 0.15 then 1
                    when coalesce(h.portfolio_weight, 0) >= 0.05 then 2
                    else 3
                end as band_rank
            from review.shareholder_holdings_daily h
            join selected_holding selected
                on selected.run_id = h.run_id
               and selected.filer_cik = h.filer_cik
        ),
        review_agg as (
            select
                b.weight_band,
                b.band_rank,
                count(*)::bigint as positions,
                round(sum(b.market_value), 6) as total_market_value,
                round(avg(b.portfolio_weight), 6) as avg_weight
            from review_bucketed b
            group by b.weight_band, b.band_rank
        ),
        mart_bucketed as (
            select
                case
                    when coalesce(h.portfolio_weight, 0) >= 0.15 then 'Core positions'
                    when coalesce(h.portfolio_weight, 0) >= 0.05 then 'Conviction positions'
                    else 'Satellite positions'
                end as weight_band,
                coalesce(h.reviewed_market_value_raw, 0) as market_value,
                h.portfolio_weight
            from marts.fact_shareholder_holding h
            join selected_holding selected
                on selected.run_id = h.run_id
               and selected.filer_cik = h.filer_cik
        ),
        mart_agg as (
            select
                b.weight_band,
                count(*)::bigint as positions,
                round(sum(b.market_value), 6) as total_market_value,
                round(avg(b.portfolio_weight), 6) as avg_weight
            from mart_bucketed b
            group by b.weight_band
        )
        select
            r.weight_band,
            r.positions,
            r.total_market_value,
            r.avg_weight,
            array_remove(array[
                case
                    when m.weight_band is not null and r.positions is distinct from m.positions
                    then 'positions'
                end,
                case
                    when m.weight_band is not null
                     and r.total_market_value is distinct from m.total_market_value
                    then 'total_market_value'
                end,
                case
                    when m.weight_band is not null and r.avg_weight is distinct from m.avg_weight
                    then 'avg_weight'
                end
            ], null)::text[] as calculation_affected_columns
        from review_agg r
        left join mart_agg m
            on m.weight_band = r.weight_band
        order by r.band_rank
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (holding_review_row_id,))
            return list(cursor.fetchall())
