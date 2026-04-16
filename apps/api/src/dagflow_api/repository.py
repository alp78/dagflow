from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import date
from typing import Any
from uuid import UUID

from psycopg.sql import SQL, Identifier
from psycopg.types.json import Jsonb

from dagflow_api.db import Database
from dagflow_api.schemas import (
    DashboardChartPoint,
    DashboardFilerLeader,
    DashboardMetric,
    DashboardOverview,
    DashboardSecurityLeader,
    DashboardSnapshot,
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


def _bucket_points(values: list[int], *, upper_bounds: list[int]) -> list[DashboardChartPoint]:
    clean_values = [int(value) for value in values]
    if not clean_values:
        return []

    counts = [0] * len(upper_bounds)
    for value in clean_values:
        for index, upper_bound in enumerate(upper_bounds):
            lower_bound = 1 if index == 0 else upper_bounds[index - 1] + 1
            if value <= upper_bound:
                counts[index] += 1
                break
        else:
            counts[-1] += 1

    total = len(clean_values)
    points: list[DashboardChartPoint] = []
    for index, upper_bound in enumerate(upper_bounds):
        lower_bound = 1 if index == 0 else upper_bounds[index - 1] + 1
        if index < len(upper_bounds) - 1:
            label = f"{lower_bound}-{upper_bound}"
        else:
            label = f"{lower_bound}+"
        count = counts[index]
        points.append(
            DashboardChartPoint(
                label=label,
                value=float(count),
                display_value=str(count),
                percentage=(count / total * 100) if total else None,
                color=DASHBOARD_COLORS[index % len(DASHBOARD_COLORS)],
            )
        )
    return points


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

    def list_review_rows(
        self,
        dataset_code: str,
        run_id: UUID | None,
        business_date: date | None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
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
        query = SQL("select * from {}.{} {} order by review_row_id limit %s").format(
            Identifier(schema_name), Identifier(table_name), SQL(where_sql)
        )
        params.append(limit)
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, params)
            return list(cursor.fetchall())

    def list_review_snapshots(
        self,
        dataset_code: str,
        limit: int = 5000,
    ) -> list[ReviewSnapshotSummary]:
        review_table = self.resolve_review_table(dataset_code)
        schema_name, table_name = review_table.split(".", maxsplit=1)
        query = SQL(
            """
            with snapshot_rows as (
                select
                    run_id,
                    business_date,
                    count(*)::bigint as row_count,
                    max(updated_at) as updated_at
                from {}.{}
                group by run_id, business_date
            ),
            current_snapshot as (
                select
                    run_id,
                    business_date
                from snapshot_rows
                order by business_date desc, updated_at desc nulls last, run_id desc
                limit 1
            )
            select
                snapshot_rows.run_id,
                snapshot_rows.business_date,
                coalesce(review.review_state, runs.status, 'pending_review') as review_state,
                snapshot_rows.row_count,
                snapshot_rows.updated_at,
                coalesce(
                    current_snapshot.run_id = snapshot_rows.run_id
                    and current_snapshot.business_date = snapshot_rows.business_date,
                    false
                ) as is_current
            from snapshot_rows
            left join workflow.dataset_review_state review
                on review.dataset_code = %s
               and review.run_id = snapshot_rows.run_id
            left join observability.pipeline_runs runs
                on runs.dataset_code = %s
               and runs.run_id = snapshot_rows.run_id
            left join current_snapshot
                on true
            order by
                snapshot_rows.business_date desc,
                snapshot_rows.updated_at desc nulls last,
                snapshot_rows.run_id desc
            limit %s
            """
        ).format(Identifier(schema_name), Identifier(table_name))
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (dataset_code, dataset_code, limit))
            rows = cursor.fetchall()
        return [ReviewSnapshotSummary.model_validate(row) for row in rows]

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
        review_table = self.resolve_review_table(dataset_code)
        schema_name, table_name = review_table.split(".", maxsplit=1)
        query = SQL(
            """
            with snapshot_rows as (
                select
                    run_id,
                    business_date,
                    max(updated_at) as updated_at
                from {}.{}
                group by run_id, business_date
            )
            select run_id, business_date
            from snapshot_rows
            order by business_date desc, updated_at desc nulls last, run_id desc
            limit 1
            """
        ).format(Identifier(schema_name), Identifier(table_name))
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
        if row is None:
            raise PermissionError(f"No review snapshots exist for {dataset_code}.")
        if row["run_id"] != run_id or row["business_date"] != business_date:
            raise PermissionError(
                "Historical review snapshots are read-only. "
                "Only the current daily snapshot can be edited or validated."
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

    def reset_demo_runs(self, actor: str, notes: str | None) -> dict[str, Any]:
        query = "select workflow.reset_demo_runs(%s, %s) as payload"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (actor, notes))
            row = cursor.fetchone()
            connection.commit()
        return row["payload"]

    def get_dashboard_overview(self) -> DashboardOverview:
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute("select count(*) as count from control.pipeline_registry")
            pipeline_count = cursor.fetchone()["count"]
            cursor.execute(
                "select count(*) as count from control.pipeline_registry where is_enabled"
            )
            enabled_pipeline_count = cursor.fetchone()["count"]
            cursor.execute(
                """
                select count(*) as count
                from control.pipeline_state
                where approval_state in ('pending_review', 'in_review', 'reopened')
                """
            )
            pending_reviews = cursor.fetchone()["count"]
            cursor.execute(
                """
                with latest_runs as (
                    select distinct on (pipeline_code, dataset_code)
                        pipeline_code,
                        dataset_code,
                        status
                    from observability.pipeline_runs
                    order by pipeline_code, dataset_code, started_at desc
                )
                select count(*) as count
                from latest_runs
                where status = 'failed'
                """
            )
            failure_count = cursor.fetchone()["count"]
            cursor.execute(
                """
                select pipeline_code, dataset_code, run_id, business_date, status, started_at
                from observability.pipeline_runs
                order by started_at desc
                limit 10
                """
            )
            recent_runs = list(cursor.fetchall())
        return DashboardOverview(
            pipeline_count=pipeline_count,
            enabled_pipeline_count=enabled_pipeline_count,
            pending_reviews=pending_reviews,
            failure_count=failure_count,
            recent_runs=recent_runs,
        )

    def get_dashboard_snapshot(self) -> DashboardSnapshot:
        overview = self.get_dashboard_overview()
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                select pipeline_code, last_run_id, last_business_date
                from control.pipeline_state
                where pipeline_code in ('security_master', 'shareholder_holdings')
                """
            )
            state_rows = {row["pipeline_code"]: row for row in cursor.fetchall()}

            security_state = state_rows.get("security_master")
            holdings_state = state_rows.get("shareholder_holdings")
            security_run_id = security_state["last_run_id"] if security_state else None
            holdings_run_id = holdings_state["last_run_id"] if holdings_state else None
            latest_business_date = None
            if security_state and security_state["last_business_date"] is not None:
                latest_business_date = security_state["last_business_date"]
            elif holdings_state and holdings_state["last_business_date"] is not None:
                latest_business_date = holdings_state["last_business_date"]

            security_rows: list[dict[str, Any]] = []
            holdings_rows: list[dict[str, Any]] = []

            if security_run_id is not None:
                cursor.execute(
                    """
                    select
                        ticker,
                        issuer_name,
                        exchange,
                        coalesce(investable_shares, 0)::double precision as investable_shares,
                        coalesce(review_materiality_score, 0)::double precision
                            as review_materiality_score,
                        approval_state,
                        coalesce(manual_edit_count, 0) as manual_edit_count
                    from review.security_master_daily
                    where run_id = %s
                    """,
                    (security_run_id,),
                )
                security_rows = list(cursor.fetchall())

            if holdings_run_id is not None:
                cursor.execute(
                    """
                    select
                        filer_name,
                        security_identifier,
                        coalesce(
                            reviewed_market_value_override,
                            reviewed_market_value_raw,
                            0
                        )::double precision
                            as reviewed_market_value,
                        coalesce(portfolio_weight, 0)::double precision as portfolio_weight,
                        approval_state,
                        coalesce(manual_edit_count, 0) as manual_edit_count
                    from review.shareholder_holdings_daily
                    where run_id = %s
                    """,
                    (holdings_run_id,),
                )
                holdings_rows = list(cursor.fetchall())

        exchange_counter = Counter(
            (row["exchange"] or "Unknown") for row in security_rows
        )
        security_approval_counter = Counter(
            (row["approval_state"] or "unknown") for row in security_rows
        )
        holdings_approval_counter = Counter(
            (row["approval_state"] or "unknown") for row in holdings_rows
        )

        holder_count_by_security = Counter(
            (row["security_identifier"] or "Unknown") for row in holdings_rows
        )
        total_market_value_by_security: defaultdict[str, float] = defaultdict(float)
        filer_rollups: dict[str, dict[str, Any]] = {}
        for row in holdings_rows:
            security_identifier = row["security_identifier"] or "Unknown"
            total_market_value_by_security[security_identifier] += _to_float(
                row["reviewed_market_value"]
            )

            filer_name = row["filer_name"] or "Unknown filer"
            filer_rollup = filer_rollups.setdefault(
                filer_name,
                {
                    "filer_name": filer_name,
                    "position_count": 0,
                    "total_market_value": 0.0,
                    "distinct_securities": set(),
                },
            )
            filer_rollup["position_count"] += 1
            filer_rollup["total_market_value"] += _to_float(row["reviewed_market_value"])
            filer_rollup["distinct_securities"].add(security_identifier)

        security_lookup = {
            row["ticker"]: row for row in security_rows if row.get("ticker") is not None
        }
        security_scores = [_to_float(row["review_materiality_score"]) for row in security_rows]
        holders_per_security = list(holder_count_by_security.values())
        portfolio_weights = [_to_float(row["portfolio_weight"]) for row in holdings_rows]

        sorted_by_materiality = sorted(
            security_rows,
            key=lambda row: _to_float(row["review_materiality_score"]),
            reverse=True,
        )
        top_materiality_securities = [
            DashboardSecurityLeader(
                ticker=str(row["ticker"]),
                issuer_name=str(row["issuer_name"]),
                exchange=str(row["exchange"]),
                materiality_score=_to_float(row["review_materiality_score"]),
                investable_shares=_to_float(row["investable_shares"]),
                holder_count=holder_count_by_security.get(str(row["ticker"]), 0),
                total_market_value=total_market_value_by_security.get(str(row["ticker"]), 0.0),
            )
            for row in sorted_by_materiality[:8]
        ]

        top_securities_by_holders = []
        for ticker, holder_count in holder_count_by_security.most_common(8):
            security_row = security_lookup.get(ticker, {})
            top_securities_by_holders.append(
                DashboardSecurityLeader(
                    ticker=ticker,
                    issuer_name=str(security_row.get("issuer_name", ticker)),
                    exchange=str(security_row.get("exchange", "Unknown")),
                    materiality_score=_to_float(security_row.get("review_materiality_score")),
                    investable_shares=_to_float(security_row.get("investable_shares")),
                    holder_count=holder_count,
                    total_market_value=total_market_value_by_security.get(ticker, 0.0),
                )
            )

        top_filers = [
            DashboardFilerLeader(
                filer_name=str(filer["filer_name"]),
                position_count=int(filer["position_count"]),
                distinct_securities=len(filer["distinct_securities"]),
                total_market_value=_to_float(filer["total_market_value"]),
            )
            for filer in sorted(
                filer_rollups.values(),
                key=lambda item: (
                    int(item["position_count"]),
                    _to_float(item["total_market_value"]),
                ),
                reverse=True,
            )[:8]
        ]

        security_metrics = [
            _metric("Securities in review", float(len(security_rows))),
            _metric("Distinct exchanges", float(len(exchange_counter))),
            _metric(
                "Edited rows",
                float(sum(1 for row in security_rows if int(row["manual_edit_count"]) > 0)),
            ),
            _metric(
                "Investable shares",
                sum(_to_float(row["investable_shares"]) for row in security_rows),
            ),
            _metric(
                "Median materiality",
                sorted(security_scores)[len(security_scores) // 2] if security_scores else 0.0,
            ),
        ]

        distinct_securities = max(1, len(holder_count_by_security))
        holdings_metrics = [
            _metric("Holder rows", float(len(holdings_rows))),
            _metric(
                "Distinct filers",
                float(len({row["filer_name"] for row in holdings_rows if row["filer_name"]})),
            ),
            _metric(
                "Avg holders / security",
                (len(holdings_rows) / distinct_securities) if holdings_rows else 0.0,
                note="Breadth of 13F ownership in the latest run.",
            ),
            _metric(
                "Edited rows",
                float(sum(1 for row in holdings_rows if int(row["manual_edit_count"]) > 0)),
            ),
            _metric(
                "Reviewed market value",
                sum(_to_float(row["reviewed_market_value"]) for row in holdings_rows),
            ),
        ]

        return DashboardSnapshot(
            overview=overview,
            latest_business_date=latest_business_date,
            security_run_id=security_run_id,
            holdings_run_id=holdings_run_id,
            security_metrics=security_metrics,
            holdings_metrics=holdings_metrics,
            exchange_distribution=_distribution_points(exchange_counter),
            materiality_histogram=_histogram_points(security_scores, bins=8, log_scale=True),
            security_approval_distribution=_distribution_points(security_approval_counter),
            holdings_approval_distribution=_distribution_points(holdings_approval_counter),
            holders_per_security_histogram=_bucket_points(
                holders_per_security,
                upper_bounds=[5, 10, 15, 20, 25, 30],
            ),
            portfolio_weight_histogram=_histogram_points(
                portfolio_weights,
                bins=8,
                percent=True,
            ),
            top_materiality_securities=top_materiality_securities,
            top_securities_by_holders=top_securities_by_holders,
            top_filers=top_filers,
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

    def get_security_shareholder_breakdown(
        self, security_review_row_id: int
    ) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_security_shareholder_breakdown(%s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (security_review_row_id,))
            return list(cursor.fetchall())

    def get_security_holder_concentration(
        self, security_review_row_id: int
    ) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_security_holder_concentration(%s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (security_review_row_id,))
            return list(cursor.fetchall())

    def get_security_holder_approval_mix(
        self, security_review_row_id: int
    ) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_security_holder_approval_mix(%s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (security_review_row_id,))
            return list(cursor.fetchall())

    def get_security_history(self, security_review_row_id: int) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_security_history(%s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (security_review_row_id,))
            return list(cursor.fetchall())

    def get_holding_peer_holders(self, holding_review_row_id: int) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_holding_peer_holders(%s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (holding_review_row_id,))
            return list(cursor.fetchall())

    def get_filer_portfolio_snapshot(self, holding_review_row_id: int) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_filer_portfolio_snapshot(%s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (holding_review_row_id,))
            return list(cursor.fetchall())

    def get_filer_weight_bands(self, holding_review_row_id: int) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_filer_weight_bands(%s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (holding_review_row_id,))
            return list(cursor.fetchall())

    def list_security_dimension(self) -> list[dict[str, Any]]:
        query = """
        select
            review_row_id,
            ticker,
            issuer_name,
            exchange,
            investable_shares,
            review_materiality_score,
            approval_state
        from review.security_master_daily
        order by review_materiality_score desc nulls last
        limit 50
        """
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query)
            return list(cursor.fetchall())
