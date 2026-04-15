from __future__ import annotations

from datetime import date
from typing import Any
from uuid import UUID

from psycopg.sql import SQL, Identifier
from psycopg.types.json import Jsonb

from dagflow_api.db import Database
from dagflow_api.schemas import DashboardOverview, PipelineView


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

    def apply_cell_edit(self, payload: dict[str, Any]) -> dict[str, Any]:
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
        query = "select calc.recalc_security_review_fields(%s, %s) as payload"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (row_id, changed_by))
            row = cursor.fetchone()
            connection.commit()
        return row["payload"]

    def recalc_holding(self, row_id: int, changed_by: str) -> dict[str, Any]:
        query = "select calc.recalc_holding_review_fields(%s, %s) as payload"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (row_id, changed_by))
            row = cursor.fetchone()
            connection.commit()
        return row["payload"]

    def call_workflow_action(self, function_name: str, payload: dict[str, Any]) -> dict[str, Any]:
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
                from workflow.dataset_review_state
                where review_state in ('pending_review', 'in_review', 'reopened')
                """
            )
            pending_reviews = cursor.fetchone()["count"]
            cursor.execute("select count(*) as count from observability.pipeline_failures")
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

    def get_security_history(self, security_review_row_id: int) -> list[dict[str, Any]]:
        query = "select * from query_api.fn_security_history(%s)"
        with self.database.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (security_review_row_id,))
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
