from __future__ import annotations

import csv
import sys
from datetime import date, datetime
from functools import cached_property
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from dagflow_pipeline_framework.contracts import PipelineRegistration
from dagflow_pipeline_framework.registry import PipelineRegistryRepository
from dagster import ConfigurableResource
from dagster_dbt import DbtCliResource
from psycopg.types.json import Jsonb

from dagflow_dagster.config import get_settings
from dagflow_dagster.dbt_topology import get_dbt_project
from dagflow_dagster.ingestion import EdgarIngestionService


class ControlPlaneResource(ConfigurableResource):
    direct_database_url: str
    export_root_dir: str
    edgar_identity: str
    sec_13f_lookback_days: int
    sec_13f_filing_limit: int
    sec_security_focus_limit: int
    openfigi_api_key: str | None = None
    finnhub_api_key: str | None = None

    @cached_property
    def ingestion(self) -> EdgarIngestionService:
        return EdgarIngestionService(
            edgar_identity=self.edgar_identity,
            sec_13f_lookback_days=self.sec_13f_lookback_days,
            sec_13f_filing_limit=self.sec_13f_filing_limit,
            sec_security_focus_limit=self.sec_security_focus_limit,
            openfigi_api_key=self.openfigi_api_key,
            finnhub_api_key=self.finnhub_api_key,
        )

    def enabled_pipelines(self) -> list[PipelineRegistration]:
        repository = PipelineRegistryRepository()
        pipelines = repository.list_pipelines(self.direct_database_url)
        return [
            pipeline for pipeline in pipelines if pipeline.is_enabled and pipeline.is_schedulable
        ]

    def pipeline_enabled(self, pipeline_code: str) -> bool:
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select is_enabled
                    from control.pipeline_registry
                    where pipeline_code = %s
                    """,
                    (pipeline_code,),
                )
                row = cursor.fetchone()
        if row is None:
            return False
        return bool(row[0])

    def current_business_date(self) -> date:
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("select current_date")
                row = cursor.fetchone()
        if row is None:
            raise RuntimeError("Unable to resolve current business date from the database")
        return row[0]

    @staticmethod
    def _normalize_csv_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.12g}"
        if isinstance(value, (UUID, date, datetime)):
            return str(value)
        return str(value)

    def last_pipeline_run_id(self, pipeline_code: str) -> UUID | None:
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select last_run_id
                    from control.pipeline_state
                    where pipeline_code = %s
                    """,
                    (pipeline_code,),
                )
                row = cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return row[0]

    def export_ready_runs(self, pipeline_code: str) -> list[dict[str, Any]]:
        query = """
        select
            review.pipeline_code,
            review.dataset_code,
            review.run_id,
            review.business_date,
            coalesce(review.review_completed_at, review.updated_at) as validated_at,
            registry.storage_prefix,
            registry.export_contract_code
        from workflow.dataset_review_state as review
        join control.pipeline_registry as registry
          on registry.pipeline_code = review.pipeline_code
        where review.pipeline_code = %s
          and review.review_state = 'approved'
        order by coalesce(review.review_completed_at, review.updated_at), review.business_date
        """
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query, (pipeline_code,))
                return list(cursor.fetchall())

    def finalize_export(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
        actor: str = "dagster",
        notes: str | None = None,
    ) -> dict[str, Any]:
        query = """
        select workflow.finalize_export(%s, %s, %s, %s, %s, %s) as payload
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        pipeline_code,
                        dataset_code,
                        run_id,
                        business_date,
                        actor,
                        notes,
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        if row is None:
            raise RuntimeError(
                f"workflow.finalize_export returned no payload for {pipeline_code} run {run_id}"
            )
        return row[0]

    def write_export_csv(
        self,
        pipeline_code: str,
        run_id: UUID,
        business_date: date,
    ) -> dict[str, Any]:
        export_queries: dict[str, tuple[str, list[str]]] = {
            "security_master": (
                """
                select
                    file_id,
                    business_date,
                    ticker,
                    issuer_name,
                    investable_shares,
                    review_materiality_score
                from export.security_master_preview
                where run_id = %s
                order by ticker
                """,
                [
                    "business_date",
                    "ticker",
                    "issuer_name",
                    "investable_shares",
                    "review_materiality_score",
                ],
            ),
            "shareholder_holdings": (
                """
                select
                    file_id,
                    business_date,
                    filer_name,
                    security_name,
                    shares_held,
                    holding_pct_of_outstanding
                from export.shareholder_holdings_preview
                where run_id = %s
                order by filer_name, security_name
                """,
                [
                    "business_date",
                    "filer_name",
                    "security_name",
                    "shares_held",
                    "holding_pct_of_outstanding",
                ],
            ),
        }
        query_details = export_queries.get(pipeline_code)
        if query_details is None:
            raise KeyError(f"Unsupported export pipeline: {pipeline_code}")

        config_query = """
        select storage_prefix, export_contract_code
        from control.pipeline_registry
        where pipeline_code = %s
        """
        export_query, fieldnames = query_details
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(config_query, (pipeline_code,))
                config = cursor.fetchone()
                cursor.execute(export_query, (run_id,))
                rows = list(cursor.fetchall())

        if config is None:
            raise RuntimeError(f"Missing pipeline registry config for {pipeline_code}")
        if not rows:
            raise RuntimeError(
                f"No export preview rows were available for {pipeline_code} run {run_id}"
            )

        file_id = str(rows[0]["file_id"])
        storage_prefix = str(config["storage_prefix"]).strip("/")
        export_dir = Path(self.export_root_dir) / storage_prefix / business_date.isoformat()
        export_dir.mkdir(parents=True, exist_ok=True)
        file_path = export_dir / file_id

        with file_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        fieldname: self._normalize_csv_value(row.get(fieldname))
                        for fieldname in fieldnames
                    }
                )

        return {
            "file_id": file_id,
            "file_path": str(file_path),
            "export_row_count": len(rows),
            "storage_prefix": storage_prefix,
            "export_contract_code": config["export_contract_code"],
        }

    def ensure_pipeline_run(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
        orchestrator_run_id: str,
    ) -> None:
        query = """
        insert into observability.pipeline_runs (
            run_id,
            pipeline_code,
            dataset_code,
            business_date,
            status,
            triggered_by,
            orchestrator_run_id,
            metadata
        )
        values (%s, %s, %s, %s, 'running', 'dagster', %s, %s)
        on conflict (run_id) do update
        set pipeline_code = excluded.pipeline_code,
            dataset_code = excluded.dataset_code,
            business_date = excluded.business_date,
            status = 'running',
            ended_at = null,
            orchestrator_run_id = excluded.orchestrator_run_id,
            metadata = observability.pipeline_runs.metadata || excluded.metadata
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        run_id,
                        pipeline_code,
                        dataset_code,
                        business_date,
                        orchestrator_run_id,
                        Jsonb(
                            {
                                "mode": "edgar_live",
                                "identifier_mapping": ["edgartools", "openfigi", "finnhub"],
                                "orchestrator": "dagster",
                                "orchestrator_run_id": orchestrator_run_id,
                            }
                        ),
                    ),
                )
            connection.commit()

    def load_security_master_tickers(self, run_id: UUID, business_date: date) -> int:
        rows = self.ingestion.build_security_master_tickers(business_date)
        return self.load_security_master_ticker_records(run_id, business_date, rows)

    def load_security_master_ticker_records(
        self, run_id: UUID, business_date: date, rows: list[dict[str, Any]]
    ) -> int:
        query = """
        insert into raw.sec_company_tickers (
            run_id,
            business_date,
            source_record_id,
            source_payload,
            source_payload_hash,
            source_file_name,
            source_file_row_number,
            cik,
            ticker,
            company_name,
            exchange,
            row_hash
        )
        values (
            %(run_id)s,
            %(business_date)s,
            %(source_record_id)s,
            %(source_payload)s,
            %(source_payload_hash)s,
            %(source_file_name)s,
            %(source_file_row_number)s,
            %(cik)s,
            %(ticker)s,
            %(company_name)s,
            %(exchange)s,
            %(row_hash)s
        )
        """
        payload = [{**row, "source_payload": Jsonb(row["source_payload"])} for row in rows]
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute("delete from raw.sec_company_tickers where run_id = %s", (run_id,))
                cursor.executemany(
                    query,
                    [
                        {
                            "run_id": run_id,
                            "business_date": business_date,
                            **row,
                        }
                        for row in payload
                    ],
                )
            connection.commit()
        return len(rows)

    def load_security_master_facts(self, run_id: UUID, business_date: date) -> int:
        rows = self.ingestion.build_security_master_facts(business_date)
        return self.load_security_master_fact_records(run_id, business_date, rows)

    def load_security_master_fact_records(
        self, run_id: UUID, business_date: date, rows: list[dict[str, Any]]
    ) -> int:
        query = """
        insert into raw.sec_company_facts (
            run_id,
            business_date,
            source_record_id,
            source_payload,
            source_payload_hash,
            source_file_name,
            source_file_row_number,
            cik,
            fact_name,
            fact_value,
            unit,
            row_hash
        )
        values (
            %(run_id)s,
            %(business_date)s,
            %(source_record_id)s,
            %(source_payload)s,
            %(source_payload_hash)s,
            %(source_file_name)s,
            %(source_file_row_number)s,
            %(cik)s,
            %(fact_name)s,
            %(fact_value)s,
            %(unit)s,
            %(row_hash)s
        )
        """
        payload = [{**row, "source_payload": Jsonb(row["source_payload"])} for row in rows]
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute("delete from raw.sec_company_facts where run_id = %s", (run_id,))
                cursor.executemany(
                    query,
                    [
                        {
                            "run_id": run_id,
                            "business_date": business_date,
                            **row,
                        }
                        for row in payload
                    ],
                )
            connection.commit()
        return len(rows)

    def load_shareholder_filers(self, run_id: UUID, business_date: date) -> int:
        rows = self.ingestion.build_shareholder_filers(business_date)
        return self.load_shareholder_filer_records(run_id, business_date, rows)

    def load_shareholder_filer_records(
        self, run_id: UUID, business_date: date, rows: list[dict[str, Any]]
    ) -> int:
        query = """
        insert into raw.holdings_13f_filers (
            run_id,
            business_date,
            source_record_id,
            source_payload,
            source_payload_hash,
            source_file_name,
            source_file_row_number,
            accession_number,
            filer_cik,
            filer_name,
            report_period,
            row_hash
        )
        values (
            %(run_id)s,
            %(business_date)s,
            %(source_record_id)s,
            %(source_payload)s,
            %(source_payload_hash)s,
            %(source_file_name)s,
            %(source_file_row_number)s,
            %(accession_number)s,
            %(filer_cik)s,
            %(filer_name)s,
            %(report_period)s,
            %(row_hash)s
        )
        """
        payload = [{**row, "source_payload": Jsonb(row["source_payload"])} for row in rows]
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute("delete from raw.holdings_13f_filers where run_id = %s", (run_id,))
                cursor.executemany(
                    query,
                    [
                        {
                            "run_id": run_id,
                            "business_date": business_date,
                            **row,
                        }
                        for row in payload
                    ],
                )
            connection.commit()
        return len(rows)

    def load_shareholder_holdings(self, run_id: UUID, business_date: date) -> int:
        rows = self.ingestion.build_shareholder_holdings(business_date)
        return self.load_shareholder_holding_records(run_id, business_date, rows)

    def load_shareholder_holding_records(
        self, run_id: UUID, business_date: date, rows: list[dict[str, Any]]
    ) -> int:
        query = """
        insert into raw.holdings_13f (
            run_id,
            business_date,
            source_record_id,
            source_payload,
            source_payload_hash,
            source_file_name,
            source_file_row_number,
            accession_number,
            filer_cik,
            security_identifier,
            cusip,
            shares_held,
            market_value,
            row_hash
        )
        values (
            %(run_id)s,
            %(business_date)s,
            %(source_record_id)s,
            %(source_payload)s,
            %(source_payload_hash)s,
            %(source_file_name)s,
            %(source_file_row_number)s,
            %(accession_number)s,
            %(filer_cik)s,
            %(security_identifier)s,
            %(cusip)s,
            %(shares_held)s,
            %(market_value)s,
            %(row_hash)s
        )
        """
        payload = [{**row, "source_payload": Jsonb(row["source_payload"])} for row in rows]
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute("delete from raw.holdings_13f where run_id = %s", (run_id,))
                cursor.executemany(
                    query,
                    [
                        {
                            "run_id": run_id,
                            "business_date": business_date,
                            **row,
                        }
                        for row in payload
                    ],
                )
            connection.commit()
        return len(rows)

    def reset_pipeline_run_data(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
        actor: str = "system",
        notes: str | None = None,
    ) -> dict[str, Any]:
        query = """
        select workflow.reset_pipeline_run_data(%s, %s, %s, %s, %s, %s) as payload
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (pipeline_code, dataset_code, run_id, business_date, actor, notes),
                )
                row = cursor.fetchone()
            connection.commit()
        if row is None:
            return {}
        return row[0]

    def publish_review_snapshot(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
        actor: str = "dagster",
        notes: str | None = None,
    ) -> dict[str, Any]:
        query = """
        select workflow.publish_review_snapshot(%s, %s, %s, %s, %s, %s) as payload
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        pipeline_code,
                        dataset_code,
                        run_id,
                        business_date,
                        actor,
                        notes,
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        review_count = self._review_row_count(dataset_code, run_id)
        if review_count == 0:
            raise RuntimeError(
                "workflow.publish_review_snapshot produced no review rows for "
                f"{dataset_code} run {run_id}"
            )
        if row is None:
            raise RuntimeError(
                "workflow.publish_review_snapshot returned no payload for "
                f"{dataset_code} run {run_id}"
            )
        self._record_review_lineage(pipeline_code, dataset_code, run_id, business_date)
        return row[0]

    def publish_historical_review_snapshot(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
        *,
        security_run_id: UUID | None = None,
        approval_state: str = "approved",
    ) -> int:
        queries = {
            "security_master": """
                insert into review.security_master_daily (
                    run_id,
                    business_date,
                    origin_mart_row_id,
                    origin_mart_row_hash,
                    current_row_hash,
                    row_version,
                    source_record_id,
                    cik,
                    ticker,
                    issuer_name,
                    exchange,
                    shares_outstanding_raw,
                    free_float_pct_raw,
                    investability_factor_raw,
                    shares_outstanding_override,
                    free_float_pct_override,
                    investability_factor_override,
                    free_float_shares,
                    investable_shares,
                    review_materiality_score,
                    approval_state,
                    edited_cells,
                    manual_edit_count,
                    review_notes
                )
                select
                    %(run_id)s,
                    %(business_date)s,
                    security_id,
                    row_hash,
                    row_hash,
                    1,
                    source_record_id,
                    cik,
                    ticker,
                    issuer_name,
                    exchange,
                    shares_outstanding_raw,
                    free_float_pct_raw,
                    investability_factor_raw,
                    null,
                    null,
                    null,
                    free_float_shares,
                    investable_shares,
                    review_materiality_score,
                    %(approval_state)s,
                    '{}'::jsonb,
                    0,
                    'Historical backfill snapshot'
                from marts.dim_security
                where run_id = %(run_id)s
            """,
            "shareholder_holdings": """
                insert into review.shareholder_holdings_daily (
                    run_id,
                    business_date,
                    origin_mart_row_id,
                    origin_mart_row_hash,
                    current_row_hash,
                    row_version,
                    source_record_id,
                    accession_number,
                    filer_cik,
                    filer_name,
                    security_identifier,
                    security_name,
                    security_review_row_id,
                    shares_held_raw,
                    reviewed_market_value_raw,
                    source_confidence_raw,
                    shares_held_override,
                    reviewed_market_value_override,
                    source_confidence_override,
                    holding_pct_of_outstanding,
                    derived_price_per_share,
                    portfolio_weight,
                    approval_state,
                    edited_cells,
                    manual_edit_count,
                    review_notes
                )
                select
                    %(run_id)s,
                    %(business_date)s,
                    h.holding_id,
                    h.row_hash,
                    h.row_hash,
                    1,
                    h.source_record_id,
                    h.accession_number,
                    h.filer_cik,
                    h.filer_name,
                    h.security_identifier,
                    h.security_name,
                    s.review_row_id,
                    h.shares_held_raw,
                    h.reviewed_market_value_raw,
                    h.source_confidence_raw,
                    null,
                    null,
                    null,
                    h.holding_pct_of_outstanding,
                    h.derived_price_per_share,
                    h.portfolio_weight,
                    %(approval_state)s,
                    '{}'::jsonb,
                    0,
                    'Historical backfill snapshot'
                from marts.fact_shareholder_holding h
                left join review.security_master_daily s
                  on s.run_id = %(security_run_id)s
                 and s.ticker = h.security_identifier
                where h.run_id = %(run_id)s
            """,
        }
        delete_queries = {
            "security_master": "delete from review.security_master_daily where run_id = %s",
            "shareholder_holdings": "delete from review.shareholder_holdings_daily where run_id = %s",
        }
        query = queries.get(dataset_code)
        delete_query = delete_queries.get(dataset_code)
        if query is None or delete_query is None:
            raise KeyError(f"Unsupported review snapshot dataset: {dataset_code}")

        payload = {
            "run_id": run_id,
            "business_date": business_date,
            "security_run_id": security_run_id,
            "approval_state": approval_state,
        }
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(delete_query, (run_id,))
                cursor.execute(query, payload)
            connection.commit()
        self._record_review_lineage(pipeline_code, dataset_code, run_id, business_date)
        return self._review_row_count(dataset_code, run_id)

    def clone_historical_review_snapshot(
        self,
        dataset_code: str,
        source_run_id: UUID,
        target_run_id: UUID,
        business_date: date,
        *,
        security_target_run_id: UUID | None = None,
        approval_state: str = "approved",
    ) -> int:
        queries = {
            "security_master": """
                insert into review.security_master_daily (
                    run_id,
                    business_date,
                    origin_mart_row_id,
                    origin_mart_row_hash,
                    current_row_hash,
                    row_version,
                    source_record_id,
                    cik,
                    ticker,
                    issuer_name,
                    exchange,
                    shares_outstanding_raw,
                    free_float_pct_raw,
                    investability_factor_raw,
                    shares_outstanding_override,
                    free_float_pct_override,
                    investability_factor_override,
                    free_float_shares,
                    investable_shares,
                    review_materiality_score,
                    approval_state,
                    edited_cells,
                    manual_edit_count,
                    review_notes
                )
                select
                    %(target_run_id)s,
                    %(business_date)s,
                    origin_mart_row_id,
                    origin_mart_row_hash,
                    current_row_hash,
                    1,
                    source_record_id,
                    cik,
                    ticker,
                    issuer_name,
                    exchange,
                    shares_outstanding_raw,
                    free_float_pct_raw,
                    investability_factor_raw,
                    shares_outstanding_override,
                    free_float_pct_override,
                    investability_factor_override,
                    free_float_shares,
                    investable_shares,
                    review_materiality_score,
                    %(approval_state)s,
                    '{}'::jsonb,
                    0,
                    'Historical backfill carried forward'
                from review.security_master_daily
                where run_id = %(source_run_id)s
            """,
            "shareholder_holdings": """
                insert into review.shareholder_holdings_daily (
                    run_id,
                    business_date,
                    origin_mart_row_id,
                    origin_mart_row_hash,
                    current_row_hash,
                    row_version,
                    source_record_id,
                    accession_number,
                    filer_cik,
                    filer_name,
                    security_identifier,
                    security_name,
                    security_review_row_id,
                    shares_held_raw,
                    reviewed_market_value_raw,
                    source_confidence_raw,
                    shares_held_override,
                    reviewed_market_value_override,
                    source_confidence_override,
                    holding_pct_of_outstanding,
                    derived_price_per_share,
                    portfolio_weight,
                    approval_state,
                    edited_cells,
                    manual_edit_count,
                    review_notes
                )
                select
                    %(target_run_id)s,
                    %(business_date)s,
                    h.origin_mart_row_id,
                    h.origin_mart_row_hash,
                    h.current_row_hash,
                    1,
                    h.source_record_id,
                    h.accession_number,
                    h.filer_cik,
                    h.filer_name,
                    h.security_identifier,
                    h.security_name,
                    s.review_row_id,
                    h.shares_held_raw,
                    h.reviewed_market_value_raw,
                    h.source_confidence_raw,
                    h.shares_held_override,
                    h.reviewed_market_value_override,
                    h.source_confidence_override,
                    h.holding_pct_of_outstanding,
                    h.derived_price_per_share,
                    h.portfolio_weight,
                    %(approval_state)s,
                    '{}'::jsonb,
                    0,
                    'Historical backfill carried forward'
                from review.shareholder_holdings_daily h
                left join review.security_master_daily s
                  on s.run_id = %(security_target_run_id)s
                 and s.ticker = h.security_identifier
                where h.run_id = %(source_run_id)s
            """,
        }
        delete_queries = {
            "security_master": "delete from review.security_master_daily where run_id = %s",
            "shareholder_holdings": "delete from review.shareholder_holdings_daily where run_id = %s",
        }
        query = queries.get(dataset_code)
        delete_query = delete_queries.get(dataset_code)
        if query is None or delete_query is None:
            raise KeyError(f"Unsupported review snapshot dataset: {dataset_code}")

        payload = {
            "source_run_id": source_run_id,
            "target_run_id": target_run_id,
            "business_date": business_date,
            "security_target_run_id": security_target_run_id,
            "approval_state": approval_state,
        }
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(delete_query, (target_run_id,))
                cursor.execute(query, payload)
            connection.commit()
        return self._review_row_count(dataset_code, target_run_id)

    def complete_pipeline_run(
        self,
        run_id: UUID,
        status: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        query = """
        update observability.pipeline_runs
        set status = %s,
            ended_at = timezone('utc', now()),
            metadata = metadata || %s
        where run_id = %s
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (status, Jsonb(metadata or {}), run_id))
            connection.commit()

    def capture_failure(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        step_name: str,
        stage: str,
        business_date: date,
        error: Exception,
        source_context: dict[str, Any] | None = None,
        diagnostic_metadata: dict[str, Any] | None = None,
    ) -> None:
        query = """
        select observability.capture_failure(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        pipeline_code,
                        dataset_code,
                        run_id,
                        step_name,
                        stage,
                        business_date,
                        Jsonb(source_context or {}),
                        error.__class__.__name__,
                        str(error),
                        Jsonb(diagnostic_metadata or {}),
                        Jsonb([]),
                    ),
                )
                cursor.execute(
                    """
                    update observability.pipeline_runs
                    set status = 'failed',
                        ended_at = timezone('utc', now()),
                        metadata = metadata || %s
                    where run_id = %s
                    """,
                    (
                        Jsonb(
                            {
                                "failed_step": step_name,
                                "failed_stage": stage,
                                "error_class": error.__class__.__name__,
                            }
                        ),
                        run_id,
                    ),
                )
            connection.commit()

    def _record_review_lineage(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
    ) -> None:
        queries: dict[str, str] = {
            "security_master": """
                insert into lineage.row_lineage_edges (
                    pipeline_code,
                    dataset_code,
                    run_id,
                    business_date,
                    from_layer,
                    from_table,
                    from_row_hash,
                    to_layer,
                    to_table,
                    to_row_hash,
                    relationship_type,
                    metadata
                )
                select
                    'security_master',
                    'security_master',
                    %s,
                    %s,
                    'raw',
                    'raw.sec_company_tickers',
                    r.row_hash,
                    'review',
                    'review.security_master_daily',
                    s.current_row_hash,
                    'published_to_review',
                    jsonb_build_object('cik', s.cik, 'ticker', s.ticker)
                from review.security_master_daily s
                join raw.sec_company_tickers r
                  on r.run_id = s.run_id
                 and r.cik = s.cik
                 and r.ticker = s.ticker
                where s.run_id = %s
            """,
            "shareholder_holdings": """
                insert into lineage.row_lineage_edges (
                    pipeline_code,
                    dataset_code,
                    run_id,
                    business_date,
                    from_layer,
                    from_table,
                    from_row_hash,
                    to_layer,
                    to_table,
                    to_row_hash,
                    relationship_type,
                    metadata
                )
                select
                    'shareholder_holdings',
                    'shareholder_holdings',
                    %s,
                    %s,
                    'raw',
                    'raw.holdings_13f',
                    r.row_hash,
                    'review',
                    'review.shareholder_holdings_daily',
                    h.current_row_hash,
                    'published_to_review',
                    jsonb_build_object(
                        'accession_number', h.accession_number,
                        'security_identifier', h.security_identifier
                    )
                from review.shareholder_holdings_daily h
                join raw.holdings_13f r
                  on r.run_id = h.run_id
                 and r.accession_number = h.accession_number
                 and r.filer_cik = h.filer_cik
                 and r.security_identifier = h.security_identifier
                where h.run_id = %s
            """,
        }
        query = queries.get(pipeline_code)
        if query is None:
            return
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    delete from lineage.row_lineage_edges
                    where pipeline_code = %s
                      and dataset_code = %s
                      and run_id = %s
                      and relationship_type = 'published_to_review'
                    """,
                    (pipeline_code, dataset_code, run_id),
                )
                cursor.execute(query, (run_id, business_date, run_id))
            connection.commit()

    def _review_row_count(self, dataset_code: str, run_id: UUID) -> int:
        queries = {
            "security_master": (
                "select count(*) from review.security_master_daily where run_id = %s"
            ),
            "shareholder_holdings": (
                "select count(*) from review.shareholder_holdings_daily where run_id = %s"
            ),
        }
        query = queries.get(dataset_code)
        if query is None:
            return 0
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (run_id,))
                row = cursor.fetchone()
        if row is None:
            return 0
        return int(row[0])

    def review_row_count(self, dataset_code: str, run_id: UUID) -> int:
        return self._review_row_count(dataset_code, run_id)


def build_resources() -> dict[str, ConfigurableResource | DbtCliResource]:
    settings = get_settings()
    dbt_executable = Path(sys.executable).with_name("dbt")
    dbt_project = get_dbt_project()
    return {
        "control_plane": ControlPlaneResource(
            direct_database_url=settings.direct_database_url,
            export_root_dir=settings.export_root_dir,
            edgar_identity=settings.edgar_identity,
            sec_13f_lookback_days=settings.sec_13f_lookback_days,
            sec_13f_filing_limit=settings.sec_13f_filing_limit,
            sec_security_focus_limit=settings.sec_security_focus_limit,
            openfigi_api_key=settings.openfigi_api_key,
            finnhub_api_key=settings.finnhub_api_key,
        ),
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            dbt_executable=str(dbt_executable),
        ),
    }
