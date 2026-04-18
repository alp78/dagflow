from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
from datetime import UTC, date, datetime
from functools import cached_property
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from dagflow_pipeline_framework.contracts import PipelineRegistration
from dagflow_pipeline_framework.registry import PipelineRegistryRepository
from dagflow_source_adapters.registry import get_adapter
from dagster import ConfigurableResource
from dagster_dbt import DbtCliResource
from psycopg.types.json import Jsonb

from dagflow_dagster.config import get_settings
from dagflow_dagster.dbt_topology import get_dbt_project
from dagflow_dagster.execution import (
    export_step_key,
    pipeline_step_definition,
    pipeline_step_definitions,
)
from dagflow_dagster.ingestion import EdgarIngestionService

SOURCE_DESCRIPTOR_INDEX: dict[str, tuple[str, int]] = {
    "sec_company_tickers": ("sec_json", 0),
    "sec_company_facts": ("sec_json", 1),
    "holdings_13f": ("sec_13f", 0),
    "holdings_13f_filers": ("sec_13f", 1),
}

CAPTURE_FIELDNAMES: dict[str, list[str]] = {
    "sec_company_tickers": [
        "source_record_id",
        "cik",
        "ticker",
        "company_name",
        "exchange",
        "holder_count_recent_window",
        "reported_shares_recent_window",
        "top_holder_shares_recent_window",
        "source_payload_hash",
        "row_hash",
    ],
    "sec_company_facts": [
        "source_record_id",
        "cik",
        "fact_name",
        "fact_value",
        "unit",
        "retrieval_status",
        "source_payload_hash",
        "row_hash",
    ],
    "holdings_13f_filers": [
        "source_record_id",
        "accession_number",
        "filer_cik",
        "filer_name",
        "report_period",
        "filing_date",
        "focus_holdings_count",
        "source_payload_hash",
        "row_hash",
    ],
    "holdings_13f": [
        "source_record_id",
        "accession_number",
        "filer_cik",
        "filer_name",
        "report_period",
        "issuer_name",
        "resolved_ticker",
        "ticker_source",
        "ticker_sources",
        "cusip",
        "shares_held",
        "market_value",
        "holder_count_recent_window",
        "top_holder_shares_recent_window",
        "reported_shares_recent_window",
        "aggregated_rows",
        "source_payload_hash",
        "row_hash",
    ],
}


class ControlPlaneResource(ConfigurableResource):
    direct_database_url: str
    export_root_dir: str
    landing_root_dir: str
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

    @staticmethod
    def _json_ready(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                str(key): ControlPlaneResource._json_ready(item)
                for key, item in value.items()
            }
        if isinstance(value, list | tuple):
            return [ControlPlaneResource._json_ready(item) for item in value]
        if isinstance(value, datetime | date):
            return value.isoformat()
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, Path):
            return str(value)
        return value

    def enabled_pipelines(self) -> list[PipelineRegistration]:
        repository = PipelineRegistryRepository()
        pipelines = repository.list_pipelines(self.direct_database_url)
        return [
            pipeline for pipeline in pipelines if pipeline.is_enabled and pipeline.is_schedulable
        ]

    def pipeline_registration(self, pipeline_code: str) -> PipelineRegistration:
        repository = PipelineRegistryRepository()
        pipelines = repository.list_pipelines(self.direct_database_url)
        for pipeline in pipelines:
            if pipeline.pipeline_code == pipeline_code:
                return pipeline
        raise KeyError(f"Unknown pipeline code: {pipeline_code}")

    def source_descriptors_for_pipeline(
        self, pipeline_code: str, business_date: date
    ) -> list[Any]:
        registration = self.pipeline_registration(pipeline_code)
        adapter = get_adapter(registration.adapter_type)
        return [
            descriptor
            for descriptor in adapter.build_sources(business_date)
            if descriptor.pipeline_code == pipeline_code
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

    def pipeline_schedulable(self, pipeline_code: str) -> bool:
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select is_enabled and is_schedulable
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

    def _source_descriptor(self, source_name: str, business_date: date) -> Any:
        adapter_type, descriptor_index = SOURCE_DESCRIPTOR_INDEX[source_name]
        return get_adapter(adapter_type).build_sources(business_date)[descriptor_index]

    def _landing_root_path(self) -> Path:
        landing_root = Path(self.landing_root_dir)
        landing_root.mkdir(parents=True, exist_ok=True)
        return landing_root

    def _resolve_storage_path(self, file_record: dict[str, Any]) -> Path:
        configured_path = Path(str(file_record["storage_path"]))
        if configured_path.exists():
            return configured_path
        return self._landing_root_path() / str(file_record["object_key"])

    def _raw_storage_path(self, descriptor: Any) -> Path:
        return self._landing_root_path() / str(descriptor.object_key)

    def _canonical_storage_path(self, descriptor: Any) -> Path:
        if descriptor.canonical_object_key:
            return self._landing_root_path() / str(descriptor.canonical_object_key)
        raw_path = self._raw_storage_path(descriptor)
        return raw_path.with_suffix(f".{descriptor.canonical_format}")

    def _manifest_storage_path(self, descriptor: Any) -> Path:
        if descriptor.manifest_object_key:
            return self._landing_root_path() / str(descriptor.manifest_object_key)
        raw_path = self._raw_storage_path(descriptor)
        return raw_path.with_name("manifest.json")

    def _source_root_path(self, descriptor: Any) -> Path:
        return self._raw_storage_path(descriptor).parent.parent

    @staticmethod
    def _read_json_file(file_path: Path) -> dict[str, Any] | None:
        if not file_path.exists():
            return None
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return None
        return payload

    @staticmethod
    def _row_count_from_csv(file_path: Path) -> int:
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return sum(1 for _ in reader)

    @staticmethod
    def _synthetic_source_file_id(source_name: str, business_date: date) -> str:
        digest = hashlib.sha1(f"{source_name}:{business_date.isoformat()}".encode()).hexdigest()
        return digest

    @staticmethod
    def _json_ready_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, dict):
            return {
                str(key): ControlPlaneResource._json_ready_value(inner_value)
                for key, inner_value in value.items()
            }
        if isinstance(value, list):
            return [ControlPlaneResource._json_ready_value(item) for item in value]
        if isinstance(value, (UUID, date, datetime)):
            return str(value)
        if isinstance(value, float):
            return float(value)
        if isinstance(value, int | str | bool):
            return value
        return str(value)

    def _write_canonical_parquet(
        self,
        descriptor: Any,
        rows: list[dict[str, Any]],
        capture_rows: list[dict[str, Any]],
        raw_storage_path: Path,
    ) -> dict[str, Any]:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ModuleNotFoundError as error:
            raise RuntimeError(
                "pyarrow is required to write canonical parquet source artifacts."
            ) from error

        canonical_path = self._canonical_storage_path(descriptor)
        canonical_path.parent.mkdir(parents=True, exist_ok=True)

        payload_rows: list[dict[str, Any]] = []
        for row, capture_row in zip(rows, capture_rows, strict=False):
            record_id = row.get("source_record_id") or capture_row.get("source_record_id") or ""
            row_hash = row.get("row_hash") or capture_row.get("row_hash") or ""
            payload_rows.append(
                {
                    "_pipeline_code": descriptor.pipeline_code,
                    "_dataset_code": descriptor.dataset_code,
                    "_source_name": descriptor.source_name,
                    "_business_date": descriptor.business_date.isoformat(),
                    "_extracted_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    "_record_id": str(record_id),
                    "_row_hash": str(row_hash),
                    "_source_uri": descriptor.source_url,
                    "_schema_version": descriptor.schema_version,
                    **{
                        key: self._json_ready_value(value)
                        for key, value in row.items()
                    },
                }
            )

        table = pa.Table.from_pylist(payload_rows)
        pq.write_table(table, canonical_path)
        return {
            "canonical_storage_path": str(canonical_path),
            "canonical_object_key": str(
                descriptor.canonical_object_key
                or canonical_path.relative_to(self._landing_root_path())
            ),
            "canonical_format": descriptor.canonical_format,
            "canonical_checksum": self._file_checksum(canonical_path),
            "canonical_row_count": len(payload_rows),
            "raw_storage_path": str(raw_storage_path),
            "raw_object_key": str(descriptor.object_key),
            "schema_version": descriptor.schema_version,
        }

    def _write_source_manifest(
        self,
        descriptor: Any,
        *,
        row_count: int,
        raw_storage_path: Path,
        raw_checksum: str,
        raw_file_size_bytes: int,
        artifact_metadata: dict[str, Any],
        extra_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        manifest_path = self._manifest_storage_path(descriptor)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest = {
            "pipeline_code": descriptor.pipeline_code,
            "dataset_code": descriptor.dataset_code,
            "source_name": descriptor.source_name,
            "business_date": descriptor.business_date.isoformat(),
            "source_url": descriptor.source_url,
            "capture_mode": descriptor.ingestion_mode,
            "raw_format": descriptor.expected_format,
            "canonical_format": descriptor.canonical_format,
            "schema_version": descriptor.schema_version,
            "row_count": row_count,
            "raw_storage_path": str(raw_storage_path),
            "raw_object_key": str(descriptor.object_key),
            "raw_checksum": raw_checksum,
            "raw_file_size_bytes": raw_file_size_bytes,
            "captured_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "availability_status": "ready",
            "artifacts": artifact_metadata,
            "metadata": extra_metadata or {},
        }
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        return {
            "manifest_storage_path": str(manifest_path),
            "manifest_object_key": str(
                descriptor.manifest_object_key
                or manifest_path.relative_to(self._landing_root_path())
            ),
            "manifest_checksum": self._file_checksum(manifest_path),
        }

    @staticmethod
    def _file_checksum(file_path: Path) -> str:
        digest = hashlib.sha256()
        with file_path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _existing_source_file(
        self,
        pipeline_code: str,
        source_name: str,
        business_date: date,
    ) -> dict[str, Any] | None:
        query = """
        select
            source_file_id,
            pipeline_code,
            dataset_code,
            source_name,
            business_date,
            storage_backend,
            storage_path,
            object_key,
            source_url,
            source_format,
            capture_mode,
            content_checksum,
            row_count,
            file_size_bytes,
            metadata,
            captured_at
        from control.source_file_registry
        where pipeline_code = %s
          and source_name = %s
          and business_date = %s
        """
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query, (pipeline_code, source_name, business_date))
                row = cursor.fetchone()
        return dict(row) if row is not None else None

    def _register_source_file(
        self,
        *,
        descriptor: Any,
        storage_path: Path,
        checksum: str,
        row_count: int,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        query = """
        insert into control.source_file_registry (
            pipeline_code,
            dataset_code,
            source_name,
            business_date,
            storage_backend,
            storage_path,
            object_key,
            source_url,
            source_format,
            capture_mode,
            content_checksum,
            row_count,
            file_size_bytes,
            metadata
        )
        values (%s, %s, %s, %s, 'filesystem', %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (pipeline_code, source_name, business_date) do update
        set storage_path = excluded.storage_path,
            object_key = excluded.object_key,
            source_url = excluded.source_url,
            source_format = excluded.source_format,
            capture_mode = excluded.capture_mode,
            content_checksum = excluded.content_checksum,
            row_count = excluded.row_count,
            file_size_bytes = excluded.file_size_bytes,
            metadata = control.source_file_registry.metadata || excluded.metadata
        returning
            source_file_id,
            pipeline_code,
            dataset_code,
            source_name,
            business_date,
            storage_backend,
            storage_path,
            object_key,
            source_url,
            source_format,
            capture_mode,
            content_checksum,
            row_count,
            file_size_bytes,
            metadata,
            captured_at
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(
                    query,
                    (
                        descriptor.pipeline_code,
                        descriptor.dataset_code,
                        descriptor.source_name,
                        descriptor.business_date,
                        str(storage_path),
                        descriptor.object_key,
                        descriptor.source_url,
                        descriptor.expected_format,
                        descriptor.ingestion_mode,
                        checksum,
                        row_count,
                        storage_path.stat().st_size,
                        Jsonb(metadata or {}),
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        if row is None:
            raise RuntimeError(
                "Unable to register landed source file for "
                f"{descriptor.source_name}"
            )
        return dict(row)

    def _register_landed_source_file(
        self,
        source_name: str,
        business_date: date,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        descriptor = self._source_descriptor(source_name, business_date)
        raw_storage_path = self._raw_storage_path(descriptor)
        if not raw_storage_path.exists():
            return None

        raw_checksum = self._file_checksum(raw_storage_path)
        manifest_path = self._manifest_storage_path(descriptor)
        manifest_payload = self._read_json_file(manifest_path) or {}
        manifest_artifacts = manifest_payload.get("artifacts")
        if not isinstance(manifest_artifacts, dict):
            manifest_artifacts = {}
        extra_metadata = manifest_payload.get("metadata")
        if not isinstance(extra_metadata, dict):
            extra_metadata = {}
        if metadata:
            extra_metadata.update(metadata)

        row_count = int(
            manifest_payload.get("row_count") or self._row_count_from_csv(raw_storage_path)
        )
        canonical_path = self._canonical_storage_path(descriptor)
        artifact_metadata: dict[str, Any] = {}
        if canonical_path.exists():
            artifact_metadata = {
                "canonical_storage_path": str(canonical_path),
                "canonical_object_key": str(
                    descriptor.canonical_object_key
                    or canonical_path.relative_to(self._landing_root_path())
                ),
                "canonical_format": descriptor.canonical_format,
                "canonical_checksum": self._file_checksum(canonical_path),
                "canonical_row_count": int(
                    manifest_artifacts.get("canonical_row_count") or row_count
                ),
                "raw_storage_path": str(raw_storage_path),
                "raw_object_key": str(descriptor.object_key),
                "schema_version": descriptor.schema_version,
            }

        manifest_metadata: dict[str, Any] = {}
        if manifest_path.exists():
            manifest_metadata = {
                "manifest_storage_path": str(manifest_path),
                "manifest_object_key": str(
                    descriptor.manifest_object_key
                    or manifest_path.relative_to(self._landing_root_path())
                ),
                "manifest_checksum": self._file_checksum(manifest_path),
            }

        file_record = self._register_source_file(
            descriptor=descriptor,
            storage_path=raw_storage_path,
            checksum=raw_checksum,
            row_count=row_count,
            metadata={
                "landing_table": descriptor.landing_table,
                "fieldnames": CAPTURE_FIELDNAMES[source_name],
                "file_name": descriptor.file_name,
                **artifact_metadata,
                **manifest_metadata,
                **extra_metadata,
            },
        )

        rebuilt_rows: list[dict[str, Any]] | None = None
        if not canonical_path.exists():
            rebuilt_rows = self._rows_from_landed_file(source_name, file_record)
            artifact_metadata = self._write_canonical_parquet(
                descriptor,
                rebuilt_rows,
                self._capture_csv_rows(source_name, rebuilt_rows),
                raw_storage_path,
            )

        if not manifest_path.exists() or rebuilt_rows is not None or metadata:
            if rebuilt_rows is None:
                rebuilt_rows = self._rows_from_landed_file(source_name, file_record)
            manifest_metadata = self._write_source_manifest(
                descriptor,
                row_count=row_count,
                raw_storage_path=raw_storage_path,
                raw_checksum=raw_checksum,
                raw_file_size_bytes=raw_storage_path.stat().st_size,
                artifact_metadata=artifact_metadata,
                extra_metadata=extra_metadata,
            )

        return self._register_source_file(
            descriptor=descriptor,
            storage_path=raw_storage_path,
            checksum=raw_checksum,
            row_count=row_count,
            metadata={
                "landing_table": descriptor.landing_table,
                "fieldnames": CAPTURE_FIELDNAMES[source_name],
                "file_name": descriptor.file_name,
                **artifact_metadata,
                **manifest_metadata,
                **extra_metadata,
            },
        )

    def _capture_rows_for_source(
        self, source_name: str, business_date: date
    ) -> list[dict[str, Any]]:
        if source_name == "sec_company_tickers":
            return self.ingestion.build_security_master_tickers(business_date)
        if source_name == "sec_company_facts":
            return self.ingestion.build_security_master_facts(business_date)
        if source_name == "holdings_13f_filers":
            return self.ingestion.build_shareholder_filers(business_date)
        if source_name == "holdings_13f":
            return self.ingestion.build_shareholder_holdings(business_date)
        raise KeyError(f"Unsupported source capture: {source_name}")

    def _capture_csv_rows(
        self, source_name: str, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if source_name == "sec_company_tickers":
            return [
                {
                    "source_record_id": row["source_record_id"],
                    "cik": row["cik"],
                    "ticker": row["ticker"],
                    "company_name": row["company_name"],
                    "exchange": row["exchange"],
                    "holder_count_recent_window": row["source_payload"].get(
                        "holder_count_recent_window"
                    ),
                    "reported_shares_recent_window": row["source_payload"].get(
                        "reported_shares_recent_window"
                    ),
                    "top_holder_shares_recent_window": row["source_payload"].get(
                        "top_holder_shares_recent_window"
                    ),
                    "source_payload_hash": row["source_payload_hash"],
                    "row_hash": row["row_hash"],
                }
                for row in rows
            ]
        if source_name == "sec_company_facts":
            return [
                {
                    "source_record_id": row["source_record_id"],
                    "cik": row["cik"],
                    "fact_name": row["fact_name"],
                    "fact_value": row["fact_value"],
                    "unit": row["unit"],
                    "retrieval_status": row["source_payload"].get("retrieval_status"),
                    "source_payload_hash": row["source_payload_hash"],
                    "row_hash": row["row_hash"],
                }
                for row in rows
            ]
        if source_name == "holdings_13f_filers":
            return [
                {
                    "source_record_id": row["source_record_id"],
                    "accession_number": row["accession_number"],
                    "filer_cik": row["filer_cik"],
                    "filer_name": row["filer_name"],
                    "report_period": row["report_period"],
                    "filing_date": row["source_payload"].get("filing_date"),
                    "focus_holdings_count": row["source_payload"].get("focus_holdings_count"),
                    "source_payload_hash": row["source_payload_hash"],
                    "row_hash": row["row_hash"],
                }
                for row in rows
            ]
        if source_name == "holdings_13f":
            return [
                {
                    "source_record_id": row["source_record_id"],
                    "accession_number": row["accession_number"],
                    "filer_cik": row["filer_cik"],
                    "filer_name": row["source_payload"].get("filer_name"),
                    "report_period": row["source_payload"].get("report_period"),
                    "issuer_name": row["source_payload"].get("issuer_name"),
                    "resolved_ticker": row["security_identifier"],
                    "ticker_source": row["source_payload"].get("ticker_source"),
                    "ticker_sources": "|".join(
                        row["source_payload"].get("ticker_sources", [])
                    ),
                    "cusip": row["cusip"],
                    "shares_held": row["shares_held"],
                    "market_value": row["market_value"],
                    "holder_count_recent_window": row["source_payload"].get(
                        "holder_count_recent_window"
                    ),
                    "top_holder_shares_recent_window": row["source_payload"].get(
                        "top_holder_shares_recent_window"
                    ),
                    "reported_shares_recent_window": row["source_payload"].get(
                        "reported_shares_recent_window"
                    ),
                    "aggregated_rows": row["source_payload"].get("aggregated_rows"),
                    "source_payload_hash": row["source_payload_hash"],
                    "row_hash": row["row_hash"],
                }
                for row in rows
            ]
        raise KeyError(f"Unsupported source capture: {source_name}")

    def capture_source_file(
        self,
        source_name: str,
        business_date: date,
        *,
        rows: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        descriptor = self._source_descriptor(source_name, business_date)
        landed_record = self._register_landed_source_file(
            source_name,
            business_date,
            metadata=metadata,
        )
        if landed_record is not None:
            return landed_record

        existing = self._existing_source_file(
            descriptor.pipeline_code,
            descriptor.source_name,
            descriptor.business_date,
        )
        if existing is not None and self._resolve_storage_path(existing).exists():
            return existing

        captured_rows = (
            rows
            if rows is not None
            else self._capture_rows_for_source(source_name, business_date)
        )
        capture_rows = self._capture_csv_rows(source_name, captured_rows)
        fieldnames = CAPTURE_FIELDNAMES[source_name]
        landing_path = self._landing_root_path() / descriptor.object_key
        landing_path.parent.mkdir(parents=True, exist_ok=True)
        with landing_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in capture_rows:
                writer.writerow(
                    {
                        fieldname: self._normalize_csv_value(row.get(fieldname))
                        for fieldname in fieldnames
                    }
                )
        checksum = self._file_checksum(landing_path)
        artifact_metadata = self._write_canonical_parquet(
            descriptor,
            captured_rows,
            capture_rows,
            landing_path,
        )
        manifest_metadata = self._write_source_manifest(
            descriptor,
            row_count=len(capture_rows),
            raw_storage_path=landing_path,
            raw_checksum=checksum,
            raw_file_size_bytes=landing_path.stat().st_size,
            artifact_metadata=artifact_metadata,
            extra_metadata=metadata,
        )
        return self._register_source_file(
            descriptor=descriptor,
            storage_path=landing_path,
            checksum=checksum,
            row_count=len(capture_rows),
            metadata={
                "landing_table": descriptor.landing_table,
                "fieldnames": fieldnames,
                "file_name": descriptor.file_name,
                **artifact_metadata,
                **manifest_metadata,
                **(metadata or {}),
            },
        )

    def capture_security_master_facts_from_landed_tickers(
        self,
        business_date: date,
    ) -> dict[str, Any]:
        existing = self._register_landed_source_file("sec_company_facts", business_date)
        if existing is not None:
            return existing

        ticker_source_file = self.latest_source_file("sec_company_tickers", business_date)
        ticker_rows = self._rows_from_landed_file("sec_company_tickers", ticker_source_file)
        fact_rows = self.ingestion.build_security_master_facts_from_ticker_rows(
            business_date,
            ticker_rows,
        )
        return self.capture_source_file(
            "sec_company_facts",
            business_date,
            rows=fact_rows,
            metadata={
                "derived_from_source_name": "sec_company_tickers",
                "derived_from_source_file_id": str(ticker_source_file["source_file_id"]),
                "derived_from_storage_path": str(ticker_source_file["storage_path"]),
            },
        )

    def capture_shareholder_filers_from_landed_holdings(
        self,
        business_date: date,
    ) -> dict[str, Any]:
        existing = self._register_landed_source_file("holdings_13f_filers", business_date)
        if existing is not None:
            return existing

        holdings_source_file = self.latest_source_file("holdings_13f", business_date)
        holding_rows = self._rows_from_landed_file("holdings_13f", holdings_source_file)
        filer_rows = self.ingestion.build_shareholder_filers_from_holding_rows(holding_rows)
        return self.capture_source_file(
            "holdings_13f_filers",
            business_date,
            rows=filer_rows,
            metadata={
                "derived_from_source_name": "holdings_13f",
                "derived_from_source_file_id": str(holdings_source_file["source_file_id"]),
                "derived_from_storage_path": str(holdings_source_file["storage_path"]),
            },
        )

    @staticmethod
    def _csv_float(value: str | None) -> float | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            return None
        return float(stripped)

    @staticmethod
    def _csv_int(value: str | None) -> int | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            return None
        return int(stripped)

    @staticmethod
    def _csv_date(value: str | None) -> date | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            return None
        return date.fromisoformat(stripped)

    def _rows_from_canonical_file(self, file_record: dict[str, Any]) -> list[dict[str, Any]] | None:
        metadata = file_record.get("metadata") or {}
        canonical_storage_path = metadata.get("canonical_storage_path")
        if canonical_storage_path is None:
            return None

        canonical_path = Path(str(canonical_storage_path))
        if not canonical_path.exists():
            canonical_object_key = metadata.get("canonical_object_key")
            if canonical_object_key is None:
                return None
            canonical_path = self._landing_root_path() / str(canonical_object_key)
        if not canonical_path.exists():
            return None

        try:
            import pyarrow.parquet as pq
        except ModuleNotFoundError:
            return None

        table = pq.read_table(canonical_path)
        rows: list[dict[str, Any]] = []
        for row in table.to_pylist():
            rows.append(
                {
                    key: value
                    for key, value in row.items()
                    if not key.startswith("_")
                }
            )
        return rows

    def _rows_from_source_file(
        self,
        source_name: str,
        file_record: dict[str, Any],
    ) -> list[dict[str, Any]]:
        canonical_rows = self._rows_from_canonical_file(file_record)
        rows = (
            canonical_rows
            if canonical_rows is not None
            else self._rows_from_landed_file(source_name, file_record)
        )
        current_source_file_id = file_record["source_file_id"]
        return [
            {
                **row,
                "source_file_id": current_source_file_id,
            }
            for row in rows
        ]

    def _rows_from_landed_file(
        self,
        source_name: str,
        file_record: dict[str, Any],
    ) -> list[dict[str, Any]]:
        file_path = self._resolve_storage_path(file_record)
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)

        source_file_id = file_record["source_file_id"]
        source_file_name = file_path.name
        if source_name == "sec_company_tickers":
            rebuilt_rows: list[dict[str, Any]] = []
            for index, row in enumerate(rows, start=1):
                payload = {
                    "cik": row["cik"],
                    "ticker": row["ticker"],
                    "company_name": row["company_name"],
                    "exchange": row["exchange"] or None,
                    "holder_count_recent_window": self._csv_int(
                        row.get("holder_count_recent_window")
                    ),
                    "reported_shares_recent_window": self._csv_float(
                        row.get("reported_shares_recent_window")
                    ),
                    "top_holder_shares_recent_window": self._csv_float(
                        row.get("top_holder_shares_recent_window")
                    ),
                }
                rebuilt_rows.append(
                    {
                        "source_file_id": source_file_id,
                        "source_record_id": row["source_record_id"],
                        "source_payload": payload,
                        "source_payload_hash": row["source_payload_hash"],
                        "source_file_name": source_file_name,
                        "source_file_row_number": index,
                        "cik": row["cik"],
                        "ticker": row["ticker"],
                        "company_name": row["company_name"],
                        "exchange": row["exchange"] or None,
                        "row_hash": row["row_hash"],
                    }
                )
            return rebuilt_rows
        if source_name == "sec_company_facts":
            rebuilt_rows = []
            for index, row in enumerate(rows, start=1):
                payload = {
                    "cik": row["cik"],
                    "fact_name": row["fact_name"],
                    "fact_value": self._csv_float(row.get("fact_value")),
                    "unit": row["unit"] or None,
                    "retrieval_status": row["retrieval_status"] or None,
                }
                rebuilt_rows.append(
                    {
                        "source_file_id": source_file_id,
                        "source_record_id": row["source_record_id"],
                        "source_payload": payload,
                        "source_payload_hash": row["source_payload_hash"],
                        "source_file_name": source_file_name,
                        "source_file_row_number": index,
                        "cik": row["cik"],
                        "fact_name": row["fact_name"],
                        "fact_value": self._csv_float(row.get("fact_value")),
                        "unit": row["unit"] or None,
                        "row_hash": row["row_hash"],
                    }
                )
            return rebuilt_rows
        if source_name == "holdings_13f_filers":
            rebuilt_rows = []
            for index, row in enumerate(rows, start=1):
                report_period = self._csv_date(row.get("report_period"))
                payload = {
                    "accession_number": row["accession_number"],
                    "filer_cik": row["filer_cik"],
                    "filer_name": row["filer_name"],
                    "report_period": report_period.isoformat() if report_period else None,
                    "filing_date": row["filing_date"] or None,
                    "focus_holdings_count": self._csv_int(row.get("focus_holdings_count")),
                }
                rebuilt_rows.append(
                    {
                        "source_file_id": source_file_id,
                        "source_record_id": row["source_record_id"],
                        "source_payload": payload,
                        "source_payload_hash": row["source_payload_hash"],
                        "source_file_name": source_file_name,
                        "source_file_row_number": index,
                        "accession_number": row["accession_number"],
                        "filer_cik": row["filer_cik"],
                        "filer_name": row["filer_name"],
                        "report_period": report_period,
                        "row_hash": row["row_hash"],
                    }
                )
            return rebuilt_rows
        if source_name == "holdings_13f":
            rebuilt_rows = []
            for index, row in enumerate(rows, start=1):
                ticker_sources = [
                    item for item in (row.get("ticker_sources") or "").split("|") if item
                ]
                report_period = self._csv_date(row.get("report_period"))
                payload = {
                    "accession_number": row["accession_number"],
                    "filer_cik": row["filer_cik"],
                    "filer_name": row["filer_name"] or None,
                    "report_period": report_period.isoformat() if report_period else None,
                    "issuer_name": row["issuer_name"] or None,
                    "resolved_ticker": row["resolved_ticker"],
                    "ticker_source": row["ticker_source"] or None,
                    "ticker_sources": ticker_sources,
                    "cusip": row["cusip"] or None,
                    "shares_held": self._csv_float(row.get("shares_held")),
                    "market_value_usd": self._csv_float(row.get("market_value")),
                    "holder_count_recent_window": self._csv_int(
                        row.get("holder_count_recent_window")
                    ),
                    "top_holder_shares_recent_window": self._csv_float(
                        row.get("top_holder_shares_recent_window")
                    ),
                    "reported_shares_recent_window": self._csv_float(
                        row.get("reported_shares_recent_window")
                    ),
                    "aggregated_rows": self._csv_int(row.get("aggregated_rows")),
                }
                rebuilt_rows.append(
                    {
                        "source_file_id": source_file_id,
                        "source_record_id": row["source_record_id"],
                        "source_payload": payload,
                        "source_payload_hash": row["source_payload_hash"],
                        "source_file_name": source_file_name,
                        "source_file_row_number": index,
                        "accession_number": row["accession_number"],
                        "filer_cik": row["filer_cik"],
                        "security_identifier": row["resolved_ticker"],
                        "cusip": row["cusip"] or None,
                        "shares_held": self._csv_float(row.get("shares_held")),
                        "market_value": self._csv_float(row.get("market_value")),
                        "row_hash": row["row_hash"],
                    }
                )
            return rebuilt_rows
        raise KeyError(f"Unsupported source capture: {source_name}")

    def latest_source_file(self, source_name: str, business_date: date) -> dict[str, Any]:
        descriptor = self._source_descriptor(source_name, business_date)
        landed_record = self._register_landed_source_file(source_name, business_date)
        if landed_record is not None:
            return landed_record

        record = self._existing_source_file(
            descriptor.pipeline_code,
            descriptor.source_name,
            descriptor.business_date,
        )
        if record is None:
            raise RuntimeError(
                "No landed source file is registered for "
                f"{source_name} on {business_date.isoformat()}"
            )
        return record

    def list_available_source_dates(self, pipeline_code: str) -> list[dict[str, Any]]:
        descriptors = self.source_descriptors_for_pipeline(pipeline_code, date.today())
        if not descriptors:
            return []

        source_names = [str(descriptor.source_name) for descriptor in descriptors]
        grouped: dict[date, dict[str, Any]] = {}
        for descriptor in descriptors:
            source_root = self._source_root_path(descriptor)
            if not source_root.exists():
                continue
            for business_date_dir in source_root.iterdir():
                if not business_date_dir.is_dir():
                    continue
                try:
                    business_date = date.fromisoformat(business_date_dir.name)
                except ValueError:
                    continue

                raw_path = business_date_dir / descriptor.file_name
                canonical_path = (
                    business_date_dir / descriptor.canonical_file_name
                    if descriptor.canonical_file_name
                    else self._canonical_storage_path(
                        self._source_descriptor(descriptor.source_name, business_date)
                    )
                )
                manifest_path = (
                    business_date_dir / descriptor.manifest_file_name
                    if descriptor.manifest_file_name
                    else business_date_dir / "manifest.json"
                )
                if (
                    not raw_path.exists()
                    and not canonical_path.exists()
                    and not manifest_path.exists()
                ):
                    continue

                manifest_payload = self._read_json_file(manifest_path) or {}
                row_count = int(
                    manifest_payload.get("row_count")
                    or (self._row_count_from_csv(raw_path) if raw_path.exists() else 0)
                )
                captured_at = manifest_payload.get("captured_at")
                if not isinstance(captured_at, str):
                    timestamp_source = manifest_path if manifest_path.exists() else raw_path
                    captured_at = datetime.fromtimestamp(
                        timestamp_source.stat().st_mtime
                    ).isoformat(timespec="seconds")

                grouped.setdefault(
                    business_date,
                    {
                        "pipeline_code": pipeline_code,
                        "business_date": business_date.isoformat(),
                        "sources": [],
                    },
                )["sources"].append(
                    {
                        "source_name": descriptor.source_name,
                        "source_file_id": self._synthetic_source_file_id(
                            descriptor.source_name,
                            business_date,
                        ),
                        "row_count": row_count,
                        "captured_at": captured_at,
                        "raw_exists": raw_path.exists(),
                        "manifest_exists": manifest_path.exists(),
                    }
                )

        results: list[dict[str, Any]] = []
        expected = len(descriptors)
        for business_date in sorted(grouped.keys(), reverse=True):
            payload = grouped[business_date]
            available_sources = list(payload["sources"])
            ready_source_names = {
                str(source["source_name"])
                for source in available_sources
                if bool(source["raw_exists"])
            }
            results.append(
                {
                    "pipeline_code": pipeline_code,
                    "business_date": payload["business_date"],
                    "available_source_count": len(ready_source_names),
                    "expected_source_count": expected,
                    "is_ready": len(ready_source_names) == expected,
                    "missing_sources": [
                        source_name
                        for source_name in source_names
                        if source_name not in ready_source_names
                    ],
                    "sources": available_sources,
                }
            )
        return results

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

    def current_pipeline_state(self, pipeline_code: str) -> dict[str, Any] | None:
        query = """
        select pipeline_code, dataset_code, approval_state, last_run_id, last_business_date
        from control.pipeline_state
        where pipeline_code = %s
        """
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query, (pipeline_code,))
                row = cursor.fetchone()
        return dict(row) if row is not None else None

    def pipeline_run_status(self, run_id: UUID) -> str | None:
        query = """
        select status
        from observability.pipeline_runs
        where run_id = %s
        """
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (run_id,))
                row = cursor.fetchone()
        if row is None:
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
          and not exists (
              select 1
              from observability.pipeline_step_runs as steps
              where steps.run_id = review.run_id
                and steps.step_key = %s
                and steps.status in ('running', 'succeeded')
          )
        order by coalesce(review.review_completed_at, review.updated_at), review.business_date
        """
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query, (pipeline_code, export_step_key(pipeline_code)))
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

    def _execution_snapshot(self, run_id: UUID) -> dict[str, Any]:
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
        where run_id = %s
        """
        step_query = """
        select
            step_key,
            step_label,
            step_group,
            sort_order,
            status,
            started_at,
            ended_at,
            metadata
        from observability.pipeline_step_runs
        where run_id = %s
        order by sort_order, step_run_id
        """
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(run_query, (run_id,))
                run_row = cursor.fetchone()
                cursor.execute(step_query, (run_id,))
                step_rows = cursor.fetchall()
        return {
            "run": self._json_ready(dict(run_row)) if run_row is not None else None,
            "steps": [self._json_ready(dict(step_row)) for step_row in step_rows],
        }

    def record_export_bundle_registry(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
        artifact_payload: dict[str, Any],
    ) -> dict[str, Any]:
        execution_snapshot = self._execution_snapshot(run_id)
        baseline_file_path = str(artifact_payload["baseline_file_path"])
        reviewed_file_path = str(artifact_payload["reviewed_file_path"])
        audit_file_path = str(artifact_payload["audit_file_path"])
        manifest_file_path = str(artifact_payload["manifest_file_path"])
        metadata = self._json_ready(
            {
                "artifacts": {
                    "baseline_file_path": baseline_file_path,
                    "reviewed_file_path": reviewed_file_path,
                    "audit_file_path": audit_file_path,
                    "manifest_file_path": manifest_file_path,
                },
                "observability_run": execution_snapshot["run"],
                "steps": execution_snapshot["steps"],
            }
        )
        query = """
        insert into control.export_bundle_registry (
            pipeline_code,
            dataset_code,
            run_id,
            business_date,
            baseline_file_path,
            reviewed_file_path,
            audit_file_path,
            manifest_file_path,
            metadata
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (run_id) do update
        set pipeline_code = excluded.pipeline_code,
            dataset_code = excluded.dataset_code,
            business_date = excluded.business_date,
            baseline_file_path = excluded.baseline_file_path,
            reviewed_file_path = excluded.reviewed_file_path,
            audit_file_path = excluded.audit_file_path,
            manifest_file_path = excluded.manifest_file_path,
            metadata = excluded.metadata,
            exported_at = timezone('utc', now())
        returning
            pipeline_code,
            dataset_code,
            run_id,
            business_date,
            baseline_file_path,
            reviewed_file_path,
            audit_file_path,
            manifest_file_path,
            exported_at
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(
                    query,
                    (
                        pipeline_code,
                        dataset_code,
                        run_id,
                        business_date,
                        baseline_file_path,
                        reviewed_file_path,
                        audit_file_path,
                        manifest_file_path,
                        Jsonb(metadata),
                    ),
                )
                row = cursor.fetchone()
            connection.commit()
        return dict(row) if row is not None else {}

    def _export_query_details(
        self, pipeline_code: str
    ) -> tuple[str, str, list[str], list[str]]:
        export_queries: dict[str, tuple[str, str, list[str], list[str]]] = {
            "security_master": (
                """
                select
                    security_id as _row_export_key,
                    business_date,
                    ticker,
                    issuer_name,
                    investable_shares,
                    review_materiality_score
                from marts.dim_security
                where run_id = %s
                order by ticker
                """,
                """
                select
                    origin_mart_row_id as _row_export_key,
                    business_date,
                    coalesce(ticker, 'UNKNOWN') as ticker,
                    issuer_name,
                    investable_shares,
                    review_materiality_score
                from review.security_master_daily
                where run_id = %s
                  and approval_state in ('approved', 'exported')
                order by ticker
                """,
                [
                    "business_date",
                    "ticker",
                    "issuer_name",
                    "investable_shares",
                    "review_materiality_score",
                ],
                ["_row_export_key"],
            ),
            "shareholder_holdings": (
                """
                select
                    holding_id as _row_export_key,
                    business_date,
                    filer_name,
                    security_name,
                    shares_held_raw as shares_held,
                    holding_pct_of_outstanding
                from marts.fact_shareholder_holding
                where run_id = %s
                order by filer_name, security_name
                """,
                """
                select
                    origin_mart_row_id as _row_export_key,
                    business_date,
                    filer_name,
                    security_name,
                    coalesce(shares_held_override, shares_held_raw) as shares_held,
                    holding_pct_of_outstanding
                from review.shareholder_holdings_daily
                where run_id = %s
                  and approval_state in ('approved', 'exported')
                order by filer_name, security_name
                """,
                [
                    "business_date",
                    "filer_name",
                    "security_name",
                    "shares_held",
                    "holding_pct_of_outstanding",
                ],
                ["_row_export_key"],
            ),
        }
        query_details = export_queries.get(pipeline_code)
        if query_details is None:
            raise KeyError(f"Unsupported export pipeline: {pipeline_code}")
        return query_details

    def _query_dict_rows(
        self,
        query: str,
        params: tuple[Any, ...],
    ) -> list[dict[str, Any]]:
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]

    def _write_csv_rows(
        self,
        file_path: Path,
        *,
        fieldnames: list[str],
        rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
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
            "path": str(file_path),
            "checksum": self._file_checksum(file_path),
            "row_count": len(rows),
        }

    def _write_audit_parquet(self, file_path: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ModuleNotFoundError as error:
            raise RuntimeError(
                "pyarrow is required to write audit parquet export artifacts."
            ) from error

        normalized_rows = [
            {key: self._json_ready_value(value) for key, value in row.items()}
            for row in rows
        ]
        table = pa.Table.from_pylist(normalized_rows)
        pq.write_table(table, file_path)
        return {
            "path": str(file_path),
            "checksum": self._file_checksum(file_path),
            "row_count": len(rows),
        }

    def _write_bundle_manifest(self, file_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
        file_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return {"path": str(file_path), "checksum": self._file_checksum(file_path)}

    def restore_pipeline_state_from_history(
        self,
        pipeline_code: str,
        actor: str = "system",
    ) -> dict[str, Any]:
        query = "select workflow.restore_pipeline_state_from_history(%s, %s) as payload"
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (pipeline_code, actor))
                row = cursor.fetchone()
            connection.commit()
        return row[0] if row is not None else {}

    def clear_pipeline_state(
        self,
        pipeline_code: str,
        actor: str = "system",
    ) -> dict[str, Any]:
        query = """
        update control.pipeline_state
        set approval_state = 'pending_review',
            last_run_id = null,
            last_business_date = null,
            paused_at = null,
            pause_reason = null,
            last_transition_by = %s,
            updated_at = timezone('utc', now())
        where pipeline_code = %s
        returning pipeline_code, dataset_code, approval_state, last_run_id, last_business_date
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query, (actor, pipeline_code))
                row = cursor.fetchone()
            connection.commit()
        return dict(row) if row is not None else {}

    @staticmethod
    def _workspace_relations_for_pipeline(pipeline_code: str) -> list[str]:
        return {
            "security_master": [
                "export.security_master_final",
                "export.security_master_preview",
                "staging_export.security_master_final",
                "review.security_master_daily",
                "marts.dim_security",
                "marts.dim_security_snapshot",
                "staging_marts.dim_security",
                "intermediate.int_security_attributes",
                "staging_intermediate.int_security_attributes",
                "intermediate.int_security_base",
                "staging_intermediate.int_security_base",
                "staging.stg_sec_company_tickers",
                "staging_staging.stg_sec_company_tickers",
                "staging.stg_sec_company_facts",
                "staging_staging.stg_sec_company_facts",
                "raw.sec_company_tickers",
                "raw.sec_company_facts",
            ],
            "shareholder_holdings": [
                "export.shareholder_holdings_final",
                "export.shareholder_holdings_preview",
                "staging_export.shareholder_holdings_final",
                "review.shareholder_holdings_daily",
                "marts.fact_shareholder_holding",
                "staging_marts.fact_shareholder_holding",
                "marts.dim_shareholder",
                "marts.dim_shareholder_snapshot",
                "staging_marts.dim_shareholder",
                "intermediate.int_holding_with_security",
                "staging_intermediate.int_holding_with_security",
                "intermediate.int_shareholder_base",
                "staging_intermediate.int_shareholder_base",
                "staging.stg_13f_holdings",
                "staging_staging.stg_13f_holdings",
                "staging.stg_13f_filers",
                "staging_staging.stg_13f_filers",
                "raw.holdings_13f",
                "raw.holdings_13f_filers",
            ],
        }.get(pipeline_code, [])

    def clear_source_file_registry(
        self,
        pipeline_code: str,
        business_date: date | None = None,
    ) -> dict[str, Any]:
        if business_date is None:
            query = """
            delete from control.source_file_registry
            where pipeline_code = %s
            """
            params: tuple[object, ...] = (pipeline_code,)
        else:
            query = """
            delete from control.source_file_registry
            where pipeline_code = %s
              and business_date = %s
            """
            params = (pipeline_code, business_date)
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                deleted_count = cursor.rowcount
            connection.commit()
        return {
            "pipeline_code": pipeline_code,
            "business_date": business_date.isoformat() if business_date is not None else None,
            "deleted_source_file_count": deleted_count,
        }

    def purge_run_residual_history(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
    ) -> dict[str, Any]:
        query = """
        delete from audit.workflow_events
        where pipeline_code = %s
          and dataset_code = %s
          and run_id = %s
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (pipeline_code, dataset_code, run_id))
                deleted_count = cursor.rowcount
            connection.commit()
        return {
            "pipeline_code": pipeline_code,
            "dataset_code": dataset_code,
            "run_id": str(run_id),
            "deleted_workflow_event_count": deleted_count,
        }

    def wipe_pipeline_workspace_for_load(
        self,
        pipeline_code: str,
        dataset_code: str,
        actor: str = "runner",
        notes: str | None = None,
    ) -> dict[str, Any]:
        deleted_relations: dict[str, int] = {}
        deleted_metadata: dict[str, int] = {}
        metadata_statements: list[tuple[str, str, tuple[object, ...]]] = [
            (
                "workflow.dataset_review_state",
                """
                delete from workflow.dataset_review_state
                where pipeline_code = %s
                  and dataset_code = %s
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "audit.review_data_issues",
                """
                delete from audit.review_data_issues
                where pipeline_code = %s
                  and dataset_code = %s
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "audit.change_log",
                """
                delete from audit.change_log
                where pipeline_code = %s
                  and dataset_code = %s
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "audit.workflow_events",
                """
                delete from audit.workflow_events
                where pipeline_code = %s
                  and dataset_code = %s
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "lineage.row_lineage_edges",
                """
                delete from lineage.row_lineage_edges
                where pipeline_code = %s
                  and dataset_code = %s
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "lineage.entity_lineage_summary",
                """
                delete from lineage.entity_lineage_summary
                where pipeline_code = %s
                  and dataset_code = %s
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "lineage.export_file_lineage",
                """
                delete from lineage.export_file_lineage
                where pipeline_code = %s
                  and dataset_code = %s
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "observability.data_quality_results",
                """
                delete from observability.data_quality_results
                where (
                        pipeline_code = %s
                    and dataset_code = %s
                )
                   or (
                        run_id is null
                    and coalesce(nullif(trim(pipeline_code), ''), '') = ''
                    and coalesce(nullif(trim(dataset_code), ''), '') = ''
                )
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "observability.pipeline_failures",
                """
                delete from observability.pipeline_failures
                where pipeline_code = %s
                  and dataset_code = %s
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "observability.pipeline_runs",
                """
                delete from observability.pipeline_runs
                where pipeline_code = %s
                  and dataset_code = %s
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "control.export_bundle_registry",
                """
                delete from control.export_bundle_registry
                where pipeline_code = %s
                  and dataset_code = %s
                """,
                (pipeline_code, dataset_code),
            ),
            (
                "control.source_file_registry",
                """
                delete from control.source_file_registry
                where pipeline_code = %s
                """,
                (pipeline_code,),
            ),
        ]
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                if pipeline_code == "security_master":
                    cursor.execute("select to_regclass('review.shareholder_holdings_daily')")
                    row = cursor.fetchone()
                    if row is not None and row[0] is not None:
                        cursor.execute(
                            """
                            update review.shareholder_holdings_daily
                            set security_review_row_id = null,
                                updated_at = timezone('utc', now())
                            where security_review_row_id is not null
                            """
                        )
                        deleted_metadata[
                            "review.shareholder_holdings_daily.security_review_row_id_reset"
                        ] = cursor.rowcount

                for relation_name in self._workspace_relations_for_pipeline(pipeline_code):
                    cursor.execute("select to_regclass(%s)", (relation_name,))
                    row = cursor.fetchone()
                    if row is None or row[0] is None:
                        continue
                    cursor.execute(f"delete from {relation_name}")
                    deleted_relations[relation_name] = cursor.rowcount

                for relation_name, query, params in metadata_statements:
                    cursor.execute(query, params)
                    deleted_metadata[relation_name] = cursor.rowcount
            connection.commit()

        cleared_state = self.clear_pipeline_state(pipeline_code, actor)
        vacuum_payload = self.vacuum_pipeline_workspace(pipeline_code)
        return {
            "pipeline_code": pipeline_code,
            "dataset_code": dataset_code,
            "actor": actor,
            "notes": notes,
            "deleted_relations": deleted_relations,
            "deleted_metadata": deleted_metadata,
            "cleared_state": cleared_state,
            "maintenance": vacuum_payload,
        }

    @staticmethod
    def _vacuum_relations_for_pipeline(pipeline_code: str) -> list[str]:
        shared_relations = [
            "control.source_file_registry",
            "control.export_bundle_registry",
            "workflow.dataset_review_state",
            "audit.change_log",
            "audit.review_data_issues",
            "audit.workflow_events",
            "observability.pipeline_runs",
            "observability.pipeline_step_runs",
            "observability.pipeline_asset_metrics",
            "observability.data_quality_results",
            "observability.pipeline_failures",
            "observability.pipeline_failure_rows",
            "lineage.row_lineage_edges",
            "lineage.entity_lineage_summary",
            "lineage.export_file_lineage",
        ]
        return shared_relations + ControlPlaneResource._workspace_relations_for_pipeline(
            pipeline_code
        )

    def vacuum_pipeline_workspace(self, pipeline_code: str) -> dict[str, Any]:
        vacuumed_relations: list[str] = []
        with psycopg.connect(self.direct_database_url, autocommit=True) as connection:
            with connection.cursor() as cursor:
                for relation_name in self._vacuum_relations_for_pipeline(pipeline_code):
                    cursor.execute("select to_regclass(%s)", (relation_name,))
                    row = cursor.fetchone()
                    if row is None or row[0] is None:
                        continue
                    cursor.execute(f"vacuum analyze {relation_name}")
                    vacuumed_relations.append(relation_name)
        return {
            "pipeline_code": pipeline_code,
            "vacuumed_relations": vacuumed_relations,
        }

    def cleanup_operational_run(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
        actor: str = "dagster",
        notes: str | None = None,
    ) -> dict[str, Any]:
        cleanup_payload = self.reset_pipeline_run_data(
            pipeline_code=pipeline_code,
            dataset_code=dataset_code,
            run_id=run_id,
            business_date=business_date,
            actor=actor,
            notes=notes,
        )
        residual_cleanup = self.purge_run_residual_history(pipeline_code, dataset_code, run_id)
        source_cleanup = self.clear_source_file_registry(pipeline_code, business_date)
        cleared_state = self.clear_pipeline_state(pipeline_code, actor)
        vacuum_payload = self.vacuum_pipeline_workspace(pipeline_code)
        return {
            "cleanup": cleanup_payload,
            "residual_cleanup": residual_cleanup,
            "source_cleanup": source_cleanup,
            "cleared_state": cleared_state,
            "maintenance": vacuum_payload,
        }

    def relink_current_holdings_security_reviews(
        self,
        business_date: date,
    ) -> dict[str, Any]:
        query = """
        update review.shareholder_holdings_daily as holdings
        set security_review_row_id = security.review_row_id,
            updated_at = timezone('utc', now())
        from control.pipeline_state as security_state
        join control.pipeline_state as holdings_state
          on holdings_state.pipeline_code = 'shareholder_holdings'
        join review.security_master_daily as security
          on security.run_id = security_state.last_run_id
         and security.business_date = %s
        where security_state.pipeline_code = 'security_master'
          and holdings.run_id = holdings_state.last_run_id
          and holdings.business_date = %s
          and security.ticker = holdings.security_identifier
          and holdings.security_review_row_id is distinct from security.review_row_id
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (business_date, business_date))
                updated_count = cursor.rowcount
            connection.commit()
        return {
            "business_date": business_date.isoformat(),
            "updated_link_count": updated_count,
        }

    def write_export_bundle(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
    ) -> dict[str, Any]:
        (
            baseline_query,
            reviewed_query,
            fieldnames,
            business_key_columns,
        ) = self._export_query_details(pipeline_code)
        config_query = """
        select storage_prefix, export_contract_code
        from control.pipeline_registry
        where pipeline_code = %s
        """
        audit_query = """
        select
            pipeline_code,
            dataset_code,
            run_id,
            business_date,
            review_table,
            row_pk,
            column_name,
            old_value,
            new_value,
            change_reason,
            changed_by,
            changed_at,
            origin_mart_row_hash,
            current_row_hash
        from audit.change_log
        where pipeline_code = %s
          and dataset_code = %s
          and run_id = %s
        order by changed_at, change_id
        """
        source_query = """
        select
            source_name,
            source_file_id,
            storage_path,
            object_key,
            metadata,
            row_count,
            content_checksum
        from control.source_file_registry
        where pipeline_code = %s
          and business_date = %s
        order by source_name
        """

        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(config_query, (pipeline_code,))
                config = cursor.fetchone()
                cursor.execute(baseline_query, (run_id,))
                baseline_rows = [dict(row) for row in cursor.fetchall()]
                cursor.execute(reviewed_query, (run_id,))
                reviewed_rows = [dict(row) for row in cursor.fetchall()]
                cursor.execute(audit_query, (pipeline_code, dataset_code, run_id))
                audit_rows = [dict(row) for row in cursor.fetchall()]
                cursor.execute(source_query, (pipeline_code, business_date))
                source_rows = [dict(row) for row in cursor.fetchall()]

        if config is None:
            raise RuntimeError(f"Missing pipeline registry config for {pipeline_code}")
        if not reviewed_rows:
            raise RuntimeError(
                f"No reviewed export rows were available for {pipeline_code} run {run_id}"
            )

        storage_prefix = str(config["storage_prefix"]).strip("/")
        export_dir = (
            Path(self.export_root_dir)
            / storage_prefix
            / business_date.isoformat()
            / str(run_id)
        )
        export_dir.mkdir(parents=True, exist_ok=True)

        baseline_meta = self._write_csv_rows(
            export_dir / "baseline.csv",
            fieldnames=fieldnames,
            rows=baseline_rows,
        )
        reviewed_meta = self._write_csv_rows(
            export_dir / "reviewed.csv",
            fieldnames=fieldnames,
            rows=reviewed_rows,
        )
        audit_meta = self._write_audit_parquet(export_dir / "audit_events.parquet", audit_rows)

        baseline_by_key = {
            tuple(str(row.get(column_name) or "") for column_name in business_key_columns): row
            for row in baseline_rows
        }
        reviewed_by_key = {
            tuple(str(row.get(column_name) or "") for column_name in business_key_columns): row
            for row in reviewed_rows
        }
        changed_keys = sorted(set(baseline_by_key) ^ set(reviewed_by_key) | {
            key
            for key in reviewed_by_key
            if baseline_by_key.get(key) != reviewed_by_key.get(key)
        })

        manifest_payload = {
            "pipeline_code": pipeline_code,
            "dataset_code": dataset_code,
            "run_id": str(run_id),
            "business_date": business_date.isoformat(),
            "export_contract_code": str(config["export_contract_code"]),
            "storage_prefix": storage_prefix,
            "artifacts": {
                "baseline_csv": baseline_meta,
                "reviewed_csv": reviewed_meta,
                "audit_events_parquet": audit_meta,
            },
            "row_counts": {
                "baseline": len(baseline_rows),
                "reviewed": len(reviewed_rows),
                "audit_events": len(audit_rows),
                "changed_rows": len(changed_keys),
            },
            "business_key_columns": business_key_columns,
            "source_artifacts": [
                {
                    "source_name": str(row["source_name"]),
                    "source_file_id": str(row["source_file_id"]),
                    "storage_path": str(row["storage_path"]),
                    "object_key": str(row["object_key"]),
                    "content_checksum": str(row["content_checksum"]),
                    "row_count": int(row["row_count"]),
                    "metadata": row.get("metadata") or {},
                }
                for row in source_rows
            ],
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        manifest_meta = self._write_bundle_manifest(export_dir / "manifest.json", manifest_payload)

        return {
            "bundle_dir": str(export_dir),
            "storage_prefix": storage_prefix,
            "export_contract_code": str(config["export_contract_code"]),
            "baseline_file_path": baseline_meta["path"],
            "baseline_row_count": baseline_meta["row_count"],
            "reviewed_file_path": reviewed_meta["path"],
            "export_row_count": reviewed_meta["row_count"],
            "audit_file_path": audit_meta["path"],
            "audit_row_count": audit_meta["row_count"],
            "manifest_file_path": manifest_meta["path"],
            "changed_row_count": len(changed_keys),
        }

    def write_export_csv(
        self,
        pipeline_code: str,
        run_id: UUID,
        business_date: date,
    ) -> dict[str, Any]:
        bundle = self.write_export_bundle(
            pipeline_code=pipeline_code,
            dataset_code=pipeline_code,
            run_id=run_id,
            business_date=business_date,
        )
        return {
            "file_id": Path(str(bundle["reviewed_file_path"])).name,
            "file_path": bundle["reviewed_file_path"],
            "export_row_count": bundle["export_row_count"],
            "storage_prefix": bundle["storage_prefix"],
            "export_contract_code": bundle["export_contract_code"],
        }

    def prepare_pipeline_run(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
        orchestrator_run_id: str | None = None,
        *,
        status: str = "queued",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        query = """
        insert into observability.pipeline_runs (
            run_id,
            pipeline_code,
            dataset_code,
            business_date,
            status,
            triggered_by,
            started_at,
            orchestrator_run_id,
            metadata
        )
        values (%s, %s, %s, %s, %s, 'dagster', %s, %s, %s)
        on conflict (run_id) do update
        set pipeline_code = excluded.pipeline_code,
            dataset_code = excluded.dataset_code,
            business_date = excluded.business_date,
            status = excluded.status,
            started_at = excluded.started_at,
            ended_at = null,
            orchestrator_run_id = excluded.orchestrator_run_id,
            metadata = excluded.metadata
        """
        step_query = """
        insert into observability.pipeline_step_runs (
            run_id,
            pipeline_code,
            dataset_code,
            business_date,
            step_key,
            step_label,
            step_group,
            sort_order,
            status
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
        on conflict (run_id, step_key) do update
        set step_label = excluded.step_label,
            step_group = excluded.step_group,
            sort_order = excluded.sort_order,
            status = 'pending',
            started_at = null,
            ended_at = null,
            metadata = '{}'::jsonb
        """
        merged_metadata = {
            "mode": "edgar_live",
            "identifier_mapping": ["edgartools", "openfigi", "finnhub"],
            "orchestrator": "dagster",
        }
        if orchestrator_run_id:
            merged_metadata["orchestrator_run_id"] = orchestrator_run_id
        if metadata:
            merged_metadata.update(metadata)
        started_at = datetime.now(UTC)
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        run_id,
                        pipeline_code,
                        dataset_code,
                        business_date,
                        status,
                        started_at,
                        orchestrator_run_id,
                        Jsonb(merged_metadata),
                    ),
                )
                for step in pipeline_step_definitions(pipeline_code):
                    if not step.seed_on_prepare:
                        continue
                    cursor.execute(
                        step_query,
                        (
                            run_id,
                            pipeline_code,
                            dataset_code,
                            business_date,
                            step.step_key,
                            step.label,
                            step.group,
                            step.sort_order,
                        ),
                    )
            connection.commit()

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
            started_at,
            orchestrator_run_id,
            metadata
        )
        values (%s, %s, %s, %s, 'running', 'dagster', %s, %s, %s)
        on conflict (run_id) do update
        set pipeline_code = excluded.pipeline_code,
            dataset_code = excluded.dataset_code,
            business_date = excluded.business_date,
            status = 'running',
            started_at = coalesce(observability.pipeline_runs.started_at, excluded.started_at),
            ended_at = null,
            orchestrator_run_id = coalesce(
                excluded.orchestrator_run_id,
                observability.pipeline_runs.orchestrator_run_id
            ),
            metadata = observability.pipeline_runs.metadata || excluded.metadata
        """
        step_query = """
        insert into observability.pipeline_step_runs (
            run_id,
            pipeline_code,
            dataset_code,
            business_date,
            step_key,
            step_label,
            step_group,
            sort_order,
            status
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
        on conflict (run_id, step_key) do update
        set pipeline_code = excluded.pipeline_code,
            dataset_code = excluded.dataset_code,
            business_date = excluded.business_date,
            step_label = excluded.step_label,
            step_group = excluded.step_group,
            sort_order = excluded.sort_order
        """
        started_at = datetime.now(UTC)
        metadata = {
            "mode": "edgar_live",
            "identifier_mapping": ["edgartools", "openfigi", "finnhub"],
            "orchestrator": "dagster",
            "orchestrator_run_id": orchestrator_run_id,
        }
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        run_id,
                        pipeline_code,
                        dataset_code,
                        business_date,
                        started_at,
                        orchestrator_run_id,
                        Jsonb(metadata),
                    ),
                )
                for step in pipeline_step_definitions(pipeline_code):
                    if not step.seed_on_prepare:
                        continue
                    cursor.execute(
                        step_query,
                        (
                            run_id,
                            pipeline_code,
                            dataset_code,
                            business_date,
                            step.step_key,
                            step.label,
                            step.group,
                            step.sort_order,
                        ),
                    )
            connection.commit()

    def _default_pipeline_step(self, pipeline_code: str, step_key: str) -> tuple[str, str, int]:
        definition = pipeline_step_definition(pipeline_code, step_key)
        if definition is not None:
            return definition.label, definition.group, definition.sort_order
        return step_key.replace("_", " "), "dagster", 999

    def start_pipeline_step(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
        step_key: str,
        metadata: dict[str, Any] | None = None,
        *,
        mark_run_running: bool = True,
    ) -> None:
        step_label, step_group, sort_order = self._default_pipeline_step(pipeline_code, step_key)
        query = """
        insert into observability.pipeline_step_runs (
            run_id,
            pipeline_code,
            dataset_code,
            business_date,
            step_key,
            step_label,
            step_group,
            sort_order,
            status,
            started_at,
            ended_at,
            metadata
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, 'running', timezone('utc', now()), null, %s)
        on conflict (run_id, step_key) do update
        set status = 'running',
            started_at = timezone('utc', now()),
            ended_at = null,
            metadata = excluded.metadata
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
                        step_key,
                        step_label,
                        step_group,
                        sort_order,
                        Jsonb(metadata or {}),
                    ),
                )
                if mark_run_running:
                    cursor.execute(
                        """
                        update observability.pipeline_runs
                        set status = 'running',
                            ended_at = null
                        where run_id = %s
                        """,
                        (run_id,),
                    )
            connection.commit()

    def complete_pipeline_step(
        self,
        run_id: UUID,
        step_key: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        query = """
        update observability.pipeline_step_runs
        set status = 'succeeded',
            started_at = coalesce(started_at, timezone('utc', now())),
            ended_at = timezone('utc', now()),
            metadata = (metadata - array['failed_stage', 'error_class', 'error_message']) || %s
        where run_id = %s
          and step_key = %s
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (Jsonb(metadata or {}), run_id, step_key))
            connection.commit()

    def block_pipeline_run(
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
        step_query = """
        update observability.pipeline_step_runs
        set status = 'blocked',
            ended_at = timezone('utc', now()),
            metadata = metadata || %s
        where run_id = %s
          and status in ('pending', 'running')
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (status, Jsonb(metadata or {}), run_id))
                cursor.execute(step_query, (Jsonb(metadata or {}), run_id))
            connection.commit()

    def load_security_master_tickers(self, run_id: UUID, business_date: date) -> int:
        source_file = self.latest_source_file("sec_company_tickers", business_date)
        rows = self._rows_from_source_file("sec_company_tickers", source_file)
        return self.load_security_master_ticker_records(run_id, business_date, rows)

    def load_security_master_ticker_records(
        self, run_id: UUID, business_date: date, rows: list[dict[str, Any]]
    ) -> int:
        query = """
        insert into raw.sec_company_tickers (
            run_id,
            business_date,
            source_file_id,
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
            %(source_file_id)s,
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
        payload = [
            {
                **row,
                "source_file_id": row.get("source_file_id"),
                "source_payload": Jsonb(row["source_payload"]),
            }
            for row in rows
        ]
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
        source_file = self.latest_source_file("sec_company_facts", business_date)
        rows = self._rows_from_source_file("sec_company_facts", source_file)
        return self.load_security_master_fact_records(run_id, business_date, rows)

    def load_security_master_fact_records(
        self, run_id: UUID, business_date: date, rows: list[dict[str, Any]]
    ) -> int:
        query = """
        insert into raw.sec_company_facts (
            run_id,
            business_date,
            source_file_id,
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
            %(source_file_id)s,
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
        payload = [
            {
                **row,
                "source_file_id": row.get("source_file_id"),
                "source_payload": Jsonb(row["source_payload"]),
            }
            for row in rows
        ]
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
        source_file = self.latest_source_file("holdings_13f_filers", business_date)
        rows = self._rows_from_source_file("holdings_13f_filers", source_file)
        return self.load_shareholder_filer_records(run_id, business_date, rows)

    def load_shareholder_filer_records(
        self, run_id: UUID, business_date: date, rows: list[dict[str, Any]]
    ) -> int:
        query = """
        insert into raw.holdings_13f_filers (
            run_id,
            business_date,
            source_file_id,
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
            %(source_file_id)s,
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
        payload = [
            {
                **row,
                "source_file_id": row.get("source_file_id"),
                "source_payload": Jsonb(row["source_payload"]),
            }
            for row in rows
        ]
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
        source_file = self.latest_source_file("holdings_13f", business_date)
        rows = self._rows_from_source_file("holdings_13f", source_file)
        return self.load_shareholder_holding_records(run_id, business_date, rows)

    def load_shareholder_holding_records(
        self, run_id: UUID, business_date: date, rows: list[dict[str, Any]]
    ) -> int:
        query = """
        insert into raw.holdings_13f (
            run_id,
            business_date,
            source_file_id,
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
            %(source_file_id)s,
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
        payload = [
            {
                **row,
                "source_file_id": row.get("source_file_id"),
                "source_payload": Jsonb(row["source_payload"]),
            }
            for row in rows
        ]
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

    def scan_review_data_issues(
        self,
        dataset_code: str,
        run_id: UUID,
        business_date: date,
        actor: str = "pipeline_scan",
    ) -> dict[str, Any]:
        query = """
        select audit.refresh_review_data_issues_for_run(%s, %s, %s, %s) as payload
        """
        with psycopg.connect(self.direct_database_url, autocommit=False) as connection:
            with connection.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query, (dataset_code, run_id, business_date, actor))
                row = cursor.fetchone()
            connection.commit()
        if row is None:
            raise RuntimeError(
                "audit.refresh_review_data_issues_for_run returned no payload for "
                f"{dataset_code} run {run_id}"
            )
        return dict(row["payload"])

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
            "shareholder_holdings": (
                "delete from review.shareholder_holdings_daily where run_id = %s"
            ),
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
            "shareholder_holdings": (
                "delete from review.shareholder_holdings_daily where run_id = %s"
            ),
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
            metadata = (
                metadata - array[
                    'failed_step',
                    'failed_stage',
                    'error_class',
                    'error_message'
                ]
            ) || %s
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
                cursor.execute(
                    """
                    update observability.pipeline_step_runs
                    set status = case
                            when step_key = %s then 'failed'
                            else 'blocked'
                        end,
                        started_at = case
                            when step_key = %s then coalesce(started_at, timezone('utc', now()))
                            else started_at
                        end,
                        ended_at = timezone('utc', now()),
                        metadata = metadata || %s
                    where run_id = %s
                      and (
                          step_key = %s
                          or status in ('pending', 'running')
                      )
                    """,
                    (
                        step_name,
                        step_name,
                        Jsonb(
                            {
                                "failed_stage": stage,
                                "error_class": error.__class__.__name__,
                                "error_message": str(error),
                            }
                        ),
                        run_id,
                        step_name,
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
    dbt_log_path = Path(dbt_project.target_path) / "logs"
    dbt_log_path.mkdir(parents=True, exist_ok=True)
    shared_partial_parse_path = Path(dbt_project.target_path) / "partial_parse.msgpack"
    shared_partial_parse_path.unlink(missing_ok=True)
    # dagster-dbt copies `partial_parse.msgpack` from the process-level DBT target path
    # into each per-invocation target directory. Clearing these avoids poisoning fresh
    # runtime builds with a stale shared parser cache.
    os.environ.pop("DBT_TARGET_PATH", None)
    os.environ.pop("DBT_LOG_PATH", None)
    return {
        "control_plane": ControlPlaneResource(
            direct_database_url=settings.direct_database_url,
            export_root_dir=settings.export_root_dir,
            landing_root_dir=str(settings.resolved_landing_root_dir),
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
            global_config_flags=["--no-partial-parse"],
        ),
    }
