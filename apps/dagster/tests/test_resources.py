from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest
from dagflow_dagster.resources import ControlPlaneResource
from dagflow_source_adapters.registry import get_adapter
from psycopg.types.json import Jsonb


def build_control_plane(tmp_path: Path) -> ControlPlaneResource:
    return ControlPlaneResource(
        direct_database_url="postgresql://unused",
        export_root_dir=str(tmp_path / "exports"),
        landing_root_dir=str(tmp_path / "landing"),
        edgar_identity="Dagflow test <test@example.com>",
        sec_13f_lookback_days=14,
        sec_13f_filing_limit=25,
        sec_security_focus_limit=10,
    )


def test_capture_security_master_facts_uses_landed_ticker_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    business_date = date(2026, 4, 16)
    ticker_source_file = {
        "source_file_id": 42,
        "storage_path": str(tmp_path / "landing" / "tickers.csv"),
    }
    landed_ticker_rows = [
        {
            "cik": "0000320193",
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "exchange": "Nasdaq",
        }
    ]
    captured: dict[str, object] = {}

    class FakeIngestion:
        def build_security_master_facts_from_ticker_rows(
            self,
            requested_date: date,
            ticker_rows: list[dict[str, object]],
        ) -> list[dict[str, object]]:
            captured["requested_date"] = requested_date
            captured["ticker_rows"] = ticker_rows
            return [
                {
                    "source_record_id": "0000320193:shares_outstanding",
                    "source_payload": {
                        "cik": "0000320193",
                        "ticker": "AAPL",
                        "fact_name": "shares_outstanding",
                        "fact_value": 14681140000.0,
                        "unit": "shares",
                    },
                    "source_payload_hash": "payload-hash",
                    "source_file_name": "CIK0000320193.json",
                    "source_file_row_number": 1,
                    "cik": "0000320193",
                    "fact_name": "shares_outstanding",
                    "fact_value": 14681140000.0,
                    "unit": "shares",
                    "row_hash": "row-hash",
                }
            ]

    control_plane.__dict__["ingestion"] = FakeIngestion()

    monkeypatch.setattr(
        ControlPlaneResource,
        "latest_source_file",
        lambda _self, source_name, _: ticker_source_file
        if source_name == "sec_company_tickers"
        else None,
    )
    monkeypatch.setattr(
        ControlPlaneResource,
        "_rows_from_landed_file",
        lambda _self, source_name, _: landed_ticker_rows
        if source_name == "sec_company_tickers"
        else [],
    )

    def fake_capture_source_file(
        _self,
        source_name: str,
        _requested_date: date,
        *,
        rows: list[dict[str, object]] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        captured["source_name"] = source_name
        captured["rows"] = rows
        captured["metadata"] = metadata
        return {"source_file_id": 77, "storage_path": "derived.csv", "row_count": len(rows or [])}

    monkeypatch.setattr(ControlPlaneResource, "capture_source_file", fake_capture_source_file)

    control_plane.capture_security_master_facts_from_landed_tickers(business_date)

    assert captured["requested_date"] == business_date
    assert captured["ticker_rows"] == landed_ticker_rows
    assert captured["source_name"] == "sec_company_facts"
    assert isinstance(captured["rows"], list)
    assert captured["metadata"] == {
        "derived_from_source_name": "sec_company_tickers",
        "derived_from_source_file_id": "42",
        "derived_from_storage_path": str(ticker_source_file["storage_path"]),
    }


def test_capture_shareholder_filers_uses_landed_holdings_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    business_date = date(2026, 4, 16)
    holdings_source_file = {
        "source_file_id": 99,
        "storage_path": str(tmp_path / "landing" / "holdings.csv"),
    }
    landed_holding_rows = [
        {
            "accession_number": "0001",
            "filer_cik": "0000123456",
            "source_payload": {
                "filer_name": "Alpha Capital",
                "report_period": "2026-03-31",
                "filing_date": "2026-04-15",
            },
        }
    ]
    captured: dict[str, object] = {}

    class FakeIngestion:
        def build_shareholder_filers_from_holding_rows(
            self,
            holding_rows: list[dict[str, object]],
        ) -> list[dict[str, object]]:
            captured["holding_rows"] = holding_rows
            return [
                {
                    "source_record_id": "0001",
                    "source_payload": {
                        "accession_number": "0001",
                        "filer_cik": "0000123456",
                        "filer_name": "Alpha Capital",
                        "report_period": "2026-03-31",
                        "filing_date": "2026-04-15",
                        "focus_holdings_count": 1,
                    },
                    "source_payload_hash": "payload-hash",
                    "source_file_name": "0001",
                    "source_file_row_number": 1,
                    "accession_number": "0001",
                    "filer_cik": "0000123456",
                    "filer_name": "Alpha Capital",
                    "report_period": date(2026, 3, 31),
                    "row_hash": "row-hash",
                }
            ]

    control_plane.__dict__["ingestion"] = FakeIngestion()

    monkeypatch.setattr(
        ControlPlaneResource,
        "latest_source_file",
        lambda _self, source_name, _: holdings_source_file
        if source_name == "holdings_13f"
        else None,
    )
    monkeypatch.setattr(
        ControlPlaneResource,
        "_rows_from_landed_file",
        lambda _self, source_name, _: landed_holding_rows
        if source_name == "holdings_13f"
        else [],
    )

    def fake_capture_source_file(
        _self,
        source_name: str,
        requested_date: date,
        *,
        rows: list[dict[str, object]] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        captured["source_name"] = source_name
        captured["requested_date"] = requested_date
        captured["rows"] = rows
        captured["metadata"] = metadata
        return {"source_file_id": 123, "storage_path": "derived.csv", "row_count": len(rows or [])}

    monkeypatch.setattr(ControlPlaneResource, "capture_source_file", fake_capture_source_file)

    control_plane.capture_shareholder_filers_from_landed_holdings(business_date)

    assert captured["holding_rows"] == landed_holding_rows
    assert captured["source_name"] == "holdings_13f_filers"
    assert captured["requested_date"] == business_date
    assert isinstance(captured["rows"], list)
    assert captured["metadata"] == {
        "derived_from_source_name": "holdings_13f",
        "derived_from_source_file_id": "99",
        "derived_from_storage_path": str(holdings_source_file["storage_path"]),
    }


def test_build_shareholder_filers_from_holding_rows_is_deterministic() -> None:
    control_plane = build_control_plane(Path("/tmp"))
    service = control_plane.ingestion
    holding_rows = [
        {
            "accession_number": "0001",
            "filer_cik": "0000123456",
            "source_payload": {
                "filer_name": "Alpha Capital",
                "report_period": "2026-03-31",
                "filing_date": "2026-04-15",
            },
        },
        {
            "accession_number": "0001",
            "filer_cik": "0000123456",
            "source_payload": {
                "filer_name": "Alpha Capital",
                "report_period": "2026-03-31",
                "filing_date": "2026-04-15",
            },
        },
    ]

    filer_rows = service.build_shareholder_filers_from_holding_rows(holding_rows)

    assert len(filer_rows) == 1
    assert filer_rows[0]["source_record_id"] == "0001"
    assert filer_rows[0]["source_payload"]["focus_holdings_count"] == 2


def test_list_available_source_dates_reads_landing_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)

    monkeypatch.setattr(
        ControlPlaneResource,
        "source_descriptors_for_pipeline",
        lambda _self, pipeline_code, business_date: [
            descriptor
            for descriptor in get_adapter("sec_json").build_sources(business_date)
            if descriptor.pipeline_code == pipeline_code
        ],
    )

    ready_date = date(2026, 1, 9)
    partial_date = date(2026, 1, 10)
    ready_ticker_dir = (
        tmp_path
        / "landing"
        / "security_master"
        / "sec_company_tickers"
        / ready_date.isoformat()
    )
    ready_ticker_dir.mkdir(parents=True, exist_ok=True)
    (ready_ticker_dir / "sec_company_tickers.csv").write_text(
        (
            "source_record_id,cik,ticker,company_name,exchange,"
            "holder_count_recent_window,reported_shares_recent_window,"
            "top_holder_shares_recent_window,source_payload_hash,row_hash\n"
            "AAPL,0000320193,AAPL,Apple Inc.,Nasdaq,10,100,25,payload,row-hash\n"
        ),
        encoding="utf-8",
    )
    ready_facts_dir = (
        tmp_path / "landing" / "security_master" / "sec_company_facts" / ready_date.isoformat()
    )
    ready_facts_dir.mkdir(parents=True, exist_ok=True)
    (ready_facts_dir / "sec_company_facts.csv").write_text(
        (
            "source_record_id,cik,fact_name,fact_value,unit,retrieval_status,"
            "source_payload_hash,row_hash\n"
            "0000320193:shares_outstanding,0000320193,shares_outstanding,"
            "14681140000,shares,ok,payload,row-hash\n"
        ),
        encoding="utf-8",
    )
    partial_ticker_dir = (
        tmp_path
        / "landing"
        / "security_master"
        / "sec_company_tickers"
        / partial_date.isoformat()
    )
    partial_ticker_dir.mkdir(parents=True, exist_ok=True)
    (partial_ticker_dir / "sec_company_tickers.csv").write_text(
        (
            "source_record_id,cik,ticker,company_name,exchange,"
            "holder_count_recent_window,reported_shares_recent_window,"
            "top_holder_shares_recent_window,source_payload_hash,row_hash\n"
            "MSFT,0000789019,MSFT,Microsoft Corp,Nasdaq,8,80,20,payload,row-hash\n"
        ),
        encoding="utf-8",
    )

    available = control_plane.list_available_source_dates("security_master")

    assert [entry["business_date"] for entry in available] == [
        partial_date.isoformat(),
        ready_date.isoformat(),
    ]
    assert available[0]["is_ready"] is False
    assert available[0]["missing_sources"] == ["sec_company_facts"]
    assert available[1]["is_ready"] is True
    assert available[1]["available_source_count"] == 2


def test_capture_source_file_reuses_existing_landing_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    business_date = date(2026, 1, 9)
    landing_dir = (
        tmp_path / "landing" / "security_master" / "sec_company_tickers" / business_date.isoformat()
    )
    landing_dir.mkdir(parents=True, exist_ok=True)
    raw_path = landing_dir / "sec_company_tickers.csv"
    raw_path.write_text(
        (
            "source_record_id,cik,ticker,company_name,exchange,"
            "holder_count_recent_window,reported_shares_recent_window,"
            "top_holder_shares_recent_window,source_payload_hash,row_hash\n"
            "AAPL,0000320193,AAPL,Apple Inc.,Nasdaq,10,100,25,payload,row-hash\n"
        ),
        encoding="utf-8",
    )

    registrations: list[dict[str, object]] = []

    def fake_register_source_file(
        _self,
        *,
        descriptor,
        storage_path: Path,
        checksum: str,
        row_count: int,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        payload = {
            "source_file_id": str(uuid4()),
            "pipeline_code": descriptor.pipeline_code,
            "dataset_code": descriptor.dataset_code,
            "source_name": descriptor.source_name,
            "business_date": descriptor.business_date,
            "storage_path": str(storage_path),
            "object_key": descriptor.object_key,
            "source_url": descriptor.source_url,
            "source_format": descriptor.expected_format,
            "capture_mode": descriptor.ingestion_mode,
            "content_checksum": checksum,
            "row_count": row_count,
            "metadata": metadata or {},
        }
        registrations.append(payload)
        return payload

    monkeypatch.setattr(
        ControlPlaneResource,
        "_register_source_file",
        fake_register_source_file,
    )

    file_record = control_plane.capture_source_file("sec_company_tickers", business_date)
    canonical_path = landing_dir / "sec_company_tickers.parquet"
    manifest_path = landing_dir / "manifest.json"

    assert file_record["storage_path"] == str(raw_path)
    assert canonical_path.exists() is True
    assert manifest_path.exists() is True
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["row_count"] == 1
    assert registrations[-1]["metadata"]["canonical_storage_path"] == str(canonical_path)
    assert registrations[-1]["metadata"]["manifest_storage_path"] == str(manifest_path)


def test_rows_from_source_file_rebinds_current_source_file_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    file_record = {"source_file_id": "current-source-file"}

    monkeypatch.setattr(
        ControlPlaneResource,
        "_rows_from_canonical_file",
        lambda _self, _file_record: [
            {"source_file_id": "stale-source-file", "ticker": "AAPL"},
            {"source_file_id": "stale-source-file", "ticker": "MSFT"},
        ],
    )

    rows = control_plane._rows_from_source_file("sec_company_tickers", file_record)

    assert rows == [
        {"source_file_id": "current-source-file", "ticker": "AAPL"},
        {"source_file_id": "current-source-file", "ticker": "MSFT"},
    ]


@pytest.mark.parametrize(
    ("pipeline_code", "expected_review_table", "forbidden_preview_table"),
    [
        (
            "security_master",
            "from review.security_master_daily",
            "from export.security_master_preview",
        ),
        (
            "shareholder_holdings",
            "from review.shareholder_holdings_daily",
            "from export.shareholder_holdings_preview",
        ),
    ],
)
def test_export_queries_read_review_tables_directly(
    tmp_path: Path,
    pipeline_code: str,
    expected_review_table: str,
    forbidden_preview_table: str,
) -> None:
    control_plane = build_control_plane(tmp_path)

    _, reviewed_query, _, business_key_columns = control_plane._export_query_details(
        pipeline_code
    )

    normalized_query = " ".join(reviewed_query.split())
    assert expected_review_table in normalized_query
    assert forbidden_preview_table not in normalized_query
    assert business_key_columns == ["_row_export_key"]


class _FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, object]] = []
        self.fetchone_results: list[object] = []
        self.fetchall_results: list[list[object]] = []

    def execute(self, query: str, params: object = None) -> None:
        self.calls.append(("execute", query, params))

    def executemany(self, query: str, seq: list[dict[str, object]]) -> None:
        self.calls.append(("executemany", query, list(seq)))

    def fetchone(self) -> object | None:
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None

    def fetchall(self) -> list[object]:
        if self.fetchall_results:
            return self.fetchall_results.pop(0)
        return []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.committed = False

    def cursor(self, **_kwargs) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


@pytest.mark.parametrize(
    ("method_name", "table_name", "rows"),
    [
        (
            "load_security_master_ticker_records",
            "raw.sec_company_tickers",
            [
                {
                    "source_file_id": 1,
                    "source_record_id": "AAPL",
                    "source_payload": {"ticker": "AAPL"},
                    "source_payload_hash": "payload",
                    "source_file_name": "tickers.csv",
                    "source_file_row_number": 1,
                    "cik": "0000320193",
                    "ticker": "AAPL",
                    "company_name": "Apple Inc.",
                    "exchange": "Nasdaq",
                    "row_hash": "row-hash",
                }
            ],
        ),
        (
            "load_security_master_fact_records",
            "raw.sec_company_facts",
            [
                {
                    "source_file_id": 2,
                    "source_record_id": "0000320193:shares_outstanding",
                    "source_payload": {"fact_name": "shares_outstanding"},
                    "source_payload_hash": "payload",
                    "source_file_name": "facts.csv",
                    "source_file_row_number": 1,
                    "cik": "0000320193",
                    "fact_name": "shares_outstanding",
                    "fact_value": 14681140000.0,
                    "unit": "shares",
                    "row_hash": "row-hash",
                }
            ],
        ),
        (
            "load_shareholder_filer_records",
            "raw.holdings_13f_filers",
            [
                {
                    "source_file_id": 3,
                    "source_record_id": "0001",
                    "source_payload": {"filer_name": "Alpha Capital"},
                    "source_payload_hash": "payload",
                    "source_file_name": "filers.csv",
                    "source_file_row_number": 1,
                    "accession_number": "0001",
                    "filer_cik": "0000123456",
                    "filer_name": "Alpha Capital",
                    "report_period": date(2026, 3, 31),
                    "row_hash": "row-hash",
                }
            ],
        ),
        (
            "load_shareholder_holding_records",
            "raw.holdings_13f",
            [
                {
                    "source_file_id": 4,
                    "source_record_id": "0001:AAPL",
                    "source_payload": {"filer_name": "Alpha Capital"},
                    "source_payload_hash": "payload",
                    "source_file_name": "holdings.csv",
                    "source_file_row_number": 1,
                    "accession_number": "0001",
                    "filer_cik": "0000123456",
                    "security_identifier": "AAPL",
                    "cusip": "037833100",
                    "shares_held": 10.0,
                    "market_value": 100.0,
                    "row_hash": "row-hash",
                }
            ],
        ),
    ],
)
def test_raw_loaders_delete_existing_run_rows_before_insert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    table_name: str,
    rows: list[dict[str, object]],
) -> None:
    control_plane = build_control_plane(tmp_path)
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)

    def fake_connect(*_args, **_kwargs) -> _FakeConnection:
        return connection

    monkeypatch.setattr("dagflow_dagster.resources.psycopg.connect", fake_connect)

    run_id = uuid4()
    business_date = date(2026, 4, 16)
    method = getattr(control_plane, method_name)
    inserted_count = method(run_id, business_date, rows)

    assert inserted_count == len(rows)
    assert connection.committed is True
    assert cursor.calls[0][0] == "execute"
    assert f"delete from {table_name} where run_id = %s" in cursor.calls[0][1].lower()
    assert cursor.calls[0][2] == (run_id,)
    assert cursor.calls[1][0] == "executemany"
    assert len(cursor.calls[1][2]) == len(rows)


def test_cleanup_operational_run_clears_state_and_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    run_id = uuid4()
    business_date = date(2026, 1, 9)

    monkeypatch.setattr(
        ControlPlaneResource,
        "reset_pipeline_run_data",
        lambda *_args, **_kwargs: {"deleted": True},
    )
    monkeypatch.setattr(
        ControlPlaneResource,
        "purge_run_residual_history",
        lambda *_args, **_kwargs: {"deleted_workflow_event_count": 1},
    )
    monkeypatch.setattr(
        ControlPlaneResource,
        "clear_source_file_registry",
        lambda *_args, **_kwargs: {"deleted_source_file_count": 2},
    )
    monkeypatch.setattr(
        ControlPlaneResource,
        "clear_pipeline_state",
        lambda *_args, **_kwargs: {"approval_state": "pending_review"},
    )
    monkeypatch.setattr(
        ControlPlaneResource,
        "vacuum_pipeline_workspace",
        lambda *_args, **_kwargs: {"vacuumed_relations": ["raw.sec_company_tickers"]},
    )

    payload = control_plane.cleanup_operational_run(
        pipeline_code="security_master",
        dataset_code="security_master",
        run_id=run_id,
        business_date=business_date,
    )

    assert payload["cleanup"] == {"deleted": True}
    assert payload["residual_cleanup"] == {"deleted_workflow_event_count": 1}
    assert payload["source_cleanup"] == {"deleted_source_file_count": 2}
    assert payload["cleared_state"] == {"approval_state": "pending_review"}
    assert payload["maintenance"] == {
        "vacuumed_relations": ["raw.sec_company_tickers"]
    }


def test_clear_source_file_registry_can_purge_all_pipeline_dates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)
    cursor.rowcount = 4  # type: ignore[attr-defined]

    def fake_connect(*_args, **_kwargs) -> _FakeConnection:
        return connection

    monkeypatch.setattr("dagflow_dagster.resources.psycopg.connect", fake_connect)

    payload = control_plane.clear_source_file_registry("security_master")

    assert payload == {
        "pipeline_code": "security_master",
        "business_date": None,
        "deleted_source_file_count": 4,
    }
    assert "where pipeline_code = %s" in cursor.calls[0][1].lower()
    assert "business_date" not in cursor.calls[0][1].lower()
    assert cursor.calls[0][2] == ("security_master",)


def test_vacuum_relations_for_pipeline_includes_operational_metadata() -> None:
    relations = ControlPlaneResource._vacuum_relations_for_pipeline("security_master")

    assert "audit.review_data_issues" in relations
    assert "observability.pipeline_step_runs" in relations
    assert "observability.pipeline_asset_metrics" in relations
    assert "observability.pipeline_failure_rows" in relations
    assert "control.export_bundle_registry" in relations
    assert "export.security_master_preview" in relations
    assert "marts.dim_security_snapshot" in relations


def test_wipe_pipeline_workspace_for_load_removes_malformed_dq_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    cursor = _FakeCursor()
    cursor.rowcount = 0  # type: ignore[attr-defined]
    connection = _FakeConnection(cursor)

    def fake_connect(*_args, **_kwargs) -> _FakeConnection:
        return connection

    monkeypatch.setattr("dagflow_dagster.resources.psycopg.connect", fake_connect)
    monkeypatch.setattr(
        ControlPlaneResource,
        "_workspace_relations_for_pipeline",
        staticmethod(lambda _pipeline_code: []),
    )
    monkeypatch.setattr(
        ControlPlaneResource,
        "clear_pipeline_state",
        lambda *_args, **_kwargs: {"approval_state": "pending_review"},
    )
    monkeypatch.setattr(
        ControlPlaneResource,
        "vacuum_pipeline_workspace",
        lambda *_args, **_kwargs: {"vacuumed_relations": []},
    )

    control_plane.wipe_pipeline_workspace_for_load(
        pipeline_code="shareholder_holdings",
        dataset_code="shareholder_holdings",
    )

    dq_query = next(
        query
        for call, query, _params in cursor.calls
        if call == "execute" and "delete from observability.data_quality_results" in query.lower()
    )
    assert "run_id is null" in dq_query.lower()
    assert "trim(pipeline_code)" in dq_query.lower()
    assert "trim(dataset_code)" in dq_query.lower()


def test_record_export_bundle_registry_persists_execution_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    cursor = _FakeCursor()
    cursor.fetchone_results = [
        {
            "pipeline_code": "security_master",
            "dataset_code": "security_master",
            "run_id": "8948c205-fe07-58ea-a625-a98cd99838b0",
            "business_date": date(2026, 1, 10),
            "baseline_file_path": "/tmp/baseline.csv",
            "reviewed_file_path": "/tmp/reviewed.csv",
            "audit_file_path": "/tmp/audit_events.parquet",
            "manifest_file_path": "/tmp/manifest.json",
            "exported_at": "2026-04-17T02:30:00+00:00",
        }
    ]
    connection = _FakeConnection(cursor)

    def fake_connect(*_args, **_kwargs) -> _FakeConnection:
        return connection

    monkeypatch.setattr("dagflow_dagster.resources.psycopg.connect", fake_connect)
    monkeypatch.setattr(
        ControlPlaneResource,
        "_execution_snapshot",
        lambda *_args, **_kwargs: {
            "run": {"status": "exported"},
            "steps": [{"step_key": "security_master_export_bundle", "status": "succeeded"}],
        },
    )

    payload = control_plane.record_export_bundle_registry(
        pipeline_code="security_master",
        dataset_code="security_master",
        run_id=uuid4(),
        business_date=date(2026, 1, 10),
        artifact_payload={
            "baseline_file_path": "/tmp/baseline.csv",
            "reviewed_file_path": "/tmp/reviewed.csv",
            "audit_file_path": "/tmp/audit_events.parquet",
            "manifest_file_path": "/tmp/manifest.json",
        },
    )

    assert connection.committed is True
    assert payload["reviewed_file_path"] == "/tmp/reviewed.csv"
    assert "insert into control.export_bundle_registry" in cursor.calls[0][1].lower()
    params = cursor.calls[0][2]
    assert isinstance(params[8], Jsonb)
    assert params[8].obj["observability_run"]["status"] == "exported"
    assert params[8].obj["artifacts"]["audit_file_path"] == "/tmp/audit_events.parquet"


def test_prepare_pipeline_run_resets_metadata_for_reruns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)

    def fake_connect(*_args, **_kwargs) -> _FakeConnection:
        return connection

    monkeypatch.setattr("dagflow_dagster.resources.psycopg.connect", fake_connect)

    control_plane.prepare_pipeline_run(
        pipeline_code="shareholder_holdings",
        dataset_code="shareholder_holdings",
        run_id=uuid4(),
        business_date=date(2026, 1, 9),
        orchestrator_run_id="dagster-run-123",
        status="running",
        metadata={"launch_relation": "primary"},
    )

    assert connection.committed is True
    run_query = cursor.calls[0][1]
    step_query = cursor.calls[1][1]
    assert "started_at = excluded.started_at" in run_query
    assert "metadata = excluded.metadata" in run_query
    run_params = cursor.calls[0][2]
    assert run_params[5] is not None
    assert isinstance(run_params[7], Jsonb)
    assert run_params[7].obj["launch_relation"] == "primary"
    assert "status = 'pending'" in step_query
    assert "metadata = '{}'::jsonb" in step_query


def test_ensure_pipeline_run_preserves_existing_step_statuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)

    def fake_connect(*_args, **_kwargs) -> _FakeConnection:
        return connection

    monkeypatch.setattr("dagflow_dagster.resources.psycopg.connect", fake_connect)

    control_plane.ensure_pipeline_run(
        pipeline_code="security_master",
        dataset_code="security_master",
        run_id=uuid4(),
        business_date=date(2026, 1, 9),
        orchestrator_run_id="dagster-run-456",
    )

    assert connection.committed is True
    run_query = cursor.calls[0][1]
    step_query = cursor.calls[1][1]
    run_params = cursor.calls[0][2]
    assert (
        "started_at = coalesce(observability.pipeline_runs.started_at, excluded.started_at)"
        in run_query
    )
    assert "metadata = observability.pipeline_runs.metadata || excluded.metadata" in run_query
    assert isinstance(run_params[6], Jsonb)
    assert run_params[6].obj["orchestrator_run_id"] == "dagster-run-456"
    assert "on conflict (run_id, step_key) do update" in step_query
    assert "step_label = excluded.step_label" in step_query
    assert "metadata = '{}'::jsonb" not in step_query
    assert "status = 'pending'" not in step_query.split("do update", 1)[1]


def test_start_pipeline_step_replaces_stale_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)

    def fake_connect(*_args, **_kwargs) -> _FakeConnection:
        return connection

    monkeypatch.setattr("dagflow_dagster.resources.psycopg.connect", fake_connect)

    control_plane.start_pipeline_step(
        pipeline_code="shareholder_holdings",
        dataset_code="shareholder_holdings",
        run_id=uuid4(),
        business_date=date(2026, 1, 9),
        step_key="shareholder_holdings_transform_assets",
        metadata={"stage": "dbt_build"},
    )

    assert connection.committed is True
    query = cursor.calls[0][1]
    assert "started_at = timezone('utc', now())" in query
    assert "metadata = excluded.metadata" in query


def test_complete_pipeline_run_and_step_clear_failure_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_plane = build_control_plane(tmp_path)
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)

    def fake_connect(*_args, **_kwargs) -> _FakeConnection:
        return connection

    monkeypatch.setattr("dagflow_dagster.resources.psycopg.connect", fake_connect)

    run_id = uuid4()
    control_plane.complete_pipeline_step(
        run_id,
        "shareholder_holdings_transform_assets",
        {"row_count": 10},
    )
    control_plane.complete_pipeline_run(run_id, "pending_review", {"review_issue_count": 3})

    assert connection.committed is True
    step_query = " ".join(cursor.calls[0][1].split())
    run_query = " ".join(cursor.calls[1][1].split())
    assert "metadata - array['failed_stage', 'error_class', 'error_message']" in step_query
    expected_run_fragment = (
        "metadata - array[ 'failed_step', 'failed_stage', 'error_class', "
        "'error_message' ]"
    )
    assert expected_run_fragment in run_query
