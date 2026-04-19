from datetime import date

import pandas as pd
import pytest
from dagflow_security_shareholder.ingestion import (
    EdgarIngestionService,
    Historical13FFiling,
    IdentifierResolver,
    Recent13FSnapshot,
)


def test_identifier_resolver_canonicalizes_class_share_symbols() -> None:
    resolver = IdentifierResolver(
        sec_ticker_aliases={"BRKB": "BRK-B", "GOOG": "GOOGL", "GOOGL": "GOOGL"}
    )

    assert resolver.canonicalize("BRKB") == "BRK-B"
    assert resolver.canonicalize("BRK.B") == "BRK-B"
    assert resolver.canonicalize("GOOG") == "GOOGL"
    assert resolver.canonicalize("GOOGL") == "GOOGL"


def test_security_master_tickers_follow_focus_universe_order() -> None:
    business_date = date(2026, 4, 16)
    service = EdgarIngestionService(
        edgar_identity="Dagflow local test support@example.com",
        sec_13f_lookback_days=14,
        sec_13f_filing_limit=50,
        sec_security_focus_limit=3,
    )
    service._company_ticker_rows = [
        {
            "cik": "0000320193",
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "exchange": "Nasdaq",
        },
        {
            "cik": "0000789019",
            "ticker": "MSFT",
            "company_name": "Microsoft Corp",
            "exchange": "Nasdaq",
        },
        {
            "cik": "0001067983",
            "ticker": "BRK-B",
            "company_name": "Berkshire Hathaway Inc",
            "exchange": "NYSE",
        },
    ]
    service._ticker_aliases = {"AAPL": "AAPL", "MSFT": "MSFT", "BRKB": "BRK-B"}
    service._recent_13f_cache[business_date] = Recent13FSnapshot(
        filer_records=[],
        holding_records=[],
        focus_tickers=["MSFT", "AAPL"],
        ticker_holder_counts={"MSFT": 20, "AAPL": 18},
        filings_scanned=10,
        filings_loaded=10,
        skipped_filings=[],
    )

    records = service.build_security_master_tickers(business_date)

    assert [record["ticker"] for record in records] == ["MSFT", "AAPL"]
    assert records[0]["source_payload"]["holder_count_recent_window"] == 20


def test_historical_13f_snapshot_uses_latest_filing_per_filer() -> None:
    service = EdgarIngestionService(
        edgar_identity="Dagflow local test support@example.com",
        sec_13f_lookback_days=14,
        sec_13f_filing_limit=50,
        sec_security_focus_limit=5,
    )

    filings = [
        Historical13FFiling(
            accession_number="0001",
            filing_date=date(2025, 6, 30),
            filer_cik="0000000001",
            filer_name="Alpha",
            report_period=date(2025, 6, 30),
            holdings=[
                {
                    "ticker": "AAPL",
                    "cusip": "037833100",
                    "issuer_name": "Apple Inc.",
                    "shares_held": 10.0,
                    "market_value": 100.0,
                    "ticker_source": "edgartools",
                    "sources": ["edgartools"],
                    "row_count": 1,
                }
            ],
        ),
        Historical13FFiling(
            accession_number="0002",
            filing_date=date(2025, 9, 30),
            filer_cik="0000000001",
            filer_name="Alpha",
            report_period=date(2025, 9, 30),
            holdings=[
                {
                    "ticker": "MSFT",
                    "cusip": "594918104",
                    "issuer_name": "Microsoft Corp",
                    "shares_held": 20.0,
                    "market_value": 300.0,
                    "ticker_source": "edgartools",
                    "sources": ["edgartools"],
                    "row_count": 1,
                }
            ],
        ),
        Historical13FFiling(
            accession_number="0003",
            filing_date=date(2025, 9, 20),
            filer_cik="0000000002",
            filer_name="Beta",
            report_period=date(2025, 9, 20),
            holdings=[
                {
                    "ticker": "AAPL",
                    "cusip": "037833100",
                    "issuer_name": "Apple Inc.",
                    "shares_held": 15.0,
                    "market_value": 200.0,
                    "ticker_source": "edgartools",
                    "sources": ["edgartools"],
                    "row_count": 1,
                }
            ],
        ),
    ]

    snapshot = service.historical_13f_snapshot(date(2025, 10, 1), filings)

    assert snapshot.focus_tickers == ["MSFT", "AAPL"]
    assert {row["accession_number"] for row in snapshot.filer_records} == {"0002", "0003"}
    assert {row["security_identifier"] for row in snapshot.holding_records} == {"AAPL", "MSFT"}


def test_shares_outstanding_series_falls_back_to_us_gaap_common_stock() -> None:
    service = EdgarIngestionService(
        edgar_identity="Dagflow local test support@example.com",
        sec_13f_lookback_days=14,
        sec_13f_filing_limit=50,
        sec_security_focus_limit=5,
    )

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "facts": {
                    "us-gaap": {
                        "CommonStockSharesOutstanding": {
                            "units": {
                                "shares": [
                                    {"end": "2025-03-31", "val": 12155000000},
                                    {"end": "2025-06-30", "val": 12104000000},
                                ]
                            }
                        }
                    }
                }
            }

    class FakeClient:
        def get(self, *_args, **_kwargs) -> FakeResponse:
            return FakeResponse()

    service.client = FakeClient()  # type: ignore[assignment]
    service._last_sec_request_at = 0.0
    service.sec_max_requests_per_second = 1_000_000.0

    history = service._shares_outstanding_series("0001652044")

    assert history == [
        (date(2025, 3, 31), 12155000000.0),
        (date(2025, 6, 30), 12104000000.0),
    ]
    assert service._shares_outstanding_as_of("0001652044", date(2025, 4, 1)) == 12155000000.0


def test_company_ticker_rows_prefer_configured_ticker_per_cik(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = EdgarIngestionService(
        edgar_identity="Dagflow local test support@example.com",
        sec_13f_lookback_days=14,
        sec_13f_filing_limit=50,
        sec_security_focus_limit=5,
    )

    monkeypatch.setattr(EdgarIngestionService, "_ensure_identity", lambda _self: None)
    monkeypatch.setattr(
        "dagflow_security_shareholder.ingestion.get_company_tickers",
        lambda: pd.DataFrame(
            [
                {
                    "cik": "0001652044",
                    "ticker": "GOOG",
                    "company": "Alphabet Inc.",
                    "exchange": "Nasdaq",
                },
                {
                    "cik": "0001652044",
                    "ticker": "GOOGL",
                    "company": "Alphabet Inc.",
                    "exchange": "Nasdaq",
                },
                {
                    "cik": "0000320193",
                    "ticker": "AAPL",
                    "company": "Apple Inc.",
                    "exchange": "Nasdaq",
                },
            ]
        ),
    )

    rows = service.company_ticker_rows()

    assert [row["ticker"] for row in rows if row["cik"] == "0001652044"] == ["GOOGL"]
    assert service._ticker_aliases["GOOG"] == "GOOGL"
