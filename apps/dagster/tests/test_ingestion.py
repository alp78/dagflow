from datetime import date

from dagflow_dagster.ingestion import (
    EdgarIngestionService,
    Historical13FFiling,
    IdentifierResolver,
    Recent13FSnapshot,
)


def test_identifier_resolver_canonicalizes_class_share_symbols() -> None:
    resolver = IdentifierResolver(sec_ticker_aliases={"BRKB": "BRK-B", "GOOGL": "GOOGL"})

    assert resolver.canonicalize("BRKB") == "BRK-B"
    assert resolver.canonicalize("BRK.B") == "BRK-B"
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
