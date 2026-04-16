from __future__ import annotations

from datetime import date, timedelta
from hashlib import md5
from typing import cast
from uuid import UUID

SecuritySeed = tuple[str, str, str, int, float]

SECURITY_UNIVERSE: tuple[SecuritySeed, ...] = (
    ("AAPL", "Apple Inc.", "NASDAQ", 15_400_000_000, 183.00),
    ("MSFT", "Microsoft Corp.", "NASDAQ", 7_460_000_000, 379.00),
    ("NVDA", "NVIDIA Corp.", "NASDAQ", 24_600_000_000, 116.00),
    ("AMZN", "Amazon.com Inc.", "NASDAQ", 10_600_000_000, 182.00),
    ("GOOGL", "Alphabet Inc. Class A", "NASDAQ", 12_300_000_000, 171.00),
    ("META", "Meta Platforms Inc.", "NASDAQ", 2_550_000_000, 502.00),
    ("TSLA", "Tesla Inc.", "NASDAQ", 3_190_000_000, 172.00),
    ("BRKB", "Berkshire Hathaway Class B", "NYSE", 2_200_000_000, 415.00),
    ("JPM", "JPMorgan Chase & Co.", "NYSE", 2_900_000_000, 198.00),
    ("V", "Visa Inc.", "NYSE", 1_650_000_000, 274.00),
    ("WMT", "Walmart Inc.", "NYSE", 8_050_000_000, 60.00),
    ("XOM", "Exxon Mobil Corp.", "NYSE", 4_200_000_000, 116.00),
    ("MA", "Mastercard Inc.", "NYSE", 917_000_000, 471.00),
    ("AVGO", "Broadcom Inc.", "NASDAQ", 465_000_000, 1340.00),
    ("COST", "Costco Wholesale Corp.", "NASDAQ", 443_000_000, 720.00),
    ("NFLX", "Netflix Inc.", "NASDAQ", 430_000_000, 620.00),
    ("HD", "Home Depot Inc.", "NYSE", 990_000_000, 338.00),
    ("PG", "Procter & Gamble Co.", "NYSE", 2_360_000_000, 166.00),
    ("ABBV", "AbbVie Inc.", "NYSE", 1_770_000_000, 172.00),
    ("ORCL", "Oracle Corp.", "NYSE", 2_740_000_000, 125.00),
    ("KO", "Coca-Cola Co.", "NYSE", 4_300_000_000, 62.00),
    ("PEP", "PepsiCo Inc.", "NASDAQ", 1_370_000_000, 176.00),
    ("BAC", "Bank of America Corp.", "NYSE", 7_900_000_000, 38.00),
    ("CRM", "Salesforce Inc.", "NYSE", 974_000_000, 289.00),
    ("CSCO", "Cisco Systems Inc.", "NASDAQ", 4_020_000_000, 48.00),
    ("ACN", "Accenture plc", "NYSE", 628_000_000, 347.00),
    ("TMO", "Thermo Fisher Scientific Inc.", "NYSE", 385_000_000, 572.00),
    ("LIN", "Linde plc", "NASDAQ", 482_000_000, 465.00),
    ("MCD", "McDonald's Corp.", "NYSE", 722_000_000, 273.00),
    ("AMD", "Advanced Micro Devices Inc.", "NASDAQ", 1_620_000_000, 165.00),
    ("DIS", "Walt Disney Co.", "NYSE", 1_820_000_000, 111.00),
    ("ADBE", "Adobe Inc.", "NASDAQ", 440_000_000, 495.00),
    ("MRK", "Merck & Co. Inc.", "NYSE", 2_530_000_000, 130.00),
    ("ABT", "Abbott Laboratories", "NYSE", 1_730_000_000, 107.00),
    ("CMCSA", "Comcast Corp.", "NASDAQ", 3_960_000_000, 41.00),
    ("WFC", "Wells Fargo & Co.", "NYSE", 3_450_000_000, 59.00),
    ("INTC", "Intel Corp.", "NASDAQ", 4_250_000_000, 31.00),
    ("UNH", "UnitedHealth Group Inc.", "NYSE", 920_000_000, 485.00),
    ("QCOM", "Qualcomm Inc.", "NASDAQ", 1_110_000_000, 169.00),
    ("TXN", "Texas Instruments Inc.", "NASDAQ", 907_000_000, 199.00),
    ("NKE", "Nike Inc.", "NYSE", 1_520_000_000, 94.00),
    ("CAT", "Caterpillar Inc.", "NYSE", 509_000_000, 333.00),
    ("AMGN", "Amgen Inc.", "NASDAQ", 536_000_000, 302.00),
    ("PFE", "Pfizer Inc.", "NYSE", 5_610_000_000, 28.00),
    ("LOW", "Lowe's Companies Inc.", "NYSE", 565_000_000, 238.00),
    ("SPGI", "S&P Global Inc.", "NYSE", 315_000_000, 432.00),
    ("IBM", "IBM Corp.", "NYSE", 911_000_000, 184.00),
    ("GE", "GE Aerospace", "NYSE", 1_090_000_000, 167.00),
    ("NOW", "ServiceNow Inc.", "NYSE", 204_000_000, 760.00),
    ("INTU", "Intuit Inc.", "NASDAQ", 280_000_000, 643.00),
    ("BKNG", "Booking Holdings Inc.", "NASDAQ", 34_000_000, 3510.00),
    ("GS", "Goldman Sachs Group Inc.", "NYSE", 321_000_000, 412.00),
    ("HON", "Honeywell International Inc.", "NASDAQ", 655_000_000, 202.00),
    ("AMAT", "Applied Materials Inc.", "NASDAQ", 824_000_000, 210.00),
    ("UBER", "Uber Technologies Inc.", "NYSE", 2_050_000_000, 77.00),
    ("PANW", "Palo Alto Networks Inc.", "NASDAQ", 324_000_000, 302.00),
    ("PLTR", "Palantir Technologies Inc.", "NASDAQ", 2_190_000_000, 25.00),
    ("DHR", "Danaher Corp.", "NYSE", 720_000_000, 247.00),
    ("RTX", "RTX Corp.", "NYSE", 1_330_000_000, 104.00),
    ("BLK", "BlackRock Inc.", "NYSE", 149_000_000, 790.00),
    ("SCHW", "Charles Schwab Corp.", "NYSE", 1_830_000_000, 74.00),
    ("ADP", "Automatic Data Processing Inc.", "NASDAQ", 412_000_000, 248.00),
    ("MDLZ", "Mondelez International Inc.", "NASDAQ", 1_340_000_000, 71.00),
)

FILER_UNIVERSE: tuple[str, ...] = (
    "Berkshire Hathaway Inc.",
    "Vanguard Group Inc.",
    "BlackRock Fund Advisors",
    "State Street Global Advisors",
    "Fidelity Management & Research",
    "Capital World Investors",
    "T. Rowe Price Associates",
    "Invesco Ltd.",
    "Northern Trust Investments",
    "Wellington Management Co.",
    "Geode Capital Management",
    "JPMorgan Investment Management",
    "Morgan Stanley Investment Management",
    "Goldman Sachs Asset Management",
    "BNY Mellon Investment Adviser",
    "Legal & General Investment Management",
    "UBS Asset Management",
    "Norges Bank Investment Management",
    "Amundi Asset Management",
    "Capital Research Global Investors",
    "Dimensional Fund Advisors",
    "Franklin Advisers",
    "AllianceBernstein LP",
    "AQR Capital Management",
    "Arrowstreet Capital LP",
    "Causeway Capital Management",
    "ClearBridge Investments",
    "Dodge & Cox",
    "MFS Investment Management",
    "PIMCO Advisors",
    "Principal Global Investors",
    "Nuveen Asset Management",
    "TIAA-CREF Investment Management",
    "Schroder Investment Management",
    "WCM Investment Management",
    "Voya Investment Management",
)

EXACT_CIKS = {
    "AAPL": "0000320193",
    "MSFT": "0000789019",
    "NVDA": "0001045810",
    "AMZN": "0001018724",
    "GOOGL": "0001652044",
    "META": "0001326801",
}

EXACT_CUSIPS = {
    "AAPL": "037833100",
    "MSFT": "594918104",
    "NVDA": "67066G104",
    "AMZN": "023135106",
    "GOOGL": "02079K305",
    "META": "30303M102",
}


def _hash(*parts: object) -> str:
    digest = md5(
        ":".join(str(part) for part in parts).encode("utf-8"),
        usedforsecurity=False,
    )
    return digest.hexdigest()


def _security_records() -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for index, (ticker, company_name, exchange, shares_outstanding, reference_price) in enumerate(
        SECURITY_UNIVERSE,
        start=1,
    ):
        records.append(
            {
                "ticker": ticker,
                "company_name": company_name,
                "exchange": exchange,
                "shares_outstanding": shares_outstanding,
                "reference_price": reference_price,
                "cik": EXACT_CIKS.get(ticker, f"{810_000_000 + index:010d}"),
                "cusip": EXACT_CUSIPS.get(ticker, f"{720_000_000 + index:09d}"),
            }
        )
    return records


def _filer_cik(index: int) -> str:
    if index == 1:
        return "0001067983"
    return f"{910_000_000 + index:010d}"


def _filer_accession_number(business_date: date, index: int) -> str:
    return f"0000950123-{business_date:%y}-{index:06d}"


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _holding_pct(filer_index: int, position_index: int, ticker: str) -> float:
    if ticker == "AAPL":
        if filer_index == 1:
            return 0.018000
        return max(0.006200 - filer_index * 0.000145, 0.000350)
    if ticker == "MSFT":
        return max(0.003700 - filer_index * 0.000075, 0.000240)
    if ticker == "NVDA":
        return max(0.003100 - filer_index * 0.000060, 0.000180)
    if ticker == "AMZN":
        return max(0.002600 - filer_index * 0.000050, 0.000150)
    return max(0.001900 - filer_index * 0.000035 - position_index * 0.000120, 0.000055)


def build_security_master_tickers(run_id: UUID, business_date: date) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, security in enumerate(_security_records(), start=1):
        rows.append(
            {
                "run_id": run_id,
                "business_date": business_date,
                "source_record_id": f"sec-ticker-{run_id}-{index}",
                "source_payload": {
                    "cik": security["cik"],
                    "ticker": security["ticker"],
                    "title": security["company_name"],
                },
                "source_payload_hash": _hash(run_id, "sec-ticker", security["ticker"]),
                "source_file_name": "company_tickers.json",
                "source_file_row_number": index,
                "cik": security["cik"],
                "ticker": security["ticker"],
                "company_name": security["company_name"],
                "exchange": security["exchange"],
                "row_hash": _hash(run_id, security["cik"], security["ticker"]),
            }
        )
    return rows


def build_security_master_facts(run_id: UUID, business_date: date) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, security in enumerate(_security_records(), start=1):
        rows.append(
            {
                "run_id": run_id,
                "business_date": business_date,
                "source_record_id": f"sec-fact-{run_id}-{index}",
                "source_payload": {
                    "fact": "shares_outstanding",
                    "value": security["shares_outstanding"],
                },
                "source_payload_hash": _hash(run_id, "sec-fact", security["ticker"]),
                "source_file_name": f"companyfacts_{security['cik']}.json",
                "source_file_row_number": 1,
                "cik": security["cik"],
                "fact_name": "shares_outstanding",
                "fact_value": security["shares_outstanding"],
                "unit": "shares",
                "row_hash": _hash(run_id, security["cik"], "shares_outstanding"),
            }
        )
    return rows


def build_shareholder_filers(run_id: UUID, business_date: date) -> list[dict[str, object]]:
    report_period = business_date - timedelta(days=15)
    rows: list[dict[str, object]] = []
    for index, filer_name in enumerate(FILER_UNIVERSE, start=1):
        filer_cik = _filer_cik(index)
        accession_number = _filer_accession_number(business_date, index)
        rows.append(
            {
                "run_id": run_id,
                "business_date": business_date,
                "source_record_id": f"13f-filer-{run_id}-{index}",
                "source_payload": {
                    "filer_cik": filer_cik,
                    "filer_name": filer_name,
                },
                "source_payload_hash": _hash(run_id, "13f-filer", filer_cik),
                "source_file_name": "13f_filers.csv",
                "source_file_row_number": index,
                "accession_number": accession_number,
                "filer_cik": filer_cik,
                "filer_name": filer_name,
                "report_period": report_period,
                "row_hash": _hash(run_id, "13f-filer", filer_cik, accession_number),
            }
        )
    return rows


def build_shareholder_holdings(run_id: UUID, business_date: date) -> list[dict[str, object]]:
    securities = _security_records()
    security_by_ticker = {str(security["ticker"]): security for security in securities}
    non_anchor_tickers = [
        str(security["ticker"])
        for security in securities
        if security["ticker"] != "AAPL"
    ]

    rows: list[dict[str, object]] = []
    source_row_number = 1

    for filer_index, _filer_name in enumerate(FILER_UNIVERSE, start=1):
        filer_cik = _filer_cik(filer_index)
        accession_number = _filer_accession_number(business_date, filer_index)
        candidate_tickers = [
            "AAPL",
            non_anchor_tickers[(filer_index * 2) % len(non_anchor_tickers)],
            non_anchor_tickers[(filer_index * 5 + 7) % len(non_anchor_tickers)],
        ]

        if filer_index % 2 == 0:
            candidate_tickers.append("MSFT")
        if filer_index % 3 == 0:
            candidate_tickers.append("NVDA")
        if filer_index % 4 == 0:
            candidate_tickers.append("AMZN")
        if filer_index % 5 == 0:
            candidate_tickers.append("META")

        for position_index, ticker in enumerate(_dedupe(candidate_tickers), start=1):
            security = security_by_ticker[ticker]
            shares_outstanding = cast(int, security["shares_outstanding"])
            reference_price = cast(float, security["reference_price"])
            holding_pct = _holding_pct(filer_index, position_index, ticker)
            shares_held = max(1, int(round(shares_outstanding * holding_pct)))
            price_multiplier = 0.96 + ((filer_index + position_index) % 5) * 0.0125
            market_value = max(1, int(round(shares_held * reference_price * price_multiplier)))

            rows.append(
                {
                    "run_id": run_id,
                    "business_date": business_date,
                    "source_record_id": f"13f-holding-{run_id}-{source_row_number}",
                    "source_payload": {
                        "ticker": ticker,
                        "cusip": security["cusip"],
                        "shares_held": shares_held,
                        "market_value": market_value,
                    },
                    "source_payload_hash": _hash(run_id, "13f-holding", filer_cik, ticker),
                    "source_file_name": "13f_holdings.csv",
                    "source_file_row_number": source_row_number,
                    "accession_number": accession_number,
                    "filer_cik": filer_cik,
                    "security_identifier": ticker,
                    "cusip": security["cusip"],
                    "shares_held": shares_held,
                    "market_value": market_value,
                    "row_hash": _hash(run_id, filer_cik, accession_number, ticker),
                }
            )
            source_row_number += 1

    return rows
