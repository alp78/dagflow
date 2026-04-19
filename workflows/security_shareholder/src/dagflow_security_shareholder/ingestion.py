from __future__ import annotations

import hashlib
import json
import re
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from edgar import get_company_tickers, get_filings, set_identity, use_local_storage

EDGAR_LOCAL_CACHE_DIR = Path(__file__).resolve().parents[4] / ".cache" / "edgar"
IDENTIFIER_CACHE_FILE = (
    Path(__file__).resolve().parents[4]
    / ".cache"
    / "identifiers"
    / "cusip_cache.json"
)
HISTORICAL_13F_CACHE_DIR = Path(__file__).resolve().parents[4] / ".cache" / "historical_13f"


def _serialize_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))


def _payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_serialize_payload(payload).encode("utf-8")).hexdigest()


def _row_hash(*parts: object) -> str:
    materialized = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(materialized.encode("utf-8")).hexdigest()


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    return text or None


def _clean_numeric(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_ticker(value: str | None) -> str | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    normalized = re.sub(r"[^A-Z0-9]", "", cleaned.upper())
    return normalized or None


def _normalize_company_name(value: str | None) -> str | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    normalized = re.sub(r"[^A-Z0-9]+", " ", cleaned.upper())
    normalized = re.sub(
        r"\b(THE|INCORPORATED|INC|CORPORATION|CORP|COMPANY|CO|LIMITED|LTD|HOLDINGS|HLDGS|GROUP|PLC|LLC|LP|SA|NV|AG|SE)\b",
        " ",
        normalized,
    )
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def _normalize_cik(value: Any) -> str:
    cleaned = _clean_string(value) or "0"
    digits = "".join(character for character in cleaned if character.isdigit())
    return digits.zfill(10)


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    try:
        return date.fromisoformat(cleaned)
    except ValueError:
        return None


def _year_quarter(value: date) -> tuple[int, int]:
    return value.year, ((value.month - 1) // 3) + 1


def _quarter_range(start_date: date, end_date: date) -> list[tuple[int, int]]:
    current_year, current_quarter = _year_quarter(start_date)
    end_year, end_quarter = _year_quarter(end_date)
    quarters: list[tuple[int, int]] = []
    while (current_year, current_quarter) <= (end_year, end_quarter):
        quarters.append((current_year, current_quarter))
        if current_quarter == 4:
            current_year += 1
            current_quarter = 1
        else:
            current_quarter += 1
    return quarters


SHARES_OUTSTANDING_CONCEPTS: tuple[tuple[str, str], ...] = (
    ("dei", "EntityCommonStockSharesOutstanding"),
    ("us-gaap", "CommonStockSharesOutstanding"),
)

PREFERRED_TICKER_BY_CIK: dict[str, str] = {
    "0001652044": "GOOGL",
}


def _canonical_ticker_for_cik(cik: str, ticker: str) -> str:
    return PREFERRED_TICKER_BY_CIK.get(cik, ticker)


@dataclass(slots=True)
class ResolvedTicker:
    ticker: str
    source: str


@dataclass(slots=True)
class Recent13FSnapshot:
    filer_records: list[dict[str, Any]]
    holding_records: list[dict[str, Any]]
    focus_tickers: list[str]
    ticker_holder_counts: dict[str, int]
    filings_scanned: int
    filings_loaded: int
    skipped_filings: list[dict[str, Any]]


@dataclass(slots=True)
class Historical13FFiling:
    accession_number: str
    filing_date: date
    filer_cik: str
    filer_name: str
    report_period: date | None
    holdings: list[dict[str, Any]]


def _serialize_historical_filing(filing: Historical13FFiling) -> dict[str, Any]:
    return {
        "accession_number": filing.accession_number,
        "filing_date": filing.filing_date.isoformat(),
        "filer_cik": filing.filer_cik,
        "filer_name": filing.filer_name,
        "report_period": filing.report_period.isoformat() if filing.report_period else None,
        "holdings": filing.holdings,
    }


def _deserialize_historical_filing(payload: dict[str, Any]) -> Historical13FFiling | None:
    accession_number = _clean_string(payload.get("accession_number"))
    filing_date = _coerce_date(payload.get("filing_date"))
    filer_cik = _clean_string(payload.get("filer_cik"))
    filer_name = _clean_string(payload.get("filer_name"))
    holdings = payload.get("holdings")
    if not accession_number or filing_date is None or not filer_cik or not filer_name:
        return None
    if not isinstance(holdings, list):
        return None
    return Historical13FFiling(
        accession_number=accession_number,
        filing_date=filing_date,
        filer_cik=filer_cik,
        filer_name=filer_name,
        report_period=_coerce_date(payload.get("report_period")),
        holdings=holdings,
    )


@dataclass(slots=True)
class IdentifierResolver:
    sec_ticker_aliases: dict[str, str]
    sec_exact_tickers: set[str] = field(default_factory=set)
    sec_company_aliases: dict[str, str] = field(default_factory=dict)
    finnhub_api_key: str | None = None
    openfigi_api_key: str | None = None
    client: httpx.Client = field(default_factory=lambda: httpx.Client(timeout=30.0))
    cache_file: Path | None = None
    _cache: dict[str, ResolvedTicker | None] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.sec_exact_tickers:
            self.sec_exact_tickers = set(self.sec_ticker_aliases.values())
        if self.cache_file and self.cache_file.exists():
            try:
                payload = json.loads(self.cache_file.read_text())
            except (OSError, ValueError, TypeError):
                payload = {}
            if isinstance(payload, dict):
                for cusip, item in payload.items():
                    if not isinstance(cusip, str):
                        continue
                    if not item:
                        self._cache[cusip] = None
                        continue
                    if not isinstance(item, dict):
                        continue
                    ticker = self.canonicalize(item.get("ticker"))
                    source = _clean_string(item.get("source"))
                    self._cache[cusip] = (
                        ResolvedTicker(ticker=ticker, source=source or "cache")
                        if ticker
                        else None
                    )

    def canonicalize(self, ticker: str | None) -> str | None:
        cleaned = _clean_string(ticker)
        if not cleaned:
            return None
        if cleaned in self.sec_exact_tickers:
            return cleaned
        return self.sec_ticker_aliases.get(_normalize_ticker(cleaned) or "")

    def resolve_from_issuer_name(self, issuer_name: str | None) -> ResolvedTicker | None:
        normalized = _normalize_company_name(issuer_name)
        if not normalized:
            return None
        ticker = self.sec_company_aliases.get(normalized)
        if not ticker:
            return None
        return ResolvedTicker(ticker=ticker, source="sec-company-name")

    def resolve_from_cusip(self, cusip: str | None) -> ResolvedTicker | None:
        cleaned_cusip = _clean_string(cusip)
        if not cleaned_cusip:
            return None
        return self.resolve_many_from_cusips([cleaned_cusip]).get(cleaned_cusip)

    def resolve_many_from_cusips(self, cusips: list[str]) -> dict[str, ResolvedTicker | None]:
        cleaned_cusips = [_clean_string(cusip) for cusip in cusips]
        normalized_cusips = [cusip for cusip in cleaned_cusips if cusip]
        pending = [cusip for cusip in normalized_cusips if cusip not in self._cache]
        if pending:
            openfigi_results = self._resolve_openfigi_batch(pending)
            for cusip in pending:
                resolved = openfigi_results.get(cusip)
                if resolved is None:
                    resolved = self._resolve_finnhub(cusip)
                self._cache[cusip] = resolved
            self._persist_cache()
        return {cusip: self._cache.get(cusip) for cusip in normalized_cusips}

    def _persist_cache(self) -> None:
        if not self.cache_file:
            return
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                cusip: (
                    {"ticker": resolved.ticker, "source": resolved.source}
                    if resolved is not None
                    else None
                )
                for cusip, resolved in self._cache.items()
            }
            self.cache_file.write_text(json.dumps(payload, sort_keys=True))
        except OSError:
            return

    def _resolve_openfigi(self, cusip: str) -> ResolvedTicker | None:
        return self._resolve_openfigi_batch([cusip]).get(cusip)

    def _resolve_openfigi_batch(self, cusips: list[str]) -> dict[str, ResolvedTicker | None]:
        if not cusips:
            return {}

        headers = {"Content-Type": "application/json"}
        if self.openfigi_api_key:
            headers["X-OPENFIGI-APIKEY"] = self.openfigi_api_key
        resolved: dict[str, ResolvedTicker | None] = {}
        for start in range(0, len(cusips), 50):
            batch = cusips[start : start + 50]
            try:
                response = self.client.post(
                    "https://api.openfigi.com/v3/mapping",
                    headers=headers,
                    json=[{"idType": "ID_CUSIP", "idValue": cusip} for cusip in batch],
                    timeout=10.0,
                )
                response.raise_for_status()
            except httpx.HTTPError:
                for cusip in batch:
                    resolved[cusip] = None
                continue

            payload = response.json()
            if not isinstance(payload, list):
                for cusip in batch:
                    resolved[cusip] = None
                continue

            for cusip, item in zip(batch, payload, strict=False):
                candidates = item.get("data", []) if isinstance(item, dict) else []
                ranked_candidates: list[tuple[int, str]] = []
                for candidate in candidates:
                    if not isinstance(candidate, dict):
                        continue
                    ticker = self.canonicalize(candidate.get("ticker"))
                    if not ticker:
                        continue
                    score = 0
                    if candidate.get("exchCode") == "US":
                        score += 5
                    if candidate.get("marketSector") == "Equity":
                        score += 3
                    if candidate.get("securityType") in {"Common Stock", "ETF", "ETP"}:
                        score += 2
                    ranked_candidates.append((score, ticker))
                if not ranked_candidates:
                    resolved[cusip] = None
                    continue
                ranked_candidates.sort(key=lambda item: (-item[0], item[1]))
                resolved[cusip] = ResolvedTicker(
                    ticker=ranked_candidates[0][1],
                    source="openfigi",
                )

        return resolved

    def _resolve_finnhub(self, cusip: str) -> ResolvedTicker | None:
        if not self.finnhub_api_key:
            return None
        try:
            response = self.client.get(
                "https://finnhub.io/api/v1/search",
                params={"q": cusip, "token": self.finnhub_api_key},
                timeout=5.0,
            )
            response.raise_for_status()
        except httpx.HTTPError:
            return None

        payload = response.json()
        results = payload.get("result", []) if isinstance(payload, dict) else []
        for result in results:
            if not isinstance(result, dict):
                continue
            ticker = self.canonicalize(result.get("symbol") or result.get("displaySymbol"))
            if ticker:
                return ResolvedTicker(ticker=ticker, source="finnhub")
        return None


@dataclass(slots=True)
class EdgarIngestionService:
    edgar_identity: str
    sec_13f_lookback_days: int
    sec_13f_filing_limit: int
    sec_security_focus_limit: int
    finnhub_api_key: str | None = None
    openfigi_api_key: str | None = None
    client: httpx.Client = field(default_factory=lambda: httpx.Client(timeout=30.0))
    sec_max_requests_per_second: float = 8.0
    _company_ticker_rows: list[dict[str, Any]] | None = None
    _ticker_aliases: dict[str, str] = field(default_factory=dict)
    _company_name_aliases: dict[str, str] = field(default_factory=dict)
    _security_ticker_cache: dict[date, list[dict[str, Any]]] = field(default_factory=dict)
    _security_fact_cache: dict[date, list[dict[str, Any]]] = field(default_factory=dict)
    _recent_13f_cache: dict[date, Recent13FSnapshot] = field(default_factory=dict)
    _historical_13f_cache: dict[tuple[date, date, int, int | None], list[Historical13FFiling]] = (
        field(default_factory=dict)
    )
    _shares_outstanding_history_cache: dict[str, list[tuple[date, float]]] = field(
        default_factory=dict
    )
    _last_sec_request_at: float = 0.0

    def build_security_master_tickers(self, business_date: date) -> list[dict[str, Any]]:
        if business_date in self._security_ticker_cache:
            return self._security_ticker_cache[business_date]

        snapshot = self.recent_13f_snapshot(business_date)
        records = self.build_security_master_tickers_from_snapshot(snapshot)
        self._security_ticker_cache[business_date] = records
        return records

    def build_security_master_tickers_from_snapshot(
        self, snapshot: Recent13FSnapshot
    ) -> list[dict[str, Any]]:
        focus_tickers = snapshot.focus_tickers
        holder_counts = snapshot.ticker_holder_counts
        ranking = {ticker: index for index, ticker in enumerate(focus_tickers)}
        reported_shares_by_ticker: Counter[str] = Counter()
        top_holder_shares_by_ticker: dict[str, float] = {}
        for holding in snapshot.holding_records:
            ticker = str(holding["security_identifier"])
            shares_held = float(holding["shares_held"] or 0.0)
            reported_shares_by_ticker[ticker] += shares_held
            top_holder_shares_by_ticker[ticker] = max(
                top_holder_shares_by_ticker.get(ticker, 0.0),
                shares_held,
            )
        rows = [
            row
            for row in self.company_ticker_rows()
            if row["ticker"] in ranking
        ]
        rows.sort(
            key=lambda row: (
                ranking.get(row["ticker"], self.sec_security_focus_limit),
                row["ticker"],
            )
        )

        records: list[dict[str, Any]] = []
        for index, row in enumerate(rows, start=1):
            payload = {
                "cik": row["cik"],
                "ticker": row["ticker"],
                "company_name": row["company_name"],
                "exchange": row["exchange"],
                "holder_count_recent_window": holder_counts.get(row["ticker"], 0),
                "reported_shares_recent_window": round(
                    reported_shares_by_ticker.get(row["ticker"], 0.0),
                    6,
                ),
                "top_holder_shares_recent_window": round(
                    top_holder_shares_by_ticker.get(row["ticker"], 0.0),
                    6,
                ),
            }
            records.append(
                {
                    "source_record_id": row["ticker"],
                    "source_payload": payload,
                    "source_payload_hash": _payload_hash(payload),
                    "source_file_name": "company_tickers.json",
                    "source_file_row_number": index,
                    "cik": row["cik"],
                    "ticker": row["ticker"],
                    "company_name": row["company_name"],
                    "exchange": row["exchange"],
                    "row_hash": _row_hash(
                        row["cik"],
                        row["ticker"],
                        row["company_name"],
                        row["exchange"],
                    ),
                }
            )

        return records

    def build_security_master_facts(self, business_date: date) -> list[dict[str, Any]]:
        if business_date in self._security_fact_cache:
            return self._security_fact_cache[business_date]

        ticker_rows = self.build_security_master_tickers(business_date)
        records = self.build_security_master_facts_from_ticker_rows(business_date, ticker_rows)
        self._security_fact_cache[business_date] = records
        return records

    def build_security_master_facts_from_ticker_rows(
        self, business_date: date, ticker_rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        companies_by_cik: dict[str, dict[str, Any]] = {}
        for row in ticker_rows:
            companies_by_cik.setdefault(row["cik"], row)

        records: list[dict[str, Any]] = []
        for index, row in enumerate(companies_by_cik.values(), start=1):
            shares_outstanding: float | None = None
            retrieval_status = "ok"
            try:
                shares_outstanding = self._shares_outstanding_as_of(row["cik"], business_date)
                if shares_outstanding is None:
                    retrieval_status = "missing"
            except Exception as error:
                retrieval_status = f"error:{error.__class__.__name__}"

            payload = {
                "cik": row["cik"],
                "ticker": row["ticker"],
                "fact_name": "shares_outstanding",
                "fact_value": shares_outstanding,
                "unit": "shares" if shares_outstanding is not None else None,
                "retrieval_status": retrieval_status,
            }
            records.append(
                {
                    "source_record_id": f"{row['cik']}:shares_outstanding",
                    "source_payload": payload,
                    "source_payload_hash": _payload_hash(payload),
                    "source_file_name": f"CIK{row['cik']}.json",
                    "source_file_row_number": index,
                    "cik": row["cik"],
                    "fact_name": "shares_outstanding",
                    "fact_value": shares_outstanding,
                    "unit": "shares",
                    "row_hash": _row_hash(row["cik"], "shares_outstanding", shares_outstanding),
                }
            )

        return records

    def build_shareholder_filers(self, business_date: date) -> list[dict[str, Any]]:
        return self.recent_13f_snapshot(business_date).filer_records

    def build_shareholder_holdings(self, business_date: date) -> list[dict[str, Any]]:
        return self.recent_13f_snapshot(business_date).holding_records

    def build_shareholder_filers_from_holding_rows(
        self, holding_rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for index, row in enumerate(holding_rows, start=1):
            accession_number = _clean_string(row.get("accession_number"))
            filer_cik = _clean_string(row.get("filer_cik"))
            payload = row.get("source_payload")
            payload_dict = payload if isinstance(payload, dict) else {}
            filer_name = _clean_string(payload_dict.get("filer_name")) or _clean_string(
                row.get("filer_name")
            )
            report_period = _coerce_date(payload_dict.get("report_period")) or _coerce_date(
                row.get("report_period")
            )
            if not accession_number or not filer_cik or not filer_name:
                continue

            key = (accession_number, filer_cik)
            grouped_record = grouped.get(key)
            if grouped_record is None:
                filer_payload = {
                    "accession_number": accession_number,
                    "filer_cik": filer_cik,
                    "filer_name": filer_name,
                    "report_period": report_period.isoformat() if report_period else None,
                    "filing_date": payload_dict.get("filing_date"),
                    "focus_holdings_count": 1,
                }
                grouped[key] = {
                    "source_record_id": accession_number,
                    "source_payload": filer_payload,
                    "source_payload_hash": _payload_hash(filer_payload),
                    "source_file_name": accession_number,
                    "source_file_row_number": index,
                    "accession_number": accession_number,
                    "filer_cik": filer_cik,
                    "filer_name": filer_name,
                    "report_period": report_period,
                    "row_hash": _row_hash(
                        accession_number,
                        filer_cik,
                        filer_name,
                        report_period,
                    ),
                }
                continue

            grouped_payload = grouped_record["source_payload"]
            grouped_payload["focus_holdings_count"] = int(
                grouped_payload.get("focus_holdings_count", 0)
            ) + 1
            grouped_record["source_payload_hash"] = _payload_hash(grouped_payload)

        return list(grouped.values())

    def company_ticker_rows(self) -> list[dict[str, Any]]:
        if self._company_ticker_rows is not None:
            return self._company_ticker_rows

        self._ensure_identity()
        tickers = get_company_tickers().copy()
        rows_by_cik: dict[str, list[dict[str, Any]]] = {}
        aliases: dict[str, str] = {}
        company_aliases: dict[str, str] = {}
        ambiguous_company_aliases: set[str] = set()
        for _, row in tickers.iterrows():
            ticker = _clean_string(row.get("ticker"))
            company_name = _clean_string(row.get("company"))
            if not ticker or not company_name:
                continue
            cik = _normalize_cik(row.get("cik"))
            exchange = _clean_string(row.get("exchange"))
            ticker_row = {
                "cik": cik,
                "ticker": ticker,
                "company_name": company_name,
                "exchange": exchange,
            }
            rows_by_cik.setdefault(cik, []).append(ticker_row)
            canonical_ticker = _canonical_ticker_for_cik(cik, ticker)
            normalized = _normalize_ticker(ticker)
            if normalized:
                aliases[normalized] = canonical_ticker
            normalized_company = _normalize_company_name(company_name)
            if normalized_company:
                existing = company_aliases.get(normalized_company)
                if existing and existing != canonical_ticker:
                    ambiguous_company_aliases.add(normalized_company)
                    company_aliases.pop(normalized_company, None)
                elif normalized_company not in ambiguous_company_aliases:
                    company_aliases[normalized_company] = canonical_ticker
        rows: list[dict[str, Any]] = []
        for cik, cik_rows in rows_by_cik.items():
            preferred_ticker = PREFERRED_TICKER_BY_CIK.get(cik)
            if not preferred_ticker:
                rows.extend(cik_rows)
                continue
            preferred_row = next(
                (ticker_row for ticker_row in cik_rows if ticker_row["ticker"] == preferred_ticker),
                None,
            )
            if preferred_row is not None:
                rows.append(preferred_row)
                continue
            rows.extend(cik_rows)
        self._company_ticker_rows = rows
        self._ticker_aliases = aliases
        self._company_name_aliases = company_aliases
        return rows

    def historical_13f_filings(
        self,
        start_date: date,
        end_date: date,
        *,
        warmup_days: int = 180,
        filing_limit: int | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> list[Historical13FFiling]:
        cache_key = (start_date, end_date, warmup_days, filing_limit)
        if cache_key in self._historical_13f_cache:
            return self._historical_13f_cache[cache_key]

        self.company_ticker_rows()
        resolver = IdentifierResolver(
            sec_ticker_aliases=self._ticker_aliases,
            sec_company_aliases=self._company_name_aliases,
            finnhub_api_key=self.finnhub_api_key,
            openfigi_api_key=self.openfigi_api_key,
            client=self.client,
            cache_file=IDENTIFIER_CACHE_FILE,
        )
        self._ensure_identity()

        filing_start = start_date - timedelta(days=warmup_days)
        parsed_filings: list[Historical13FFiling] = []
        max_filings_per_quarter = filing_limit or max(self.sec_13f_filing_limit * 4, 400)
        scanned = 0

        if progress_callback:
            progress_callback(
                "Scanning historical 13F filings by quarter "
                f"from {filing_start.isoformat()} to {end_date.isoformat()} "
                f"(up to {max_filings_per_quarter} filings per quarter)"
            )

        for year, quarter in _quarter_range(filing_start, end_date):
            quarter_scanned = 0
            quarter_cache_path = (
                HISTORICAL_13F_CACHE_DIR
                / f"{year}Q{quarter}_limit{max_filings_per_quarter}.json"
            )
            if quarter_cache_path.exists():
                try:
                    payload = json.loads(quarter_cache_path.read_text())
                except (OSError, ValueError, TypeError):
                    payload = []
                if isinstance(payload, list):
                    cached_filings = [
                        filing
                        for item in payload
                        if isinstance(item, dict)
                        and (filing := _deserialize_historical_filing(item)) is not None
                    ]
                    if progress_callback:
                        progress_callback(
                            "Using cached "
                            f"{year} Q{quarter} 13F filings "
                            f"({len(cached_filings)} parsed)"
                        )
                    parsed_filings.extend(
                        filing
                        for filing in cached_filings
                        if filing_start <= filing.filing_date <= end_date
                    )
                    scanned += len(cached_filings)
                    continue
            quarter_filings = get_filings(
                year=year,
                quarter=quarter,
                form="13F-HR",
                amendments=False,
            )
            if quarter_filings is None:
                continue
            relevant_filings = quarter_filings.filter(
                date=f"{filing_start.isoformat()}:{end_date.isoformat()}"
            ).head(max_filings_per_quarter)
            if len(relevant_filings) == 0:
                continue
            if progress_callback:
                progress_callback(f"Loading {year} Q{quarter} 13F filings")
            quarter_parsed_filings: list[Historical13FFiling] = []
            for filing in relevant_filings:
                filing_date = _coerce_date(getattr(filing, "filing_date", None))
                if filing_date is None:
                    continue
                quarter_scanned += 1
                scanned += 1
                if progress_callback and (scanned == 1 or scanned % 25 == 0):
                    progress_callback(
                        "Scanned "
                        f"{scanned} filings and parsed {len(parsed_filings)} usable 13F reports"
                    )
                try:
                    parsed = self._parse_13f_filing(filing, resolver)
                except Exception:
                    continue
                if parsed is None:
                    continue
                quarter_parsed_filings.append(
                    Historical13FFiling(
                        accession_number=parsed["accession_number"],
                        filing_date=_coerce_date(parsed["filing_date"]) or filing_start,
                        filer_cik=parsed["filer_cik"],
                        filer_name=parsed["filer_name"],
                        report_period=parsed["report_period"],
                        holdings=parsed["holdings"],
                    )
                )
            parsed_filings.extend(quarter_parsed_filings)
            try:
                HISTORICAL_13F_CACHE_DIR.mkdir(parents=True, exist_ok=True)
                quarter_cache_path.write_text(
                    json.dumps(
                        [_serialize_historical_filing(filing) for filing in quarter_parsed_filings],
                        sort_keys=True,
                    )
                )
            except OSError:
                pass

        parsed_filings.sort(
            key=lambda filing: (filing.filing_date, filing.accession_number, filing.filer_cik)
        )
        if progress_callback:
            progress_callback(
                "Historical preload complete: parsed "
                f"{len(parsed_filings)} usable filings out of {scanned} scanned"
            )
        self._historical_13f_cache[cache_key] = parsed_filings
        return parsed_filings

    def historical_13f_snapshot(
        self,
        business_date: date,
        filings: list[Historical13FFiling],
        *,
        stale_after_days: int = 180,
    ) -> Recent13FSnapshot:
        selected_by_filer: dict[str, dict[str, Any]] = {}
        stale_threshold = business_date - timedelta(days=stale_after_days)

        for filing in filings:
            if filing.filing_date > business_date:
                break
            if filing.filing_date < stale_threshold:
                continue
            existing = selected_by_filer.get(filing.filer_cik)
            if existing is not None and existing["_filing_date"] >= filing.filing_date:
                continue
            selected_by_filer[filing.filer_cik] = {
                "_filing_date": filing.filing_date,
                "accession_number": filing.accession_number,
                "filer_cik": filing.filer_cik,
                "filer_name": filing.filer_name,
                "report_period": filing.report_period,
                "filing_date": filing.filing_date.isoformat(),
                "holdings": filing.holdings,
            }

        selected_by_accession = {
            record["accession_number"]: {
                key: value for key, value in record.items() if key != "_filing_date"
            }
            for record in selected_by_filer.values()
        }

        return self._materialize_snapshot_from_accessions(
            selected_by_accession,
            filings_scanned=len(filings),
            filings_loaded=len(selected_by_accession),
            skipped_filings=[],
        )

    def recent_13f_snapshot(self, business_date: date) -> Recent13FSnapshot:
        if business_date in self._recent_13f_cache:
            return self._recent_13f_cache[business_date]

        self.company_ticker_rows()
        resolver = IdentifierResolver(
            sec_ticker_aliases=self._ticker_aliases,
            sec_company_aliases=self._company_name_aliases,
            finnhub_api_key=self.finnhub_api_key,
            openfigi_api_key=self.openfigi_api_key,
            client=self.client,
            cache_file=IDENTIFIER_CACHE_FILE,
        )
        self._ensure_identity()

        filing_end = business_date - timedelta(days=1)
        filing_start = filing_end - timedelta(days=self.sec_13f_lookback_days)
        filing_range = f"{filing_start.isoformat()}:{filing_end.isoformat()}"
        filings = get_filings(form="13F-HR", amendments=False, filing_date=filing_range)

        by_accession: dict[str, dict[str, Any]] = {}
        skipped_filings: list[dict[str, Any]] = []
        filings_scanned = 0

        if filings is not None:
            for filing in filings:
                if filings_scanned >= self.sec_13f_filing_limit:
                    break
                filings_scanned += 1

                accession_number = _clean_string(getattr(filing, "accession_no", None))
                try:
                    parsed = self._parse_13f_filing(filing, resolver)
                except Exception as error:
                    if accession_number:
                        skipped_filings.append(
                            {
                                "accession_number": accession_number,
                                "error_class": error.__class__.__name__,
                                "error_message": str(error),
                            }
                        )
                    continue
                if parsed is None:
                    if accession_number:
                        skipped_filings.append(
                            {
                                "accession_number": accession_number,
                                "error_class": "EmptyOrUnsupported",
                                "error_message": "Filing produced no supported holdings.",
                            }
                        )
                    continue

                by_accession[parsed["accession_number"]] = parsed

        snapshot = self._materialize_snapshot_from_accessions(
            by_accession,
            filings_scanned=filings_scanned,
            filings_loaded=len(by_accession),
            skipped_filings=skipped_filings,
        )
        self._recent_13f_cache[business_date] = snapshot
        return snapshot

    def _materialize_snapshot_from_accessions(
        self,
        by_accession: dict[str, dict[str, Any]],
        *,
        filings_scanned: int,
        filings_loaded: int,
        skipped_filings: list[dict[str, Any]],
    ) -> Recent13FSnapshot:
        holder_counts: Counter[str] = Counter()
        reported_shares_by_ticker: Counter[str] = Counter()
        top_holder_shares_by_ticker: dict[str, float] = {}
        for record in by_accession.values():
            holder_counts.update(holding["ticker"] for holding in record["holdings"])
            for holding in record["holdings"]:
                ticker = holding["ticker"]
                shares_held = float(holding["shares_held"] or 0.0)
                reported_shares_by_ticker[ticker] += shares_held
                top_holder_shares_by_ticker[ticker] = max(
                    top_holder_shares_by_ticker.get(ticker, 0.0),
                    shares_held,
                )

        focus_tickers = [
            ticker for ticker, _ in holder_counts.most_common(self.sec_security_focus_limit)
        ]
        focus_ticker_set = set(focus_tickers)

        filer_records: list[dict[str, Any]] = []
        holding_records: list[dict[str, Any]] = []
        for accession_number, record in by_accession.items():
            filtered_holdings = [
                holding
                for holding in record["holdings"]
                if holding["ticker"] in focus_ticker_set
            ]
            if not filtered_holdings:
                continue

            filer_payload = {
                "accession_number": accession_number,
                "filer_cik": record["filer_cik"],
                "filer_name": record["filer_name"],
                "report_period": record["report_period"].isoformat()
                if record["report_period"]
                else None,
                "filing_date": record["filing_date"],
                "focus_holdings_count": len(filtered_holdings),
            }
            filer_records.append(
                {
                    "source_record_id": accession_number,
                    "source_payload": filer_payload,
                    "source_payload_hash": _payload_hash(filer_payload),
                    "source_file_name": accession_number,
                    "source_file_row_number": 1,
                    "accession_number": accession_number,
                    "filer_cik": record["filer_cik"],
                    "filer_name": record["filer_name"],
                    "report_period": record["report_period"],
                    "row_hash": _row_hash(
                        accession_number,
                        record["filer_cik"],
                        record["filer_name"],
                        record["report_period"],
                    ),
                }
            )

            for index, holding in enumerate(
                sorted(
                    filtered_holdings,
                    key=lambda item: (-holder_counts[item["ticker"]], item["ticker"]),
                ),
                start=1,
            ):
                holding_payload = {
                    "accession_number": accession_number,
                    "filer_cik": record["filer_cik"],
                    "filer_name": record["filer_name"],
                    "report_period": record["report_period"].isoformat()
                    if record["report_period"]
                    else None,
                    "issuer_name": holding["issuer_name"],
                    "resolved_ticker": holding["ticker"],
                    "ticker_source": holding["ticker_source"] or "edgartools",
                    "ticker_sources": holding["sources"],
                    "cusip": holding["cusip"],
                    "shares_held": round(holding["shares_held"], 6),
                    "market_value_usd": round(holding["market_value"], 6),
                    "holder_count_recent_window": holder_counts[holding["ticker"]],
                    "reported_shares_recent_window": round(
                        reported_shares_by_ticker[holding["ticker"]],
                        6,
                    ),
                    "top_holder_shares_recent_window": round(
                        top_holder_shares_by_ticker[holding["ticker"]],
                        6,
                    ),
                    "aggregated_rows": holding["row_count"],
                }
                holding_records.append(
                    {
                        "source_record_id": f"{accession_number}:{holding['ticker']}",
                        "source_payload": holding_payload,
                        "source_payload_hash": _payload_hash(holding_payload),
                        "source_file_name": accession_number,
                        "source_file_row_number": index,
                        "accession_number": accession_number,
                        "filer_cik": record["filer_cik"],
                        "security_identifier": holding["ticker"],
                        "cusip": holding["cusip"],
                        "shares_held": round(holding["shares_held"], 6),
                        "market_value": round(holding["market_value"], 6),
                        "row_hash": _row_hash(
                            accession_number,
                            record["filer_cik"],
                            holding["ticker"],
                            holding["cusip"],
                            round(holding["shares_held"], 6),
                            round(holding["market_value"], 6),
                        ),
                    }
                )

        return Recent13FSnapshot(
            filer_records=filer_records,
            holding_records=holding_records,
            focus_tickers=focus_tickers,
            ticker_holder_counts=dict(holder_counts),
            filings_scanned=filings_scanned,
            filings_loaded=filings_loaded,
            skipped_filings=skipped_filings,
        )

    def _parse_13f_filing(
        self, filing: Any, resolver: IdentifierResolver
    ) -> dict[str, Any] | None:
        accession_number = _clean_string(getattr(filing, "accession_no", None))
        if not accession_number:
            return None

        report = filing.obj()
        holdings = getattr(report, "holdings", None)
        if holdings is None or holdings.empty:
            return None

        filer_name = _clean_string(getattr(filing, "company", None)) or "Unknown filer"
        filer_cik = _normalize_cik(getattr(filing, "cik", None))
        report_period = _coerce_date(getattr(report, "report_period", None))

        prepared_rows: list[dict[str, Any]] = []
        unresolved_cusips: set[str] = set()
        for _, row in holdings.iterrows():
            issuer_name = _clean_string(row.get("Issuer"))
            source_ticker = _clean_string(row.get("Ticker"))
            resolved_ticker = resolver.canonicalize(source_ticker)
            ticker_source = "edgartools" if resolved_ticker else ""
            if not resolved_ticker and issuer_name:
                resolved_from_name = resolver.resolve_from_issuer_name(issuer_name)
                if resolved_from_name is not None:
                    resolved_ticker = resolved_from_name.ticker
                    ticker_source = resolved_from_name.source
            cusip = _clean_string(row.get("Cusip"))
            if not resolved_ticker and not source_ticker and cusip:
                unresolved_cusips.add(cusip)
            prepared_rows.append(
                {
                    "row": row,
                    "issuer_name": issuer_name,
                    "source_ticker": source_ticker,
                    "resolved_ticker": resolved_ticker,
                    "ticker_source": ticker_source,
                    "cusip": cusip,
                }
            )

        resolved_cusips = resolver.resolve_many_from_cusips(list(unresolved_cusips))

        aggregated_holdings: dict[str, dict[str, Any]] = {}
        for prepared in prepared_rows:
            row = prepared["row"]
            cusip = prepared["cusip"]
            source_ticker = prepared["source_ticker"]
            resolved_ticker = prepared["resolved_ticker"]
            ticker_source = prepared["ticker_source"]

            if not resolved_ticker and not source_ticker and cusip:
                resolved = resolved_cusips.get(cusip)
                if resolved is not None:
                    resolved_ticker = resolved.ticker
                    ticker_source = resolved.source

            if not resolved_ticker:
                continue

            shares_held = _clean_numeric(row.get("SharesPrnAmount"))
            market_value_thousands = _clean_numeric(row.get("Value"))
            if shares_held is None or market_value_thousands is None:
                continue

            market_value = round(market_value_thousands * 1000.0, 6)
            issuer_name = prepared["issuer_name"]
            existing = aggregated_holdings.get(resolved_ticker)
            if existing is None:
                aggregated_holdings[resolved_ticker] = {
                    "ticker": resolved_ticker,
                    "cusip": cusip,
                    "issuer_name": issuer_name,
                    "shares_held": shares_held,
                    "market_value": market_value,
                    "ticker_source": ticker_source,
                    "sources": [ticker_source] if ticker_source else [],
                    "row_count": 1,
                }
                continue

            existing["shares_held"] += shares_held
            existing["market_value"] += market_value
            existing["row_count"] += 1
            if cusip and not existing.get("cusip"):
                existing["cusip"] = cusip
            if issuer_name and not existing.get("issuer_name"):
                existing["issuer_name"] = issuer_name
            if ticker_source and ticker_source not in existing["sources"]:
                existing["sources"].append(ticker_source)

        if not aggregated_holdings:
            return None

        return {
            "accession_number": accession_number,
            "filer_cik": filer_cik,
            "filer_name": filer_name,
            "report_period": report_period,
            "filing_date": str(getattr(filing, "filing_date", "")),
            "holdings": list(aggregated_holdings.values()),
        }

    def _shares_outstanding_as_of(self, cik: str, business_date: date) -> float | None:
        history = self._shares_outstanding_series(cik)
        candidate_value: float | None = None
        for fact_date, fact_value in history:
            if fact_date <= business_date:
                candidate_value = fact_value
            else:
                break
        return candidate_value

    def _shares_outstanding_series(self, cik: str) -> list[tuple[date, float]]:
        if cik in self._shares_outstanding_history_cache:
            return self._shares_outstanding_history_cache[cik]

        try:
            self._pace_sec_request()
            response = self.client.get(
                f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
                headers={"User-Agent": self.edgar_identity},
            )
            if response.status_code == 404:
                self._shares_outstanding_history_cache[cik] = []
                return []
            response.raise_for_status()
        except httpx.HTTPError:
            self._shares_outstanding_history_cache[cik] = []
            return []

        try:
            payload = response.json()
        except ValueError:
            self._shares_outstanding_history_cache[cik] = []
            return []

        latest_by_date: dict[date, float] = {}
        facts_payload = payload.get("facts", {})
        for taxonomy, concept_name in SHARES_OUTSTANDING_CONCEPTS:
            units = (
                facts_payload
                .get(taxonomy, {})
                .get(concept_name, {})
                .get("units", {})
            )
            for fact in units.get("shares", []):
                fact_date = (
                    _coerce_date(fact.get("end"))
                    or _coerce_date(fact.get("filed"))
                )
                fact_value = _clean_numeric(fact.get("val"))
                if fact_date is None or fact_value is None:
                    continue
                latest_by_date.setdefault(fact_date, fact_value)

        history = sorted(latest_by_date.items(), key=lambda item: item[0])
        self._shares_outstanding_history_cache[cik] = history
        return history

    def _pace_sec_request(self) -> None:
        interval_seconds = 1.0 / max(self.sec_max_requests_per_second, 1.0)
        now = time.monotonic()
        elapsed = now - self._last_sec_request_at
        if elapsed < interval_seconds:
            time.sleep(interval_seconds - elapsed)
        self._last_sec_request_at = time.monotonic()

    def _ensure_identity(self) -> None:
        set_identity(self.edgar_identity)
        EDGAR_LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        use_local_storage(EDGAR_LOCAL_CACHE_DIR, allow_network_fallback=True)
