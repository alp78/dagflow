from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from dagflow_source_adapters.contracts import SourceDescriptor


@dataclass(frozen=True)
class Sec13FBulkAdapter:
    adapter_type: str = "sec_13f"
    filings_url: str = "https://www.sec.gov/Archives/edgar/full-index/"

    def build_sources(self, business_date: date) -> list[SourceDescriptor]:
        return [
            SourceDescriptor(
                source_name="holdings_13f",
                landing_table="raw.holdings_13f",
                source_url=self.filings_url,
                object_key=f"shareholder_holdings/{business_date.isoformat()}/13f/holdings_index",
                business_date=business_date,
                ingestion_mode="edgar_filing_pull",
                expected_format="xml",
            ),
            SourceDescriptor(
                source_name="holdings_13f_filers",
                landing_table="raw.holdings_13f_filers",
                source_url=self.filings_url,
                object_key=f"shareholder_holdings/{business_date.isoformat()}/13f/filers_index",
                business_date=business_date,
                ingestion_mode="edgar_filing_pull",
                expected_format="xml",
            ),
        ]
