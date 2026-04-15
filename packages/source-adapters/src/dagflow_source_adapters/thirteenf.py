from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from dagflow_source_adapters.contracts import SourceDescriptor


@dataclass(frozen=True)
class Sec13FBulkAdapter:
    adapter_type: str = "sec_13f"
    bulk_url: str = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/13f.zip"

    def build_sources(self, business_date: date) -> list[SourceDescriptor]:
        return [
            SourceDescriptor(
                source_name="holdings_13f",
                landing_table="raw.holdings_13f",
                source_url=self.bulk_url,
                object_key=f"shareholder_holdings/{business_date.isoformat()}/13f/holdings.zip",
                business_date=business_date,
                ingestion_mode="bulk_file",
                expected_format="zip",
            ),
            SourceDescriptor(
                source_name="holdings_13f_filers",
                landing_table="raw.holdings_13f_filers",
                source_url=self.bulk_url,
                object_key=f"shareholder_holdings/{business_date.isoformat()}/13f/filers.zip",
                business_date=business_date,
                ingestion_mode="bulk_file",
                expected_format="zip",
            ),
        ]
