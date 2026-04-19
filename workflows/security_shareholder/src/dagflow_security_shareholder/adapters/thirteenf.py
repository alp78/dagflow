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
                pipeline_code="shareholder_holdings",
                dataset_code="shareholder_holdings",
                source_name="holdings_13f",
                landing_table="raw.holdings_13f",
                source_url=self.filings_url,
                object_key=(
                    f"shareholder_holdings/holdings_13f/"
                    f"{business_date.isoformat()}/holdings_13f.csv"
                ),
                file_name="holdings_13f.csv",
                business_date=business_date,
                ingestion_mode="edgar_filing_pull",
                expected_format="csv",
                canonical_object_key=(
                    f"shareholder_holdings/holdings_13f/"
                    f"{business_date.isoformat()}/holdings_13f.parquet"
                ),
                canonical_file_name="holdings_13f.parquet",
                manifest_object_key=(
                    f"shareholder_holdings/holdings_13f/"
                    f"{business_date.isoformat()}/manifest.json"
                ),
                manifest_file_name="manifest.json",
            ),
            SourceDescriptor(
                pipeline_code="shareholder_holdings",
                dataset_code="shareholder_holdings",
                source_name="holdings_13f_filers",
                landing_table="raw.holdings_13f_filers",
                source_url=self.filings_url,
                object_key=(
                    f"shareholder_holdings/holdings_13f_filers/"
                    f"{business_date.isoformat()}/holdings_13f_filers.csv"
                ),
                file_name="holdings_13f_filers.csv",
                business_date=business_date,
                ingestion_mode="edgar_filing_pull",
                expected_format="csv",
                canonical_object_key=(
                    f"shareholder_holdings/holdings_13f_filers/"
                    f"{business_date.isoformat()}/holdings_13f_filers.parquet"
                ),
                canonical_file_name="holdings_13f_filers.parquet",
                manifest_object_key=(
                    f"shareholder_holdings/holdings_13f_filers/"
                    f"{business_date.isoformat()}/manifest.json"
                ),
                manifest_file_name="manifest.json",
            ),
        ]
