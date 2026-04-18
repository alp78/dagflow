from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from dagflow_source_adapters.contracts import SourceDescriptor


@dataclass(frozen=True)
class SecJsonAdapter:
    adapter_type: str = "sec_json"
    company_tickers_url: str = "https://www.sec.gov/files/company_tickers.json"
    company_facts_url_template: str = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    def build_sources(self, business_date: date) -> list[SourceDescriptor]:
        return [
            SourceDescriptor(
                pipeline_code="security_master",
                dataset_code="security_master",
                source_name="sec_company_tickers",
                landing_table="raw.sec_company_tickers",
                source_url=self.company_tickers_url,
                object_key=(
                    f"security_master/sec_company_tickers/"
                    f"{business_date.isoformat()}/sec_company_tickers.csv"
                ),
                file_name="sec_company_tickers.csv",
                business_date=business_date,
                ingestion_mode="api_pull",
                expected_format="csv",
                canonical_object_key=(
                    f"security_master/sec_company_tickers/"
                    f"{business_date.isoformat()}/sec_company_tickers.parquet"
                ),
                canonical_file_name="sec_company_tickers.parquet",
                manifest_object_key=(
                    f"security_master/sec_company_tickers/"
                    f"{business_date.isoformat()}/manifest.json"
                ),
                manifest_file_name="manifest.json",
            ),
            SourceDescriptor(
                pipeline_code="security_master",
                dataset_code="security_master",
                source_name="sec_company_facts",
                landing_table="raw.sec_company_facts",
                source_url=self.company_facts_url_template,
                object_key=(
                    f"security_master/sec_company_facts/"
                    f"{business_date.isoformat()}/sec_company_facts.csv"
                ),
                file_name="sec_company_facts.csv",
                business_date=business_date,
                ingestion_mode="api_pull",
                expected_format="csv",
                canonical_object_key=(
                    f"security_master/sec_company_facts/"
                    f"{business_date.isoformat()}/sec_company_facts.parquet"
                ),
                canonical_file_name="sec_company_facts.parquet",
                manifest_object_key=(
                    f"security_master/sec_company_facts/"
                    f"{business_date.isoformat()}/manifest.json"
                ),
                manifest_file_name="manifest.json",
            ),
        ]
