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
                source_name="sec_company_tickers",
                landing_table="raw.sec_company_tickers",
                source_url=self.company_tickers_url,
                object_key=f"security_master/{business_date.isoformat()}/company_tickers.json",
                business_date=business_date,
                ingestion_mode="api_pull",
                expected_format="json",
            ),
            SourceDescriptor(
                source_name="sec_company_facts",
                landing_table="raw.sec_company_facts",
                source_url=self.company_facts_url_template,
                object_key=f"security_master/{business_date.isoformat()}/company_facts/{{cik}}.json",
                business_date=business_date,
                ingestion_mode="api_pull",
                expected_format="json",
            ),
        ]
