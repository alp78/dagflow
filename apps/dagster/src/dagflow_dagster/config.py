from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings


class DagsterSettings(BaseSettings):
    app_env: str = "local"
    database_url: str = "postgresql://postgres:postgres@pgbouncer:6432/dagflow"
    direct_database_url: str = "postgresql://postgres:postgres@postgres:5432/dagflow"
    dbt_profiles_dir: str = "/workspace/dbt"
    export_root_dir: str = "/workspace/generated_exports"
    edgar_identity: str = "Dagflow local dev support@dagflow.local"
    sec_13f_lookback_days: int = 14
    sec_13f_filing_limit: int = 50
    sec_security_focus_limit: int = 80
    openfigi_api_key: str | None = None
    finnhub_api_key: str | None = None

    @property
    def dbt_project_dir(self) -> Path:
        return Path(__file__).resolve().parents[4] / "dbt"

    @property
    def resolved_dbt_profiles_dir(self) -> Path:
        configured_path = Path(self.dbt_profiles_dir)
        if configured_path.exists():
            return configured_path
        return self.dbt_project_dir


@lru_cache(maxsize=1)
def get_settings() -> DagsterSettings:
    return DagsterSettings()
