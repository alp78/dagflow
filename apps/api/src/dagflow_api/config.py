from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "local"
    database_url: str = "postgresql://postgres:postgres@pgbouncer:6432/dagflow"
    direct_database_url: str = "postgresql://postgres:postgres@postgres:5432/dagflow"
    business_timezone: str = "UTC"
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minio"
    minio_secret_key: str = "minio123"
    minio_secure: bool = False


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
