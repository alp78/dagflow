from __future__ import annotations

from datetime import date

import psycopg
from dagflow_pipeline_framework.contracts import PipelineRegistration
from dagflow_pipeline_framework.registry import PipelineRegistryRepository
from dagster import ConfigurableResource
from dagster_dbt import DbtCliResource

from dagflow_dagster.config import get_settings


class ControlPlaneResource(ConfigurableResource):
    direct_database_url: str

    def enabled_pipelines(self) -> list[PipelineRegistration]:
        repository = PipelineRegistryRepository()
        pipelines = repository.list_pipelines(self.direct_database_url)
        return [
            pipeline for pipeline in pipelines if pipeline.is_enabled and pipeline.is_schedulable
        ]

    def pipeline_enabled(self, pipeline_code: str) -> bool:
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select is_enabled
                    from control.pipeline_registry
                    where pipeline_code = %s
                    """,
                    (pipeline_code,),
                )
                row = cursor.fetchone()
        if row is None:
            return False
        return bool(row[0])

    def current_business_date(self) -> date:
        with psycopg.connect(self.direct_database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute("select current_date")
                row = cursor.fetchone()
        if row is None:
            raise RuntimeError("Unable to resolve current business date from the database")
        return row[0]


def build_resources() -> dict[str, ConfigurableResource | DbtCliResource]:
    settings = get_settings()
    return {
        "control_plane": ControlPlaneResource(direct_database_url=settings.direct_database_url),
        "dbt": DbtCliResource(
            project_dir=str(settings.dbt_project_dir),
            profiles_dir=str(settings.resolved_dbt_profiles_dir),
            target="local",
        ),
    }
