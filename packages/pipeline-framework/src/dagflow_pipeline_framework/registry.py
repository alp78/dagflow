from __future__ import annotations

from typing import Final

import psycopg
from psycopg.rows import dict_row

from dagflow_pipeline_framework.contracts import PipelineRegistration

PIPELINE_REGISTRY_QUERY: Final[str] = """
select
    pipeline_code,
    pipeline_name,
    dataset_code,
    source_type,
    adapter_type,
    is_enabled,
    is_schedulable,
    default_schedule,
    review_required,
    review_table_name,
    export_contract_code,
    calc_package_code,
    approval_workflow_code,
    storage_prefix,
    owner_team
from control.pipeline_registry
order by pipeline_code
"""


class PipelineRegistryRepository:
    def list_pipelines(self, dsn: str) -> list[PipelineRegistration]:
        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cursor:
                cursor.execute(PIPELINE_REGISTRY_QUERY)
                rows = cursor.fetchall()
        return [PipelineRegistration.model_validate(row) for row in rows]
