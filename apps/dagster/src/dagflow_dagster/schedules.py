from __future__ import annotations

from typing import Any

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)


def build_pipeline_sensor(job: Any, pipeline_code: str) -> Any:
    @sensor(
        name=f"{pipeline_code}_sensor",
        job=job,
        default_status=DefaultSensorStatus.RUNNING,
        required_resource_keys={"control_plane"},
    )
    def _sensor(_context: SensorEvaluationContext) -> Any:
        yield SkipReason(
            f"{pipeline_code} is loaded explicitly from landed source dates, "
            "not from current_date sensors."
        )

    return _sensor


def build_review_validation_sensor(job: Any, pipeline_code: str) -> Any:
    @sensor(
        name=f"{pipeline_code}_review_validation_sensor",
        job=job,
        default_status=DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=5,
        required_resource_keys={"control_plane"},
    )
    def _sensor(context: SensorEvaluationContext) -> Any:
        control_plane = context.resources.control_plane
        if not control_plane.pipeline_enabled(pipeline_code):
            yield SkipReason(f"{pipeline_code} is disabled in control.pipeline_registry")
            return

        approved_runs = control_plane.export_ready_runs(pipeline_code)
        if not approved_runs:
            yield SkipReason(f"No validated {pipeline_code} review runs are waiting for export")
            return

        for approved_run in approved_runs:
            validated_at = approved_run["validated_at"].isoformat()
            run_id = str(approved_run["run_id"])
            business_date = approved_run["business_date"].isoformat()
            yield RunRequest(
                run_key=f"{pipeline_code}:{run_id}:validated-export:{validated_at}",
                tags={
                    "pipeline_code": pipeline_code,
                    "dagflow_pipeline_code": pipeline_code,
                    "dagflow_run_id": run_id,
                    "dagflow_business_date": business_date,
                    "validated_business_date": business_date,
                    "validated_at": validated_at,
                    "storage_prefix": str(approved_run["storage_prefix"]),
                    "export_contract_code": str(approved_run["export_contract_code"]),
                },
            )

    return _sensor
