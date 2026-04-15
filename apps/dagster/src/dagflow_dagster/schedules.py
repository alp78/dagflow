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
    @sensor(name=f"{pipeline_code}_sensor", job=job, default_status=DefaultSensorStatus.RUNNING)
    def _sensor(context: SensorEvaluationContext) -> Any:
        control_plane = context.resources.control_plane
        if not control_plane.pipeline_enabled(pipeline_code):
            yield SkipReason(f"{pipeline_code} is disabled in control.pipeline_registry")
            return
        business_date = control_plane.current_business_date()
        run_key = f"{pipeline_code}:{business_date.isoformat()}"
        yield RunRequest(
            run_key=run_key,
            tags={"pipeline_code": pipeline_code, "business_date": business_date.isoformat()},
        )

    return _sensor
