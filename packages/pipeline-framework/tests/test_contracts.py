from uuid import UUID

from dagflow_pipeline_framework.contracts import PipelineRunContext


def test_pipeline_run_context_accepts_uuid_string() -> None:
    context = PipelineRunContext(
        pipeline_code="security_master",
        dataset_code="security_master",
        run_id="00000000-0000-0000-0000-000000000001",
        business_date="2026-04-15",
    )

    assert isinstance(context.run_id, UUID)
