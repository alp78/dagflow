from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineStepDefinition:
    step_key: str
    label: str
    group: str
    sort_order: int
    seed_on_prepare: bool = True


PIPELINE_STEP_DEFINITIONS: dict[str, tuple[PipelineStepDefinition, ...]] = {
    "security_master": (
        PipelineStepDefinition(
            step_key="sec_company_tickers_capture",
            label="Capture SEC tickers",
            group="source_capture",
            sort_order=10,
        ),
        PipelineStepDefinition(
            step_key="sec_company_facts_capture",
            label="Capture SEC facts",
            group="source_capture",
            sort_order=20,
        ),
        PipelineStepDefinition(
            step_key="sec_company_tickers_raw",
            label="Load ticker contract rows",
            group="contract_load",
            sort_order=30,
        ),
        PipelineStepDefinition(
            step_key="sec_company_facts_raw",
            label="Load facts contract rows",
            group="contract_load",
            sort_order=40,
        ),
        PipelineStepDefinition(
            step_key="security_master_transform_assets",
            label="Run security transforms",
            group="dbt_build",
            sort_order=50,
        ),
        PipelineStepDefinition(
            step_key="security_master_review_snapshot",
            label="Publish review snapshot",
            group="review",
            sort_order=60,
        ),
        PipelineStepDefinition(
            step_key="security_master_export_bundle",
            label="Write export bundle",
            group="validated_export",
            sort_order=80,
            seed_on_prepare=False,
        ),
    ),
    "shareholder_holdings": (
        PipelineStepDefinition(
            step_key="holdings_13f_capture",
            label="Capture 13F holdings",
            group="source_capture",
            sort_order=10,
        ),
        PipelineStepDefinition(
            step_key="holdings_13f_filers_capture",
            label="Capture filer roster",
            group="source_capture",
            sort_order=20,
        ),
        PipelineStepDefinition(
            step_key="holdings_13f_raw",
            label="Load holdings contract rows",
            group="contract_load",
            sort_order=30,
        ),
        PipelineStepDefinition(
            step_key="holdings_13f_filers_raw",
            label="Load filer contract rows",
            group="contract_load",
            sort_order=40,
        ),
        PipelineStepDefinition(
            step_key="shareholder_holdings_transform_assets",
            label="Run holdings transforms",
            group="dbt_build",
            sort_order=50,
        ),
        PipelineStepDefinition(
            step_key="shareholder_holdings_review_snapshot",
            label="Publish review snapshot",
            group="review",
            sort_order=60,
        ),
        PipelineStepDefinition(
            step_key="shareholder_holdings_export_bundle",
            label="Write export bundle",
            group="validated_export",
            sort_order=80,
            seed_on_prepare=False,
        ),
    ),
}


def pipeline_step_definitions(pipeline_code: str) -> tuple[PipelineStepDefinition, ...]:
    return PIPELINE_STEP_DEFINITIONS.get(pipeline_code, ())


def pipeline_step_definition(
    pipeline_code: str,
    step_key: str,
) -> PipelineStepDefinition | None:
    for step in pipeline_step_definitions(pipeline_code):
        if step.step_key == step_key:
            return step
    return None


def export_step_key(pipeline_code: str) -> str:
    return f"{pipeline_code}_export_bundle"
