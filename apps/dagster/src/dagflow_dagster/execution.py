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
    "security_shareholder": (
        PipelineStepDefinition(
            step_key="sec_company_tickers_capture",
            label="Capture Tickers",
            group="source_capture",
            sort_order=10,
        ),
        PipelineStepDefinition(
            step_key="sec_company_facts_capture",
            label="Capture Facts",
            group="source_capture",
            sort_order=20,
        ),
        PipelineStepDefinition(
            step_key="holdings_13f_capture",
            label="Capture Holdings",
            group="source_capture",
            sort_order=30,
        ),
        PipelineStepDefinition(
            step_key="holdings_13f_filers_capture",
            label="Capture Filers",
            group="source_capture",
            sort_order=40,
        ),
        PipelineStepDefinition(
            step_key="security_shareholder_transform_assets",
            label="dbt Transform",
            group="dbt_build",
            sort_order=50,
        ),
        PipelineStepDefinition(
            step_key="security_shareholder_security_master_review",
            label="Review Securities",
            group="review",
            sort_order=60,
        ),
        PipelineStepDefinition(
            step_key="security_shareholder_shareholder_holdings_review",
            label="Review Holdings",
            group="review",
            sort_order=70,
        ),
        PipelineStepDefinition(
            step_key="security_shareholder_export_assets",
            label="dbt Export",
            group="validated_export",
            sort_order=80,
            seed_on_prepare=False,
        ),
        PipelineStepDefinition(
            step_key="security_shareholder_export_bundle_security_master",
            label="Export Securities",
            group="validated_export",
            sort_order=90,
            seed_on_prepare=False,
        ),
        PipelineStepDefinition(
            step_key="security_shareholder_export_bundle_shareholder_holdings",
            label="Export Holdings",
            group="validated_export",
            sort_order=100,
            seed_on_prepare=False,
        ),
        PipelineStepDefinition(
            step_key="security_shareholder__csv_export",
            label="Write CSVs",
            group="validated_export",
            sort_order=110,
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
