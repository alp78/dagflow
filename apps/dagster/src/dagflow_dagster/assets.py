from __future__ import annotations

from collections.abc import Iterator
from datetime import date

from dagflow_source_adapters.registry import get_adapter
from dagster import MaterializeResult, asset
from dagster_dbt import DbtCliResource


@asset(group_name="security_master", key_prefix=["ingestion"])
def sec_company_tickers_raw(context) -> MaterializeResult:
    descriptor = get_adapter("sec_json").build_sources(date.today())[0]
    context.log.info("Prepared source descriptor for sec_company_tickers")
    return MaterializeResult(
        metadata={"landing_table": descriptor.landing_table, "source_url": descriptor.source_url}
    )


@asset(group_name="security_master", key_prefix=["ingestion"])
def sec_company_facts_raw(context) -> MaterializeResult:
    descriptor = get_adapter("sec_json").build_sources(date.today())[1]
    context.log.info("Prepared source descriptor for sec_company_facts")
    return MaterializeResult(
        metadata={"landing_table": descriptor.landing_table, "source_url": descriptor.source_url}
    )


@asset(group_name="shareholder_holdings", key_prefix=["ingestion"])
def holdings_13f_raw(context) -> MaterializeResult:
    descriptor = get_adapter("sec_13f").build_sources(date.today())[0]
    context.log.info("Prepared source descriptor for holdings_13f")
    return MaterializeResult(
        metadata={"landing_table": descriptor.landing_table, "source_url": descriptor.source_url}
    )


@asset(group_name="shareholder_holdings", key_prefix=["ingestion"])
def holdings_13f_filers_raw(context) -> MaterializeResult:
    descriptor = get_adapter("sec_13f").build_sources(date.today())[1]
    context.log.info("Prepared source descriptor for holdings_13f_filers")
    return MaterializeResult(
        metadata={"landing_table": descriptor.landing_table, "source_url": descriptor.source_url}
    )


def _run_dbt_selection(
    context,
    dbt: DbtCliResource,
    *selection: str,
) -> Iterator[MaterializeResult]:
    command = ["run", "--select", ",".join(selection)]
    dbt.cli(command, context=context).wait()
    yield MaterializeResult(metadata={"dbt_selection": ", ".join(selection)})


@asset(
    deps=[sec_company_tickers_raw, sec_company_facts_raw],
    group_name="security_master",
    key_prefix=["dbt", "staging"],
)
def security_master_staging(context, dbt: DbtCliResource) -> Iterator[MaterializeResult]:
    yield from _run_dbt_selection(context, dbt, "tag:security_master", "tag:staging")


@asset(
    deps=[security_master_staging], group_name="security_master", key_prefix=["dbt", "intermediate"]
)
def security_master_intermediate(context, dbt: DbtCliResource) -> Iterator[MaterializeResult]:
    yield from _run_dbt_selection(context, dbt, "tag:security_master", "tag:intermediate")


@asset(
    deps=[security_master_intermediate], group_name="security_master", key_prefix=["dbt", "marts"]
)
def security_master_marts(context, dbt: DbtCliResource) -> Iterator[MaterializeResult]:
    yield from _run_dbt_selection(context, dbt, "tag:security_master", "tag:marts")


@asset(deps=[security_master_marts], group_name="security_master", key_prefix=["dbt", "exports"])
def security_master_exports(context, dbt: DbtCliResource) -> Iterator[MaterializeResult]:
    yield from _run_dbt_selection(context, dbt, "tag:security_master", "tag:exports")


@asset(
    deps=[holdings_13f_raw, holdings_13f_filers_raw],
    group_name="shareholder_holdings",
    key_prefix=["dbt", "staging"],
)
def shareholder_holdings_staging(context, dbt: DbtCliResource) -> Iterator[MaterializeResult]:
    yield from _run_dbt_selection(context, dbt, "tag:shareholder_holdings", "tag:staging")


@asset(
    deps=[shareholder_holdings_staging],
    group_name="shareholder_holdings",
    key_prefix=["dbt", "intermediate"],
)
def shareholder_holdings_intermediate(
    context,
    dbt: DbtCliResource,
) -> Iterator[MaterializeResult]:
    yield from _run_dbt_selection(context, dbt, "tag:shareholder_holdings", "tag:intermediate")


@asset(
    deps=[shareholder_holdings_intermediate, security_master_marts],
    group_name="shareholder_holdings",
    key_prefix=["dbt", "marts"],
)
def shareholder_holdings_marts(context, dbt: DbtCliResource) -> Iterator[MaterializeResult]:
    yield from _run_dbt_selection(context, dbt, "tag:shareholder_holdings", "tag:marts")


@asset(
    deps=[shareholder_holdings_marts],
    group_name="shareholder_holdings",
    key_prefix=["dbt", "exports"],
)
def shareholder_holdings_exports(context, dbt: DbtCliResource) -> Iterator[MaterializeResult]:
    yield from _run_dbt_selection(context, dbt, "tag:shareholder_holdings", "tag:exports")
