#!/usr/bin/env python3
# ruff: noqa: I001
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "apps" / "dagster" / "src"))

from dagflow_dagster.dbt_topology import get_dbt_project  # noqa: E402
from dagflow_dagster.resources import ControlPlaneResource  # noqa: E402


SECURITY_EXPORT_HEADERS = [
    "business_date",
    "ticker",
    "issuer_name",
    "investable_shares",
    "review_materiality_score",
]

HOLDINGS_EXPORT_HEADERS = [
    "business_date",
    "filer_name",
    "security_name",
    "shares_held",
    "holding_pct_of_outstanding",
]


@dataclass
class DayReport:
    business_date: str
    security_run_id: str
    holdings_run_id: str
    landed_files: dict[str, dict[str, Any]]
    security_review_rows: int
    holdings_review_rows: int
    security_issue_count: int
    holdings_issue_count: int
    security_edits: int
    holdings_edits: int
    validation_states: dict[str, str]
    export_files: dict[str, str]
    contract_checks: dict[str, Any]
    notes: list[str]


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        os.environ.setdefault(key.strip(), value.strip())


def calendar_days(start_date: date, end_date: date) -> list[date]:
    days: list[date] = []
    current = start_date
    while current <= end_date:
        days.append(current)
        current += timedelta(days=1)
    return days


def deterministic_run_id(pipeline_code: str, business_date: date) -> uuid.UUID:
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"https://dagflow.local/replay/{pipeline_code}/{business_date.isoformat()}",
    )


def configure_dbt_env(database_url: str) -> None:
    parsed = urlparse(database_url)
    if parsed.hostname:
        os.environ["DBT_HOST"] = parsed.hostname
    if parsed.port:
        os.environ["PGBOUNCER_PORT"] = str(parsed.port)
    if parsed.username:
        os.environ["POSTGRES_USER"] = unquote(parsed.username)
    if parsed.password:
        os.environ["POSTGRES_PASSWORD"] = unquote(parsed.password)
    if parsed.path and parsed.path != "/":
        os.environ["POSTGRES_DB"] = parsed.path.lstrip("/")


def run_dbt_command(command: list[str]) -> None:
    project = get_dbt_project()
    dbt_executable = Path(sys.executable).with_name("dbt")
    full_command = [
        str(dbt_executable),
        *command,
        "--project-dir",
        str(project.project_dir),
        "--profiles-dir",
        str(project.profiles_dir),
        "--target-path",
        str(project.target_path),
    ]
    if project.target:
        full_command.extend(["--target", project.target])
    subprocess.run(full_command, check=True, cwd=ROOT)


def report_log(message: str) -> None:
    print(f"{datetime.now().isoformat(timespec='seconds')} {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replay daily Dagflow processing for 2026, simulate reviewer edits, "
            "and verify sensor-driven exports."
        )
    )
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 1, 1))
    parser.add_argument("--end-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--limit-days", type=int)
    parser.add_argument("--warmup-days", type=int, default=180)
    parser.add_argument("--stale-after-days", type=int, default=180)
    parser.add_argument("--filing-limit", type=int, default=800)
    parser.add_argument("--review-edit-ratio", type=float, default=0.30)
    parser.add_argument(
        "--database-url",
        default=os.getenv("POSTGRES_DSN")
        or "postgresql://postgres:postgres@localhost:5432/dagflow",
    )
    parser.add_argument(
        "--landing-root-dir",
        default=os.getenv("LANDING_ROOT_DIR") or str(ROOT / "source_landing"),
    )
    parser.add_argument(
        "--export-root-dir",
        default=os.getenv("EXPORT_ROOT_DIR") or str(ROOT / "generated_exports"),
    )
    parser.add_argument("--edgar-identity", default=os.getenv("EDGAR_IDENTITY") or "")
    parser.add_argument("--openfigi-api-key", default=os.getenv("OPENFIGI_API_KEY") or None)
    parser.add_argument("--finnhub-api-key", default=os.getenv("FINNHUB_API_KEY") or None)
    parser.add_argument("--export-timeout-seconds", type=int, default=3600)
    parser.add_argument("--export-poll-seconds", type=int, default=10)
    parser.add_argument(
        "--report-path",
        default=str(ROOT / "ops" / "reports" / "2026_pipeline_monitor_report.json"),
    )
    return parser.parse_args()


def db_fetch_all(dsn: str, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return list(cursor.fetchall())


def db_fetch_one(dsn: str, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
            return dict(row) if row is not None else None


def ensure_pipeline_run(
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    dataset_code: str,
    run_id: uuid.UUID,
    business_date: date,
) -> None:
    control_plane.ensure_pipeline_run(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=business_date,
        orchestrator_run_id=f"monitor-{pipeline_code}-{business_date.isoformat()}",
    )


def publish_review(
    control_plane: ControlPlaneResource,
    pipeline_code: str,
    dataset_code: str,
    run_id: uuid.UUID,
    business_date: date,
) -> tuple[int, int]:
    control_plane.publish_review_snapshot(
        pipeline_code=pipeline_code,
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=business_date,
        actor="dagster-monitor",
        notes="Review snapshot published by 2026 monitoring run",
    )
    row_count = control_plane.review_row_count(dataset_code, run_id)
    issue_payload = control_plane.scan_review_data_issues(
        dataset_code=dataset_code,
        run_id=run_id,
        business_date=business_date,
        actor="dagster-monitor",
    )
    control_plane.complete_pipeline_run(
        run_id=run_id,
        status="pending_review",
        metadata={
            "review_row_count": row_count,
            "review_issue_count": int(issue_payload["issue_count"]),
            "mode": "2026_monitor",
        },
    )
    return row_count, int(issue_payload["issue_count"])


def mutate_numeric_value(column_name: str, value: Any, rng: random.Random) -> str:
    numeric_value = float(value)
    if column_name in {"shares_outstanding_raw", "shares_held_raw"}:
        factor = rng.choice([0.94, 0.96, 1.04, 1.06])
        return str(max(1, int(round(numeric_value * factor))))
    if column_name in {"free_float_pct_raw", "investability_factor_raw"}:
        factor = rng.choice([0.92, 0.96, 1.04, 1.08])
        mutated = max(0.01, min(0.99, round(numeric_value * factor, 6)))
        return f"{mutated:.6f}"
    if column_name == "reviewed_market_value_raw":
        factor = rng.choice([0.93, 0.97, 1.03, 1.07])
        mutated = round(max(1.0, numeric_value * factor), 6)
        return f"{mutated:.6f}"
    raise KeyError(f"Unsupported editable column: {column_name}")


def _sample_size(row_count: int, ratio: float) -> int:
    if row_count <= 0:
        return 0
    return max(1, int(round(row_count * ratio)))


def simulate_holdings_edits(
    dsn: str,
    run_id: uuid.UUID,
    business_date: date,
    ratio: float,
) -> int:
    rows = db_fetch_all(
        dsn,
        """
        select review_row_id, shares_held_raw, reviewed_market_value_raw
        from review.shareholder_holdings_daily
        where run_id = %s
        order by review_row_id
        """,
        (run_id,),
    )
    if not rows:
        return 0

    rng = random.Random(f"holdings:{business_date.isoformat()}")
    sampled_rows = rng.sample(rows, k=min(len(rows), _sample_size(len(rows), ratio)))
    edits = 0

    with psycopg.connect(dsn, autocommit=False) as connection:
        with connection.cursor() as cursor:
            for index, row in enumerate(sampled_rows, start=1):
                available_columns = [
                    column_name
                    for column_name in ("shares_held_raw", "reviewed_market_value_raw")
                    if row.get(column_name) is not None
                ]
                if not available_columns:
                    continue
                column_name = rng.choice(available_columns)
                new_value = mutate_numeric_value(column_name, row[column_name], rng)
                cursor.execute(
                    """
                    select review.apply_cell_edit(
                        'review.shareholder_holdings_daily'::regclass,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    """,
                    (
                        row["review_row_id"],
                        column_name,
                        new_value,
                        "reviewer-sim",
                        "Automated monitoring edit",
                    ),
                )
                cursor.execute(
                    "select calc.recalc_holding_review_fields(%s, %s)",
                    (row["review_row_id"], "reviewer-sim"),
                )
                edits += 1
                if index % 100 == 0:
                    connection.commit()
            connection.commit()

    issue_query = "select audit.refresh_review_data_issues_for_run(%s, %s, %s, %s)"
    with psycopg.connect(dsn, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                issue_query,
                ("shareholder_holdings", run_id, business_date, "reviewer-sim"),
            )
        connection.commit()
    return edits


def simulate_security_edits(
    dsn: str,
    run_id: uuid.UUID,
    business_date: date,
    ratio: float,
) -> int:
    rows = db_fetch_all(
        dsn,
        """
        select
            review_row_id,
            shares_outstanding_raw,
            free_float_pct_raw,
            investability_factor_raw
        from review.security_master_daily
        where run_id = %s
        order by review_row_id
        """,
        (run_id,),
    )
    if not rows:
        return 0

    rng = random.Random(f"security:{business_date.isoformat()}")
    sampled_rows = rng.sample(rows, k=min(len(rows), _sample_size(len(rows), ratio)))
    edits = 0

    with psycopg.connect(dsn, autocommit=False) as connection:
        with connection.cursor() as cursor:
            for index, row in enumerate(sampled_rows, start=1):
                available_columns = [
                    column_name
                    for column_name in (
                        "shares_outstanding_raw",
                        "free_float_pct_raw",
                        "investability_factor_raw",
                    )
                    if row.get(column_name) is not None
                ]
                if not available_columns:
                    continue
                column_name = rng.choice(available_columns)
                new_value = mutate_numeric_value(column_name, row[column_name], rng)
                cursor.execute(
                    """
                    select review.apply_cell_edit(
                        'review.security_master_daily'::regclass,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    """,
                    (
                        row["review_row_id"],
                        column_name,
                        new_value,
                        "reviewer-sim",
                        "Automated monitoring edit",
                    ),
                )
                cursor.execute(
                    "select calc.recalc_security_review_fields(%s, %s)",
                    (row["review_row_id"], "reviewer-sim"),
                )
                edits += 1
                if index % 100 == 0:
                    connection.commit()
            cursor.execute("select calc.recalc_security_rollups(%s)", (run_id,))
            cursor.execute(
                "select audit.refresh_review_data_issues_for_run(%s, %s, %s, %s)",
                ("security_master", run_id, business_date, "reviewer-sim"),
            )
            cursor.execute(
                "select audit.refresh_review_data_issues_for_run(%s, %s, %s, %s)",
                ("shareholder_holdings", run_id, business_date, "reviewer-sim"),
            )
        connection.commit()
    return edits


def validate_run(
    dsn: str,
    pipeline_code: str,
    dataset_code: str,
    run_id: uuid.UUID,
    business_date: date,
) -> str:
    with psycopg.connect(dsn, autocommit=False, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select workflow.validate_dataset(%s, %s, %s, %s, %s, %s) as payload
                """,
                (
                    pipeline_code,
                    dataset_code,
                    run_id,
                    business_date,
                    "reviewer-sim",
                    "Validated by automated monitoring flow",
                ),
            )
            payload = cursor.fetchone()["payload"]
        connection.commit()
    return str(payload["review_state"])


def dataset_state_summary(dsn: str, start_date: date, end_date: date) -> list[dict[str, Any]]:
    return db_fetch_all(
        dsn,
        """
        select pipeline_code, dataset_code, run_id, business_date, review_state
        from workflow.dataset_review_state
        where business_date between %s and %s
        order by business_date, pipeline_code
        """,
        (start_date, end_date),
    )


def wait_for_exports(
    dsn: str,
    start_date: date,
    end_date: date,
    expected_runs: int,
    timeout_seconds: int,
    poll_seconds: int,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        rows = dataset_state_summary(dsn, start_date, end_date)
        exported = [row for row in rows if row["review_state"] == "exported"]
        if len(exported) >= expected_runs:
            return {
                "completed": True,
                "rows": rows,
                "exported_count": len(exported),
            }
        time.sleep(poll_seconds)
    rows = dataset_state_summary(dsn, start_date, end_date)
    exported = [row for row in rows if row["review_state"] == "exported"]
    return {
        "completed": False,
        "rows": rows,
        "exported_count": len(exported),
    }


def export_file_paths(export_root_dir: str, business_date: date) -> dict[str, Path]:
    return {
        "security_master": (
            Path(export_root_dir)
            / "security-master"
            / business_date.isoformat()
            / f"security_master_{business_date.isoformat()}.csv"
        ),
        "shareholder_holdings": (
            Path(export_root_dir)
            / "shareholder-holdings"
            / business_date.isoformat()
            / f"shareholder_holdings_{business_date.isoformat()}.csv"
        ),
    }


def validate_csv_contract(
    file_path: Path,
    expected_headers: list[str],
    min_rows: int = 1,
) -> dict[str, Any]:
    if not file_path.exists():
        return {"exists": False, "row_count": 0, "headers_match": False}
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = reader.fieldnames or []
        rows = list(reader)
    return {
        "exists": True,
        "row_count": len(rows),
        "headers_match": headers == expected_headers,
        "headers": headers,
        "meets_min_rows": len(rows) >= min_rows,
    }


def export_final_row_count(dsn: str, table_name: str, run_id: uuid.UUID) -> int:
    row = db_fetch_one(
        dsn,
        f"select count(*) as count from export.{table_name} where run_id = %s",
        (run_id,),
    )
    return int(row["count"]) if row is not None else 0


def run_day(
    *,
    control_plane: ControlPlaneResource,
    ingestion_service: Any,
    database_url: str,
    business_date: date,
    edit_ratio: float,
    notes: list[str],
) -> DayReport | None:
    snapshot = ingestion_service.recent_13f_snapshot(business_date)
    if not snapshot.focus_tickers:
        notes.append("No active 13F-derived focus tickers were available; day skipped.")
        return None

    security_run_id = deterministic_run_id("security_master", business_date)
    holdings_run_id = deterministic_run_id("shareholder_holdings", business_date)

    report_log(
        f"[{business_date.isoformat()}] capture start: "
        f"{len(snapshot.focus_tickers)} focus tickers, "
        f"{len(snapshot.holding_records)} holdings, {len(snapshot.filer_records)} filers"
    )

    ticker_rows = ingestion_service.build_security_master_tickers_from_snapshot(snapshot)
    ticker_file = control_plane.capture_source_file(
        "sec_company_tickers",
        business_date,
        rows=ticker_rows,
    )
    facts_file = control_plane.capture_security_master_facts_from_landed_tickers(business_date)
    holdings_file = control_plane.capture_source_file(
        "holdings_13f",
        business_date,
        rows=snapshot.holding_records,
    )
    filers_file = control_plane.capture_shareholder_filers_from_landed_holdings(business_date)

    landed_files = {
        "sec_company_tickers": ticker_file,
        "sec_company_facts": facts_file,
        "holdings_13f": holdings_file,
        "holdings_13f_filers": filers_file,
    }
    for source_name, file_record in landed_files.items():
        path = Path(str(file_record["storage_path"]))
        if not path.exists():
            raise RuntimeError(
                f"{business_date.isoformat()} {source_name} landing file is missing at {path}"
            )

    ensure_pipeline_run(
        control_plane,
        "security_master",
        "security_master",
        security_run_id,
        business_date,
    )
    security_tickers_loaded = control_plane.load_security_master_tickers(
        security_run_id,
        business_date,
    )
    security_facts_loaded = control_plane.load_security_master_facts(
        security_run_id,
        business_date,
    )
    report_log(
        f"[{business_date.isoformat()}] security_master raw load: "
        f"{security_tickers_loaded} tickers, {security_facts_loaded} facts"
    )
    run_dbt_command(
        [
            "build",
            "--select",
            "tag:security_master",
            "dim_security_snapshot",
            "--exclude",
            "tag:exports",
            "--vars",
            json.dumps(
                {
                    "dagflow_run_id": str(security_run_id),
                    "dagflow_pipeline_code": "security_master",
                    "dagflow_business_date": business_date.isoformat(),
                }
            ),
        ]
    )
    security_review_rows, security_issue_count = publish_review(
        control_plane,
        "security_master",
        "security_master",
        security_run_id,
        business_date,
    )

    ensure_pipeline_run(
        control_plane,
        "shareholder_holdings",
        "shareholder_holdings",
        holdings_run_id,
        business_date,
    )
    holdings_loaded = control_plane.load_shareholder_holdings(holdings_run_id, business_date)
    filers_loaded = control_plane.load_shareholder_filers(holdings_run_id, business_date)
    report_log(
        f"[{business_date.isoformat()}] shareholder_holdings raw load: "
        f"{holdings_loaded} holdings, {filers_loaded} filers"
    )
    run_dbt_command(
        [
            "build",
            "--select",
            "tag:shareholder_holdings",
            "dim_shareholder_snapshot",
            "--exclude",
            "tag:exports",
            "--vars",
            json.dumps(
                {
                    "dagflow_run_id": str(holdings_run_id),
                    "dagflow_pipeline_code": "shareholder_holdings",
                    "dagflow_business_date": business_date.isoformat(),
                    "dagflow_security_run_id": str(security_run_id),
                }
            ),
        ]
    )
    holdings_review_rows, holdings_issue_count = publish_review(
        control_plane,
        "shareholder_holdings",
        "shareholder_holdings",
        holdings_run_id,
        business_date,
    )

    holdings_edits = simulate_holdings_edits(
        database_url,
        holdings_run_id,
        business_date,
        edit_ratio,
    )
    security_edits = simulate_security_edits(
        database_url,
        security_run_id,
        business_date,
        edit_ratio,
    )
    report_log(
        f"[{business_date.isoformat()}] simulated reviewer edits: "
        f"{security_edits} security edits, {holdings_edits} holding edits"
    )

    security_state = validate_run(
        database_url,
        "security_master",
        "security_master",
        security_run_id,
        business_date,
    )
    holdings_state = validate_run(
        database_url,
        "shareholder_holdings",
        "shareholder_holdings",
        holdings_run_id,
        business_date,
    )

    return DayReport(
        business_date=business_date.isoformat(),
        security_run_id=str(security_run_id),
        holdings_run_id=str(holdings_run_id),
        landed_files={
            source_name: {
                "storage_path": str(file_record["storage_path"]),
                "row_count": int(file_record["row_count"]),
            }
            for source_name, file_record in landed_files.items()
        },
        security_review_rows=security_review_rows,
        holdings_review_rows=holdings_review_rows,
        security_issue_count=security_issue_count,
        holdings_issue_count=holdings_issue_count,
        security_edits=security_edits,
        holdings_edits=holdings_edits,
        validation_states={
            "security_master": security_state,
            "shareholder_holdings": holdings_state,
        },
        export_files={
            pipeline_code: str(path)
            for pipeline_code, path in export_file_paths(
                control_plane.export_root_dir,
                business_date,
            ).items()
        },
        contract_checks={},
        notes=notes,
    )


def main() -> int:
    load_env_file(ROOT / ".env")
    args = parse_args()
    configure_dbt_env(args.database_url)

    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    all_days = calendar_days(args.start_date, args.end_date)
    if args.limit_days:
        all_days = all_days[: args.limit_days]
    if not all_days:
        print("No days to process.", file=sys.stderr)
        return 1

    control_plane = ControlPlaneResource(
        direct_database_url=args.database_url,
        export_root_dir=args.export_root_dir,
        landing_root_dir=args.landing_root_dir,
        edgar_identity=args.edgar_identity or "Dagflow monitor support@dagflow.local",
        sec_13f_lookback_days=14,
        sec_13f_filing_limit=max(50, args.filing_limit),
        sec_security_focus_limit=80,
        openfigi_api_key=args.openfigi_api_key,
        finnhub_api_key=args.finnhub_api_key,
    )

    ingestion_service = control_plane.ingestion
    report_log(
        f"Running daily recent-source replay for {all_days[0].isoformat()} -> "
        f"{all_days[-1].isoformat()} ({len(all_days)} days)"
    )

    day_reports: list[dict[str, Any]] = []
    processed_days: list[date] = []
    failures: list[dict[str, Any]] = []

    for business_date in all_days:
        notes: list[str] = []
        try:
            day_report = run_day(
                control_plane=control_plane,
                ingestion_service=ingestion_service,
                database_url=args.database_url,
                business_date=business_date,
                edit_ratio=args.review_edit_ratio,
                notes=notes,
            )
            if day_report is None:
                failures.append(
                    {
                        "business_date": business_date.isoformat(),
                        "stage": "capture",
                        "error": "No active holdings-derived snapshot was available",
                    }
                )
                continue
            day_reports.append(day_report.__dict__)
            processed_days.append(business_date)
        except Exception as error:  # noqa: BLE001
            report_log(f"[{business_date.isoformat()}] failure: {error}")
            failures.append(
                {
                    "business_date": business_date.isoformat(),
                    "stage": "daily_run",
                    "error": f"{error.__class__.__name__}: {error}",
                }
            )
            break

    if processed_days:
        wait_result = wait_for_exports(
            args.database_url,
            processed_days[0],
            processed_days[-1],
            expected_runs=len(processed_days) * 2,
            timeout_seconds=args.export_timeout_seconds,
            poll_seconds=args.export_poll_seconds,
        )
    else:
        wait_result = {"completed": False, "rows": [], "exported_count": 0}

    for day_report in day_reports:
        business_date = date.fromisoformat(day_report["business_date"])
        paths = export_file_paths(args.export_root_dir, business_date)
        security_contract = validate_csv_contract(
            paths["security_master"],
            SECURITY_EXPORT_HEADERS,
        )
        holdings_contract = validate_csv_contract(
            paths["shareholder_holdings"],
            HOLDINGS_EXPORT_HEADERS,
        )
        security_export_rows = export_final_row_count(
            args.database_url,
            "security_master_final",
            uuid.UUID(day_report["security_run_id"]),
        )
        holdings_export_rows = export_final_row_count(
            args.database_url,
            "shareholder_holdings_final",
            uuid.UUID(day_report["holdings_run_id"]),
        )
        day_report["contract_checks"] = {
            "security_master": {
                **security_contract,
                "export_table_rows": security_export_rows,
                "row_count_matches_export_table": (
                    security_contract["row_count"] == security_export_rows
                ),
            },
            "shareholder_holdings": {
                **holdings_contract,
                "export_table_rows": holdings_export_rows,
                "row_count_matches_export_table": (
                    holdings_contract["row_count"] == holdings_export_rows
                ),
            },
        }

    summary = {
        "start_date": all_days[0].isoformat(),
        "end_date": all_days[-1].isoformat(),
        "requested_days": len(all_days),
        "processed_days": len(processed_days),
        "export_wait_completed": wait_result["completed"],
        "exported_run_count": wait_result["exported_count"],
        "expected_exported_run_count": len(processed_days) * 2,
        "failures": failures,
        "states": wait_result["rows"],
        "days": day_reports,
    }
    report_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    report_log(f"Wrote report to {report_path}")

    if failures:
        return 1
    if not wait_result["completed"]:
        return 2
    for day_report in day_reports:
        for contract_check in day_report["contract_checks"].values():
            if not contract_check["exists"] or not contract_check["headers_match"]:
                return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
