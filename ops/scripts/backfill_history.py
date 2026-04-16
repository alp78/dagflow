#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import uuid
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "apps" / "dagster" / "src"))

from dagflow_dagster.dbt_topology import get_dbt_project  # noqa: E402
from dagflow_dagster.resources import ControlPlaneResource  # noqa: E402


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        os.environ.setdefault(key.strip(), value.strip())


def business_days(start_date: date, end_date: date) -> list[date]:
    days: list[date] = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def deterministic_run_id(pipeline_code: str, business_date: date) -> uuid.UUID:
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"https://dagflow.local/backfill/{pipeline_code}/{business_date.isoformat()}",
    )


def run_dbt_command(
    dbt_executable: Path,
    command: list[str],
) -> None:
    project = get_dbt_project()
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


def log(message: str) -> None:
    print(message, flush=True)


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


def digest_rows(rows: list[dict[str, object]]) -> str:
    materialized = "".join(
        sorted(str(row.get("row_hash") or row.get("source_payload_hash") or "") for row in rows)
    )
    return hashlib.sha256(materialized.encode("utf-8")).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay two years of business-day Dagflow history and populate SCD2 snapshots."
    )
    parser.add_argument("--years", type=int, default=2)
    parser.add_argument("--end-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--limit-days", type=int)
    parser.add_argument("--warmup-days", type=int, default=180)
    parser.add_argument("--stale-after-days", type=int, default=180)
    parser.add_argument("--filing-limit", type=int, default=800)
    parser.add_argument(
        "--database-url",
        default=os.getenv("POSTGRES_DSN") or "postgresql://postgres:postgres@localhost:5432/dagflow",
    )
    parser.add_argument(
        "--export-root-dir",
        default=os.getenv("EXPORT_ROOT_DIR") or str(ROOT / "generated_exports"),
    )
    parser.add_argument("--edgar-identity", default=os.getenv("EDGAR_IDENTITY") or "")
    parser.add_argument("--openfigi-api-key", default=os.getenv("OPENFIGI_API_KEY") or None)
    parser.add_argument("--finnhub-api-key", default=os.getenv("FINNHUB_API_KEY") or None)
    parser.add_argument("--publish-review-snapshots", action="store_true")
    return parser.parse_args()


def main() -> int:
    load_env_file(ROOT / ".env")
    args = parse_args()

    start_date = args.start_date or (args.end_date - timedelta(days=(365 * args.years) - 1))
    days = business_days(start_date, args.end_date)
    if args.limit_days:
        days = days[: args.limit_days]
    if not days:
        print("No business days to replay.", file=sys.stderr, flush=True)
        return 1

    control_plane = ControlPlaneResource(
        direct_database_url=args.database_url,
        export_root_dir=args.export_root_dir,
        edgar_identity=args.edgar_identity or "Dagflow local dev support@dagflow.local",
        sec_13f_lookback_days=14,
        sec_13f_filing_limit=max(50, args.filing_limit),
        sec_security_focus_limit=80,
        openfigi_api_key=args.openfigi_api_key,
        finnhub_api_key=args.finnhub_api_key,
    )
    configure_dbt_env(args.database_url)
    dbt_executable = Path(sys.executable).with_name("dbt")

    log(
        f"Preparing historical filing cache for {days[0].isoformat()} -> {days[-1].isoformat()} "
        f"({len(days)} business days)..."
    )
    historical_filings = control_plane.ingestion.historical_13f_filings(
        days[0],
        days[-1],
        warmup_days=args.warmup_days,
        filing_limit=args.filing_limit,
        progress_callback=log,
    )
    log(f"Loaded {len(historical_filings)} parsed 13F filings for replay.")
    previous_security_hash: str | None = None
    previous_holdings_hash: str | None = None
    previous_security_review_run_id: uuid.UUID | None = None
    previous_holdings_review_run_id: uuid.UUID | None = None

    for index, business_date in enumerate(days, start=1):
        log(f"[{index}/{len(days)}] Replaying {business_date.isoformat()}...")
        snapshot = control_plane.ingestion.historical_13f_snapshot(
            business_date,
            historical_filings,
            stale_after_days=args.stale_after_days,
        )
        if not snapshot.focus_tickers:
            log(f"  skipping {business_date.isoformat()} because no active holdings were found.")
            continue

        ticker_rows = control_plane.ingestion.build_security_master_tickers_from_snapshot(snapshot)
        fact_rows = control_plane.ingestion.build_security_master_facts_from_ticker_rows(
            business_date,
            ticker_rows,
        )
        security_hash = digest_rows(ticker_rows + fact_rows)
        holdings_hash = digest_rows(snapshot.filer_records + snapshot.holding_records)

        security_run_id = deterministic_run_id("security_master", business_date)
        holdings_run_id = deterministic_run_id("shareholder_holdings", business_date)

        if (
            previous_security_hash is not None
            and previous_holdings_hash is not None
            and security_hash == previous_security_hash
            and holdings_hash == previous_holdings_hash
        ):
            if args.publish_review_snapshots:
                log("  no source or dimension change detected; cloning prior review snapshots")
            else:
                log("  no source or dimension change detected; recording lightweight daily runs")
            control_plane.ensure_pipeline_run(
                pipeline_code="security_master",
                dataset_code="security_master",
                run_id=security_run_id,
                business_date=business_date,
                orchestrator_run_id=f"backfill-security-master-{business_date.isoformat()}",
            )
            control_plane.ensure_pipeline_run(
                pipeline_code="shareholder_holdings",
                dataset_code="shareholder_holdings",
                run_id=holdings_run_id,
                business_date=business_date,
                orchestrator_run_id=f"backfill-shareholder-holdings-{business_date.isoformat()}",
            )
            if args.publish_review_snapshots and previous_security_review_run_id is not None:
                security_review_count = control_plane.clone_historical_review_snapshot(
                    "security_master",
                    previous_security_review_run_id,
                    security_run_id,
                    business_date,
                )
                log(
                    "  security_master: cloned "
                    f"{security_review_count} review rows from {previous_security_review_run_id}"
                )
                previous_security_review_run_id = security_run_id
            if args.publish_review_snapshots and previous_holdings_review_run_id is not None:
                holdings_review_count = control_plane.clone_historical_review_snapshot(
                    "shareholder_holdings",
                    previous_holdings_review_run_id,
                    holdings_run_id,
                    business_date,
                    security_target_run_id=security_run_id,
                )
                log(
                    "  shareholder_holdings: cloned "
                    f"{holdings_review_count} review rows from {previous_holdings_review_run_id}"
                )
                previous_holdings_review_run_id = holdings_run_id
            for pipeline_code, dataset_code, run_id in (
                ("security_master", "security_master", security_run_id),
                ("shareholder_holdings", "shareholder_holdings", holdings_run_id),
            ):
                control_plane.complete_pipeline_run(
                    run_id,
                    status="historical_loaded",
                    metadata={
                        "mode": "backfill",
                        "change": "none",
                        "review_snapshot_published": args.publish_review_snapshots,
                        "review_snapshot_mode": "cloned" if args.publish_review_snapshots else "none",
                    },
                )
            continue

        control_plane.ensure_pipeline_run(
            pipeline_code="security_master",
            dataset_code="security_master",
            run_id=security_run_id,
            business_date=business_date,
            orchestrator_run_id=f"backfill-security-master-{business_date.isoformat()}",
        )
        try:
            log(
                "  security_master: building raw rows for "
                f"{len(snapshot.focus_tickers)} focus tickers"
            )
            log(
                "  security_master: loading "
                f"{len(ticker_rows)} ticker rows and {len(fact_rows)} fact rows"
            )
            control_plane.load_security_master_ticker_records(
                security_run_id,
                business_date,
                ticker_rows,
            )
            control_plane.load_security_master_fact_records(
                security_run_id,
                business_date,
                fact_rows,
            )
            log("  security_master: running dbt build + snapshot")
            run_dbt_command(
                dbt_executable,
                [
                    "build",
                    "--select",
                    "tag:security_master",
                    "dim_security_snapshot",
                    "--exclude",
                    "tag:exports",
                    "--vars",
                    (
                        "{"
                        f"\"dagflow_run_id\":\"{security_run_id}\","
                        "\"dagflow_pipeline_code\":\"security_master\","
                        f"\"dagflow_business_date\":\"{business_date.isoformat()}\""
                        "}"
                    ),
                ],
            )
            control_plane.complete_pipeline_run(
                security_run_id,
                status="historical_loaded",
                metadata={"mode": "backfill", "review_snapshot_published": False},
            )
            if args.publish_review_snapshots:
                review_count = control_plane.publish_historical_review_snapshot(
                    "security_master",
                    "security_master",
                    security_run_id,
                    business_date,
                )
                control_plane.complete_pipeline_run(
                    security_run_id,
                    status="historical_loaded",
                    metadata={
                        "mode": "backfill",
                        "review_snapshot_published": True,
                        "review_snapshot_mode": "materialized",
                        "review_row_count": review_count,
                    },
                )
                previous_security_review_run_id = security_run_id
            log(f"  security_master: completed run {security_run_id}")
        except Exception as error:
            control_plane.capture_failure(
                pipeline_code="security_master",
                dataset_code="security_master",
                run_id=security_run_id,
                step_name="backfill_history",
                stage="historical_replay",
                business_date=business_date,
                error=error,
            )
            raise

        control_plane.ensure_pipeline_run(
            pipeline_code="shareholder_holdings",
            dataset_code="shareholder_holdings",
            run_id=holdings_run_id,
            business_date=business_date,
            orchestrator_run_id=f"backfill-shareholder-holdings-{business_date.isoformat()}",
        )
        try:
            log(
                "  shareholder_holdings: loading "
                f"{len(snapshot.filer_records)} filer rows and "
                f"{len(snapshot.holding_records)} holding rows"
            )
            control_plane.load_shareholder_filer_records(
                holdings_run_id,
                business_date,
                snapshot.filer_records,
            )
            control_plane.load_shareholder_holding_records(
                holdings_run_id,
                business_date,
                snapshot.holding_records,
            )
            log("  shareholder_holdings: running dbt build + snapshot")
            run_dbt_command(
                dbt_executable,
                [
                    "build",
                    "--select",
                    "tag:shareholder_holdings",
                    "dim_shareholder_snapshot",
                    "--exclude",
                    "tag:exports",
                    "--vars",
                    (
                        "{"
                        f"\"dagflow_run_id\":\"{holdings_run_id}\","
                        "\"dagflow_pipeline_code\":\"shareholder_holdings\","
                        f"\"dagflow_business_date\":\"{business_date.isoformat()}\","
                        f"\"dagflow_security_run_id\":\"{security_run_id}\""
                        "}"
                    ),
                ],
            )
            control_plane.complete_pipeline_run(
                holdings_run_id,
                status="historical_loaded",
                metadata={"mode": "backfill", "review_snapshot_published": False},
            )
            if args.publish_review_snapshots:
                review_count = control_plane.publish_historical_review_snapshot(
                    "shareholder_holdings",
                    "shareholder_holdings",
                    holdings_run_id,
                    business_date,
                    security_run_id=security_run_id,
                )
                control_plane.complete_pipeline_run(
                    holdings_run_id,
                    status="historical_loaded",
                    metadata={
                        "mode": "backfill",
                        "review_snapshot_published": True,
                        "review_snapshot_mode": "materialized",
                        "review_row_count": review_count,
                    },
                )
                previous_holdings_review_run_id = holdings_run_id
            log(f"  shareholder_holdings: completed run {holdings_run_id}")
            previous_security_hash = security_hash
            previous_holdings_hash = holdings_hash
        except Exception as error:
            control_plane.capture_failure(
                pipeline_code="shareholder_holdings",
                dataset_code="shareholder_holdings",
                run_id=holdings_run_id,
                step_name="backfill_history",
                stage="historical_replay",
                business_date=business_date,
                error=error,
            )
            raise

    log(f"Backfill complete for {len(days)} business days ending {days[-1].isoformat()}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
