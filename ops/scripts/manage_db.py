from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = ROOT / "db" / "migrations"
SEEDS_DIR = ROOT / "db" / "seeds"
LOCK_KEY = 824_377_111
HISTORY_TABLE_SQL = """
create table if not exists public.dagflow_schema_history (
    change_type text not null,
    version text not null,
    checksum text not null,
    applied_at timestamptz not null default timezone('utc', now()),
    primary key (change_type, version)
);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dagflow SQL migration and seed runner")
    parser.add_argument("command", choices=["migrate", "seed", "status"])
    parser.add_argument("--dsn", required=True, help="PostgreSQL connection string")
    return parser.parse_args()


def checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def discover(kind: str) -> list[Path]:
    base_dir = MIGRATIONS_DIR if kind == "migration" else SEEDS_DIR
    return sorted(base_dir.glob("*.sql"))


def ensure_history_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(HISTORY_TABLE_SQL)
    conn.commit()


def fetch_applied(conn: psycopg.Connection, kind: str) -> dict[str, str]:
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            select version, checksum
            from public.dagflow_schema_history
            where change_type = %s
            order by version
            """,
            (kind,),
        )
        rows = cursor.fetchall()
    return {row["version"]: row["checksum"] for row in rows}


def apply_change(conn: psycopg.Connection, kind: str, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    digest = checksum(path)
    with conn.transaction():
        with conn.cursor() as cursor:
            cursor.execute(sql)
            cursor.execute(
                """
                insert into public.dagflow_schema_history (change_type, version, checksum)
                values (%s, %s, %s)
                on conflict (change_type, version) do update
                set checksum = excluded.checksum,
                    applied_at = timezone('utc', now())
                """,
                (kind, path.name, digest),
            )


def run_changes(conn: psycopg.Connection, kind: str) -> int:
    applied = fetch_applied(conn, kind)
    files = discover(kind)
    executed = 0
    for path in files:
        digest = checksum(path)
        if path.name in applied:
            if applied[path.name] != digest:
                raise RuntimeError(
                    f"{kind} {path.name} has changed after being applied. "
                    "Create a new numbered SQL file instead of editing history."
                )
            continue
        print(f"Applying {kind}: {path.name}")
        apply_change(conn, kind, path)
        executed += 1
    return executed


def print_status(conn: psycopg.Connection) -> None:
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            select change_type, version, checksum, applied_at
            from public.dagflow_schema_history
            order by change_type, version
            """
        )
        rows = cursor.fetchall()
    if not rows:
        print("No migrations or seeds have been applied yet.")
        return
    for row in rows:
        print(
            f"{row['change_type']:>9}  {row['version']:<40}  "
            f"{row['applied_at'].isoformat()}  {row['checksum'][:12]}"
        )


def main() -> int:
    args = parse_args()
    change_type = "migration" if args.command == "migrate" else "seed"
    with psycopg.connect(args.dsn, autocommit=False) as conn:
        ensure_history_table(conn)
        with conn.cursor() as cursor:
            cursor.execute("select pg_advisory_lock(%s)", (LOCK_KEY,))
        try:
            if args.command == "status":
                print_status(conn)
                conn.commit()
                return 0
            executed = run_changes(conn, change_type)
            conn.commit()
        finally:
            with conn.cursor() as cursor:
                cursor.execute("select pg_advisory_unlock(%s)", (LOCK_KEY,))
            conn.commit()
    print(f"Applied {executed} {change_type}(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
