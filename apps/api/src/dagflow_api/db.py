from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


class Database:
    def __init__(self, dsn: str) -> None:
        self.pool = ConnectionPool(
            conninfo=dsn,
            open=False,
            min_size=1,
            max_size=10,
            kwargs={"row_factory": dict_row},
        )

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        with self.pool.connection() as connection:
            yield connection

    def close(self) -> None:
        self.pool.close()
