"""
Hardened DuckDB access for visitor-supplied SQL (SQL tab, Ask Claude, replayed history).

- connect_readonly(): read-only connection with file/network access and extension
  loading disabled, and configuration locked so SQL can't turn them back on.
- guard_sql(): allows exactly one SELECT statement (parsed by DuckDB, not regex).
- run_user_query(): guard + per-query cursor + timeout + row cap.
"""

import threading

import duckdb
import pandas as pd

QUERY_TIMEOUT_S = 15
MAX_ROWS = 10_000

_HARDENED_CONFIG = {
    "enable_external_access": False,
    "autoinstall_known_extensions": False,
    "autoload_known_extensions": False,
    "lock_configuration": True,
}


class UnsafeQueryError(Exception):
    """Query rejected by the guard. The message is safe to show to users."""


class QueryTimeoutError(Exception):
    """Query exceeded QUERY_TIMEOUT_S. The message is safe to show to users."""


def connect_readonly(path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(path, read_only=True, config=_HARDENED_CONFIG)


def guard_sql(conn: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Raise UnsafeQueryError unless `sql` is exactly one SELECT statement."""
    try:
        statements = conn.extract_statements(sql)
    except Exception:
        raise UnsafeQueryError("The query could not be parsed.") from None
    if len(statements) != 1:
        raise UnsafeQueryError("Only a single SELECT statement is allowed.")
    if statements[0].type != duckdb.StatementType.SELECT:
        raise UnsafeQueryError("Only SELECT queries are allowed.")


def run_user_query(conn: duckdb.DuckDBPyConnection, sql: str):
    """Run visitor SQL safely. Returns (DataFrame, truncated: bool).

    Uses its own cursor so a timeout interrupt only cancels this query, not other
    sessions sharing the cached connection.
    """
    guard_sql(conn, sql)
    cur = conn.cursor()
    timer = threading.Timer(QUERY_TIMEOUT_S, cur.interrupt)
    timer.start()
    try:
        cur.execute(sql)
        # Fetch chunk by chunk until we have one row past the cap or the result ends
        chunks, n = [], 0
        while n <= MAX_ROWS:
            chunk = cur.fetch_df_chunk(1)
            if chunk.empty:
                break
            chunks.append(chunk)
            n += len(chunk)
        df = pd.concat(chunks, ignore_index=True) if len(chunks) > 1 else (
            chunks[0] if chunks else cur.fetch_df()
        )
    except duckdb.InterruptException:
        raise QueryTimeoutError(
            f"The query took longer than {QUERY_TIMEOUT_S} s and was stopped."
        ) from None
    finally:
        timer.cancel()
        cur.close()
    truncated = len(df) > MAX_ROWS
    return df.head(MAX_ROWS), truncated
