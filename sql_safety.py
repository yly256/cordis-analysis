"""
Hardened DuckDB access for visitor-supplied SQL (SQL tab, Ask Claude, replayed history).

- connect_readonly(): read-only connection with file/network access and extension
  loading disabled, and configuration locked so SQL can't turn them back on.
- guard_sql(): allows exactly one SELECT statement (parsed by DuckDB, not regex).
- run_user_query(): guard + per-query cursor + timeout + row cap.
"""

import logging
import os
import threading

import duckdb
import pandas as pd

QUERY_TIMEOUT_S = 15
MAX_ROWS = 10_000

# app.py compares this with the file's current mtime to detect a stale module after a deploy
_LOADED_MTIME = os.path.getmtime(__file__)

# Server-side log (Streamlit Cloud "Manage app" logs). Visitors never see this.
log = logging.getLogger("cordis")
if not log.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
    log.addHandler(_h)
    log.setLevel(logging.INFO)
    log.propagate = False

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


def query_error_message(exc: Exception) -> str:
    """Log the full error server-side; return a one-line message safe to show visitors.

    DuckDB messages put the summary on the first line and echo the query
    ("LINE 1: ...") and candidate bindings on later lines — only the first is kept.
    """
    log.warning("Query failed", exc_info=exc)
    text = str(exc).strip()
    first = text.splitlines()[0].split("LINE ")[0].strip() if text else ""
    if len(first) > 200:
        first = first[:200] + "…"
    return f"Query failed: {first}" if first else "Query failed."


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
