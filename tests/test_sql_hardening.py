"""
Tests for sql_safety: the hardened connection + guard used by every visitor-SQL path.

Run from the repo root:  python -m unittest tests.test_sql_hardening -v
Needs cordis.duckdb in the repo root (same as the app).
"""

import ast
import sys
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import sql_safety  # noqa: E402
from sql_safety import (  # noqa: E402
    connect_readonly, run_user_query, UnsafeQueryError, QueryTimeoutError,
)

DB_PATH = ROOT / "cordis.duckdb"

# Errors that mean "blocked by policy", as opposed to e.g. file-not-found
BLOCKED = (UnsafeQueryError, duckdb.PermissionException)

MUST_FAIL = [
    "SELECT * FROM read_text('/proc/self/environ')",
    "SELECT * FROM read_csv('http://example.com/x.csv')",
    "SET enable_external_access=true",
    "INSTALL httpfs",
    "LOAD httpfs",
    "ATTACH '/tmp/x.db'",
    "COPY (SELECT 1) TO '/tmp/x.csv'",
    "SELECT 1; SELECT 2",
    # Extras
    "PRAGMA enable_external_access=true",
    "CALL pragma_version()",
    "CREATE TEMP TABLE t AS SELECT 1",
    "EXPORT DATABASE '/tmp/x'",
    "SELECT 1; SET enable_external_access=true",
    "SELECT * FROM read_text('requirements.txt')",  # a file that exists: still blocked
]

MUST_SUCCEED = [
    "SELECT COUNT(*) AS n FROM projects",
    """WITH hp AS (SELECT id FROM projects WHERE totalCost > 1e6)
       SELECT o.country, COUNT(DISTINCT hp.id) AS n
       FROM hp JOIN organizations o ON o.projectID = hp.id
       WHERE o.country IS NOT NULL
       GROUP BY 1 ORDER BY 2 DESC LIMIT 5""",
    "SELECT FP, COUNT(*) AS n FROM projects GROUP BY 1 ORDER BY 1",
]


@unittest.skipUnless(DB_PATH.exists(), "cordis.duckdb not present")
class TestSqlHardening(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = connect_readonly(str(DB_PATH))

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    def test_config_is_locked(self):
        row = self.con.execute(
            "SELECT current_setting('enable_external_access'),"
            " current_setting('autoinstall_known_extensions'),"
            " current_setting('autoload_known_extensions'),"
            " current_setting('lock_configuration')"
        ).fetchone()
        self.assertEqual(row, (False, False, False, True))

    def test_dangerous_queries_fail(self):
        for sql in MUST_FAIL:
            with self.subTest(sql=sql):
                with self.assertRaises(BLOCKED):
                    run_user_query(self.con, sql)

    def test_dangerous_queries_fail_even_without_guard(self):
        """Defence in depth: the connection itself blocks file/network/config changes."""
        for sql in [
            "SELECT * FROM read_text('requirements.txt')",
            "SELECT * FROM read_csv('http://example.com/x.csv')",
            "SET enable_external_access=true",
            "ATTACH '/tmp/x.db'",
            "COPY (SELECT 1) TO 'x_should_not_exist.csv'",
        ]:
            with self.subTest(sql=sql):
                with self.assertRaises(duckdb.Error):
                    self.con.cursor().execute(sql)
        self.assertFalse((ROOT / "x_should_not_exist.csv").exists())

    def test_normal_queries_succeed(self):
        for sql in MUST_SUCCEED:
            with self.subTest(sql=sql):
                df, truncated = run_user_query(self.con, sql)
                self.assertFalse(df.empty)
                self.assertFalse(truncated)

    def test_row_cap(self):
        df, truncated = run_user_query(self.con, "SELECT * FROM range(50000)")
        self.assertEqual(len(df), sql_safety.MAX_ROWS)
        self.assertTrue(truncated)
        df, truncated = run_user_query(self.con, f"SELECT * FROM range({sql_safety.MAX_ROWS})")
        self.assertEqual(len(df), sql_safety.MAX_ROWS)
        self.assertFalse(truncated)

    def test_empty_result_keeps_columns(self):
        df, truncated = run_user_query(self.con, "SELECT acronym FROM projects WHERE 1=0")
        self.assertEqual(list(df.columns), ["acronym"])
        self.assertTrue(df.empty)

    def test_timeout(self):
        orig = sql_safety.QUERY_TIMEOUT_S
        sql_safety.QUERY_TIMEOUT_S = 1
        try:
            with self.assertRaises(QueryTimeoutError):
                run_user_query(self.con, "SELECT COUNT(*) FROM range(100000000000)")
        finally:
            sql_safety.QUERY_TIMEOUT_S = orig
        # Shared connection still usable after an interrupted query
        self.assertEqual(self.con.execute("SELECT 42").fetchone()[0], 42)


class TestAppWiring(unittest.TestCase):
    """Every visitor-SQL path in app.py must go through run_user_query."""

    @classmethod
    def setUpClass(cls):
        cls.tree = ast.parse((ROOT / "app.py").read_text(encoding="utf-8"))

    def _calls(self, attr=None, name=None):
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                f = node.func
                if attr and isinstance(f, ast.Attribute) and f.attr == attr:
                    yield node
                if name and isinstance(f, ast.Name) and f.id == name:
                    yield node

    def test_no_raw_duckdb_connect(self):
        raw = [c.lineno for c in self._calls(attr="connect")
               if isinstance(c.func.value, ast.Name) and c.func.value.id == "duckdb"]
        self.assertEqual(raw, [], "use sql_safety.connect_readonly() instead")

    def test_con_execute_only_takes_app_authored_sql(self):
        """con.execute() may only get string literals / f-strings written in app.py,
        never a variable holding visitor, AI-generated or history SQL."""
        for call in self._calls(attr="execute"):
            recv = call.func.value
            if not (isinstance(recv, ast.Name) and recv.id == "con"):
                continue
            with self.subTest(line=call.lineno):
                self.assertTrue(call.args)
                self.assertIsInstance(call.args[0], (ast.Constant, ast.JoinedStr))

    def test_run_user_query_call_sites(self):
        args = sorted(ast.unparse(c.args[1]) for c in self._calls(name="run_user_query"))
        self.assertEqual(args, sorted([
            "sel['sql_text']",       # History tab replay (_render_query_table)
            "q",                     # SQL tab
            "sql", "sql",            # Ask Claude: generated SQL + corrected SQL
            "_ai5_sel['sql_text']",  # Ask Claude tab: recent-queries replay
        ]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
