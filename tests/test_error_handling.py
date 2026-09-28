"""
Visitors never see internals: startup failures, query errors and counter failures.

Runs the real app.py headlessly with Streamlit's AppTest. Secrets are blanked in the
environment (load_dotenv never overrides existing vars), so no Anthropic, Upstash or GA
calls are made.

Run from the repo root:  python -m unittest tests.test_error_handling -v
"""

import os
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402
import streamlit as st  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

import sql_safety  # noqa: E402
from sql_safety import query_error_message  # noqa: E402

APP = str(ROOT / "app.py")
DB_PATH = ROOT / "cordis.duckdb"
GENERIC = "The app is temporarily unavailable, please try again later."
_SECRET_VARS = ("ANTHROPIC_API_KEY", "UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN",
                "GA_MEASUREMENT_ID")


def _blank_env(**overrides):
    env = {k: "" for k in _SECRET_VARS}
    env.update(overrides)
    return mock.patch.dict(os.environ, env)


def _run_app():
    st.cache_resource.clear()
    return AppTest.from_file(APP, default_timeout=120).run()


def _visible_text(at) -> str:
    parts = []
    for kind in ("error", "warning", "info", "markdown", "code", "exception", "caption"):
        parts += [str(getattr(e, "value", "")) for e in getattr(at, kind)]
    return "\n".join(parts)


class TestQueryErrorMessage(unittest.TestCase):
    def test_duckdb_error_is_one_line_without_query_echo(self):
        con = duckdb.connect()
        try:
            con.execute("SELECT nosuchcol FROM range(1)")
        except duckdb.Error as e:
            exc = e
        self.assertIn("LINE 1", str(exc))  # DuckDB does echo the query...
        with self.assertLogs("cordis", "WARNING") as logs:
            msg = query_error_message(exc)
        self.assertTrue(msg.startswith("Query failed: Binder Error:"), msg)
        self.assertNotIn("\n", msg)
        self.assertNotIn("LINE", msg)  # ...but the visitor message doesn't
        self.assertNotIn("Candidate bindings", msg)
        self.assertIn("Traceback", "\n".join(logs.output))  # full error goes to the log

    def test_empty_message(self):
        with self.assertLogs("cordis", "WARNING"):
            self.assertEqual(query_error_message(RuntimeError()), "Query failed.")


@unittest.skipUnless(DB_PATH.exists(), "cordis.duckdb not present")
class TestAppErrorHandling(unittest.TestCase):
    def test_startup_failure_shows_generic_message(self):
        boom = RuntimeError("internal detail /mount/src/secret-path")
        with _blank_env(), mock.patch.object(sql_safety, "connect_readonly", side_effect=boom), \
                self.assertLogs("cordis", "ERROR") as logs:
            at = _run_app()
        self.assertEqual([e.value for e in at.error], [GENERIC])
        self.assertEqual(len(at.exception), 0)
        shown = _visible_text(at)
        self.assertNotIn("internal detail", shown)
        self.assertNotIn("Traceback", shown)
        logged = "\n".join(logs.output)
        self.assertIn("Startup failed", logged)
        self.assertIn("internal detail", logged)  # full detail kept server-side

    def test_bad_query_shows_one_line_error(self):
        with _blank_env():
            at = _run_app()
            sql_box = next(t for t in at.text_area if t.label == "SQL")
            sql_box.input("SELECT nosuchcol FROM projects")
            run_btn = next(b for b in at.button if b.label == "▶ Run Query")
            with self.assertLogs("cordis", "WARNING"):
                at = run_btn.click().run()
        errors = [e.value for e in at.error]
        self.assertEqual(len(errors), 1, errors)
        self.assertTrue(errors[0].startswith("Query failed: Binder Error:"), errors[0])
        self.assertIn("nosuchcol", errors[0])
        self.assertNotIn("\n", errors[0])
        self.assertNotIn("LINE", errors[0])
        self.assertEqual(len(at.exception), 0)
        self.assertNotIn("Traceback", _visible_text(at))

    def test_counter_failure_is_logged_and_silent(self):
        url = "https://fake-host-123.upstash.io"
        token = "FAKE_TOKEN_abc123"

        class FailingRedis:
            def __init__(self, url, token):
                self._url, self._token = url, token

            def setnx(self, *a):
                raise ConnectionError(f"cannot reach {self._url} (host fake-host-123.upstash.io) "
                                      f"with token {self._token}")

            get = incr = setnx

        with _blank_env(UPSTASH_REDIS_REST_URL=url, UPSTASH_REDIS_REST_TOKEN=token), \
                mock.patch("upstash_redis.Redis", FailingRedis), \
                self.assertLogs("cordis", "ERROR") as logs:
            at = _run_app()
        logged = "\n".join(logs.output)
        self.assertIn("Queries-run counter read failed: ConnectionError", logged)
        for secret in (url, token, "fake-host-123.upstash.io"):
            self.assertNotIn(secret, logged)
        # Silent for visitors: page renders, no error or exception shown
        self.assertEqual(len(at.exception), 0)
        self.assertEqual(len(at.error), 0)
        self.assertGreater(len(at.tabs), 0)


class TestNoTracebackOutput(unittest.TestCase):
    def test_app_source_has_no_traceback_display(self):
        src = (ROOT / "app.py").read_text(encoding="utf-8")
        for bad in ("st.exception(", "format_exc(", "print_exc(", "_Detail:", "Technical detail"):
            self.assertNotIn(bad, src)
        self.assertIn('st.set_option("client.showErrorDetails", "none")', src)


if __name__ == "__main__":
    unittest.main(verbosity=2)
