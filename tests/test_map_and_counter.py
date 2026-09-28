"""
Geography map respects the sidebar filters; the queries-run counter can't be
inflated by one session.

Runs the real app.py with Streamlit's AppTest (no real Anthropic/Upstash/GA calls).

Run from the repo root:  python -m unittest tests.test_map_and_counter -v
"""

import base64
import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import sql_safety  # noqa: E402
import streamlit as st  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

APP = str(ROOT / "app.py")
DB_PATH = ROOT / "cordis.duckdb"
COUNTER_KEY = "cordis_analytics_queries_run"


def _env(**extra):
    env = {"ANTHROPIC_API_KEY": "", "GA_MEASUREMENT_ID": "",
           "UPSTASH_REDIS_REST_URL": "", "UPSTASH_REDIS_REST_TOKEN": ""}
    env.update(extra)
    return mock.patch.dict(os.environ, env)


def _run_app():
    st.cache_resource.clear()
    return AppTest.from_file(APP, default_timeout=120).run()


def map_counts(at) -> dict:
    """{ISO-3 country: projects} from the Country Participation Map's chart data."""
    for chart in at.get("plotly_chart"):
        spec = json.loads(chart.proto.spec)
        if spec["layout"].get("title", {}).get("text") == "Organisation Participation by Country":
            trace = spec["data"][0]
            z = trace["z"]
            if isinstance(z, dict):  # plotly's typed-array encoding
                z = np.frombuffer(base64.b64decode(z["bdata"]), dtype=z["dtype"]).tolist()
            return dict(zip(trace["locations"], z))
    raise AssertionError("map not found")


@unittest.skipUnless(DB_PATH.exists(), "cordis.duckdb not present")
class TestGeographyMapFilters(unittest.TestCase):
    def test_map_follows_sidebar_filters(self):
        with _env():
            at = _run_app()
            all_fp = map_counts(at)
            fp_box, status_box = at.sidebar.multiselect[0], at.sidebar.multiselect[1]
            statuses = status_box.value
            year_lo, year_hi = at.sidebar.slider[0].value
            at = fp_box.set_value(["H2020"]).run()
            h2020 = map_counts(at)
        self.assertEqual(len(at.exception), 0)

        # Independent count with the same filters
        con = sql_safety.connect_readonly(str(DB_PATH))  # same config as the app's connection
        status_list = ",".join(f"'{s}'" for s in statuses)
        expected_de = con.execute(f"""
            SELECT COUNT(DISTINCT o.projectID) FROM organizations o JOIN projects p ON p.id = o.projectID
            WHERE o.country = 'DE' AND p.FP = 'H2020' AND p.status IN ({status_list})
              AND YEAR(p.startDate) BETWEEN {year_lo} AND {year_hi}""").fetchone()[0]
        con.close()

        self.assertEqual(h2020["DEU"], expected_de)
        self.assertLess(h2020["DEU"], all_fp["DEU"])
        for iso3, n in h2020.items():  # filtering can only shrink each country's count
            self.assertLessEqual(n, all_fp.get(iso3, 0), iso3)

    def test_all_dashboard_queries_use_the_filter(self):
        src = (ROOT / "app.py").read_text(encoding="utf-8")
        start, end = src.index("with tab1:"), src.index("with tab4:")
        dashboard = src[start:end]
        queries = dashboard.split("con.execute(")[1:]
        self.assertGreaterEqual(len(queries), 10)
        for q in queries:
            with self.subTest(query=q[:80]):
                self.assertIn("{W()}", q.split(".df()")[0])


class CountingRedis:
    store = {}
    incrs = []

    def __init__(self, url, token):
        pass

    def setnx(self, key, value):
        self.store.setdefault(key, value)

    def get(self, key):
        return self.store.get(key)

    def incr(self, key):
        self.incrs.append(key)
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    def expire(self, key, seconds):
        pass


@unittest.skipUnless(DB_PATH.exists(), "cordis.duckdb not present")
class TestQueriesRunCounterCap(unittest.TestCase):
    def setUp(self):
        CountingRedis.store, CountingRedis.incrs = {}, []
        self.patches = [_env(UPSTASH_REDIS_REST_URL="https://fake.upstash.io",
                             UPSTASH_REDIS_REST_TOKEN="FAKE"),
                        mock.patch("upstash_redis.Redis", CountingRedis)]
        for p in self.patches:
            p.start()
        self.at = _run_app()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()

    def run_sql(self, sql):
        box = next(t for t in self.at.text_area if t.label == "SQL")
        box.input(sql)
        self.at = next(b for b in self.at.button if b.label == "▶ Run Query").click().run()
        self.assertEqual(len(self.at.exception), 0)
        self.assertEqual(len(self.at.error), 0)

    def counter_incrs(self):
        return CountingRedis.incrs.count(COUNTER_KEY)

    def test_identical_query_counts_once_per_session(self):
        self.run_sql("SELECT COUNT(*) FROM projects")
        self.run_sql("SELECT COUNT(*) FROM projects")
        self.run_sql("select   count(*)  from projects;")  # same query, different spacing/case
        self.assertEqual(self.counter_incrs(), 1)

    def test_at_most_30_per_session(self):
        for i in range(35):
            self.run_sql(f"SELECT {i} AS n")
        self.assertEqual(self.counter_incrs(), 30)
        # Silent for visitors: no warnings about the cap
        self.assertEqual(len(self.at.warning), 0)

    def test_no_identifiers_in_session(self):
        self.run_sql("SELECT 1")
        keys = set(self.at.session_state.filtered_state)
        self.assertIn("_counted_query_hashes", keys)
        stored = self.at.session_state["_counted_query_hashes"]
        self.assertTrue(all(len(h) == 12 for h in stored))  # short query hashes only


if __name__ == "__main__":
    unittest.main(verbosity=2)
