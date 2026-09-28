"""
Ask Claude cost limits: daily cap, per-session limit, cooldown, question length,
Redis-down fallback, and counting before the API call.

Runs the real app.py with Streamlit's AppTest, with a fake Anthropic client and an
in-memory fake Redis — no real API, Redis or GA calls.

Run from the repo root:  python -m unittest tests.test_ai_limits -v
"""

import os
import re
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import streamlit as st  # noqa: E402
from streamlit.testing.v1 import AppTest  # noqa: E402

APP = str(ROOT / "app.py")
DB_PATH = ROOT / "cordis.duckdb"
QUESTION = "How many projects are there per framework programme?"
DAILY_KEY = re.compile(r"^cordis_ai_questions:\d{4}-\d{2}-\d{2}$")

events = []  # ordered log of fake Redis INCRs and fake API calls


class FakeRedis:
    store = {}

    def __init__(self, url, token):
        pass

    def setnx(self, key, value):
        self.store.setdefault(key, value)

    def get(self, key):
        return self.store.get(key)

    def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        if DAILY_KEY.match(key):
            events.append("daily_incr")
        return self.store[key]

    def expire(self, key, seconds):
        self.store[f"ttl:{key}"] = seconds


class DownRedis(FakeRedis):
    def _down(self, *args):
        raise ConnectionError("cannot reach https://fake-host.upstash.io with token FAKE_TOKEN")

    setnx = get = incr = expire = _down


class FakeAnthropic:
    def __init__(self, api_key):
        self.messages = self

    def create(self, model, max_tokens, system, messages):
        events.append("api")
        text = {600: "SELECT FP, COUNT(*) AS n FROM projects GROUP BY FP",
                250: "Summary.", 20: "Projects per framework programme"}[max_tokens]
        return SimpleNamespace(content=[SimpleNamespace(text=text)])


def _env():
    return mock.patch.dict(os.environ, {
        "ANTHROPIC_API_KEY": "test-key-not-real", "GA_MEASUREMENT_ID": "",
        "UPSTASH_REDIS_REST_URL": "https://fake-host.upstash.io",
        "UPSTASH_REDIS_REST_TOKEN": "FAKE_TOKEN",
    })


@unittest.skipUnless(DB_PATH.exists(), "cordis.duckdb not present")
class TestAiLimits(unittest.TestCase):
    redis_cls = FakeRedis

    def setUp(self):
        events.clear()
        FakeRedis.store = {}
        self._patches = [_env(), mock.patch("anthropic.Anthropic", FakeAnthropic),
                         mock.patch("upstash_redis.Redis", self.redis_cls)]
        for p in self._patches:
            p.start()
        st.cache_resource.clear()
        self.at = AppTest.from_file(APP, default_timeout=120).run()

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    def ask(self, question=QUESTION, skip_cooldown=True):
        if skip_cooldown and "_ai_last_question_at" in self.at.session_state:
            self.at.session_state["_ai_last_question_at"] = -1e9
        box = next(t for t in self.at.text_input if t.label == "Your question")
        box.set_value(question)
        self.at = next(b for b in self.at.button if b.label == "Ask Claude").click().run()
        self.assertEqual(len(self.at.exception), 0)
        return [w.value for w in self.at.warning]

    def captions(self):
        return [c.value for c in self.at.caption]

    def test_normal_question_calls_api_and_shows_session_remaining(self):
        self.assertEqual(self.ask(), [])
        self.assertIn("api", events)
        self.assertIn("9 of 10 questions left this session", self.captions())
        self.assertFalse(any("200" in c for c in self.captions()))  # no global count shown

    def test_counter_incremented_before_api_call(self):
        self.ask()
        self.assertEqual(events[0], "daily_incr")
        self.assertEqual(events.count("daily_incr"), 1)
        self.assertGreater(events.count("api"), 0)
        key = next(k for k in FakeRedis.store if DAILY_KEY.match(k))
        self.assertEqual(FakeRedis.store[f"ttl:{key}"], 48 * 3600)

    def test_daily_cap_reached_means_no_api_call(self):
        from datetime import datetime, timezone
        FakeRedis.store[f"cordis_ai_questions:{datetime.now(timezone.utc):%Y-%m-%d}"] = 200
        warnings = self.ask()
        self.assertIn("The AI assistant has reached today's limit, please try again tomorrow. "
                      "The SQL tab is still available.", warnings)
        self.assertNotIn("api", events)
        self.assertIn("10 of 10 questions left this session", self.captions())  # not charged

    def test_per_session_limit(self):
        for i in range(10):
            self.assertEqual(self.ask(), [], f"question {i + 1}")
        api_calls = events.count("api")
        self.assertIn("0 of 10 questions left this session", self.captions())
        warnings = self.ask()
        self.assertTrue(any("used all 10 questions" in w for w in warnings), warnings)
        self.assertEqual(events.count("api"), api_calls)  # no further API calls
        self.assertEqual(events.count("daily_incr"), 10)  # refused question not counted

    def test_cooldown(self):
        self.assertEqual(self.ask(), [])
        api_calls = events.count("api")
        warnings = self.ask(skip_cooldown=False)  # immediately again
        self.assertIn("Please wait 5 seconds between questions.", warnings)
        self.assertEqual(events.count("api"), api_calls)
        self.assertEqual(self.ask(), [])  # fine once the cooldown has passed

    def test_long_question_rejected(self):
        long_q = "Which countries received the most funding " * 12  # > 500 chars
        self.assertGreater(len(long_q), 500)
        warnings = self.ask(long_q)
        self.assertIn("Please keep questions under 500 characters.", warnings)
        self.assertNotIn("api", events)
        self.assertNotIn("daily_incr", events)

    def test_sql_tab_note(self):
        sql_tab = self.at.tabs[3]
        notes = " ".join(m.value for m in sql_tab.markdown)
        self.assertIn("Queries you run here are not saved. This site uses Google Analytics only if you accept cookies.", notes)
        self.assertNotIn("shown to all visitors", notes)
        for i in (4, 5):  # AI Query and History tabs keep the full note
            self.assertIn("shown to all visitors",
                          " ".join(m.value for m in self.at.tabs[i].markdown))


@unittest.skipUnless(DB_PATH.exists(), "cordis.duckdb not present")
class TestAiLimitsRedisDown(TestAiLimits.__base__):
    def setUp(self):
        events.clear()
        self._patches = [_env(), mock.patch("anthropic.Anthropic", FakeAnthropic),
                         mock.patch("upstash_redis.Redis", DownRedis)]
        for p in self._patches:
            p.start()
        st.cache_resource.clear()
        self.at = AppTest.from_file(APP, default_timeout=120).run()

    tearDown = TestAiLimits.tearDown
    ask = TestAiLimits.ask

    def test_redis_down_allows_with_session_limit_only(self):
        with self.assertLogs("cordis", "ERROR") as logs:
            self.assertEqual(self.ask(), [])
        self.assertIn("api", events)  # question allowed
        logged = "\n".join(logs.output)
        self.assertIn("AI daily cap failed: ConnectionError", logged)
        for secret in ("fake-host.upstash.io", "FAKE_TOKEN"):
            self.assertNotIn(secret, logged)
        # The per-session limit still applies
        self.at.session_state["_ai_questions_used"] = 10
        api_calls = events.count("api")
        warnings = self.ask()
        self.assertTrue(any("used all 10 questions" in w for w in warnings), warnings)
        self.assertEqual(events.count("api"), api_calls)


if __name__ == "__main__":
    unittest.main(verbosity=2)
