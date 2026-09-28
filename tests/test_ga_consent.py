"""
Static checks on the GA consent script (ga_consent.js) and how app.py injects it.
The behaviour in a real browser is checked by tests/ga_browser_check.py (Playwright).

Run from the repo root:  python -m unittest tests.test_ga_consent -v
"""

import ast
import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JS = (ROOT / "ga_consent.js").read_text(encoding="utf-8")
APP = (ROOT / "app.py").read_text(encoding="utf-8")


def _consent_calls(kind):
    """Bodies of gtag("consent", "<kind>", {...}) calls."""
    return re.findall(r'gtag\("consent", "%s", \{(.*?)\}\)' % kind, JS, re.S)


class TestConsentScript(unittest.TestCase):
    def test_consent_mode_defaults_all_denied(self):
        defaults = _consent_calls("default")
        self.assertEqual(len(defaults), 1)
        for key in ("analytics_storage", "ad_storage", "ad_user_data", "ad_personalization"):
            self.assertRegex(defaults[0], r'%s: "denied"' % key)

    def test_only_analytics_storage_is_ever_granted(self):
        for body in _consent_calls("update"):
            self.assertNotRegex(body, r"ad_(storage|user_data|personalization)")
        self.assertNotRegex(JS, r'ad_\w+: "granted"')

    def test_default_is_set_before_config_and_script_load(self):
        order = [JS.index('gtag("consent", "default"'), JS.index('gtag("consent", "update", '
                 '{ analytics_storage: "granted" });\n    w.gtag("js"'),
                 JS.index('w.gtag("config"'), JS.index("d.head.appendChild(s)")]
        self.assertEqual(order, sorted(order))

    def test_ga_only_loaded_from_accept_or_stored_grant(self):
        # loadGA() is called only after Accept (choose("granted")) or a stored "granted"
        calls = re.findall(r"[^\n]*loadGA\(\);", JS)
        self.assertEqual(sorted(c.strip() for c in calls),
                         sorted(['if (value === "granted") loadGA();',
                                 'if (choice === "granted") loadGA();']))
        self.assertEqual(JS.count("googletagmanager.com"), 1)  # only inside loadGA

    def test_page_location_strips_query_and_hash(self):
        self.assertIn("page_location: cleanLocation()", JS)
        self.assertIn("/^utm_/i.test(key)", JS)
        body = JS[JS.index("function cleanLocation"):JS.index("function loadGA")]
        self.assertIn("u.origin + path", body)
        self.assertNotIn("u.hash", body)
        self.assertNotIn("location.href;", body)

    def test_referrer_sent_as_origin_only(self):
        self.assertIn("page_location: cleanLocation(), page_referrer: referrerOrigin()", JS)
        body = JS[JS.index("function referrerOrigin"):JS.index("function loadGA")]
        self.assertIn("new URL(ref).origin", body)
        self.assertIn('origin === w.location.origin ? "" : origin', body)
        self.assertIn('return "";', body)  # no/invalid referrer -> nothing

    def test_bot_check(self):
        self.assertIn("w.navigator.webdriver === true", JS)
        self.assertIn("/HeadlessChrome|Headless|bot/i.test(ua)", JS)
        load = JS[JS.index("function loadGA"):JS.index("function deleteGaCookies")]
        self.assertTrue(load.split("{", 1)[1].lstrip().startswith("if (isBot()) return;"))

    def test_once_per_page_load_and_storage_key(self):
        self.assertIn("if (w.__cordisConsentInit) return;", JS)
        self.assertIn('var KEY = "cordis_ga_consent";', JS)

    def test_withdraw_deletes_ga_cookies(self):
        body = JS[JS.index("function withdraw"):JS.index("function button")]
        for step in ("clearChoice()", '"ga-disable-" + GA_ID] = true',
                     'analytics_storage: "denied"', "deleteGaCookies()"):
            self.assertIn(step, body)

    def test_decline_deletes_leftover_ga_cookies(self):
        body = JS[JS.index("function choose"):JS.index("function showBanner")]
        self.assertIn("else deleteGaCookies();", body)

    def test_close_button_declines(self):
        self.assertIn('close.addEventListener("click", function () { choose("denied"); });', JS)


class TestAppInjection(unittest.TestCase):
    def test_no_direct_ga_in_app(self):
        self.assertNotIn("googletagmanager", APP)
        self.assertNotIn("gtag(", APP)

    def test_ga_id_validated_and_escaped(self):
        self.assertIn('_GA_ID_RE = re.compile(r"G-[A-Z0-9]{4,20}")', APP)
        self.assertIn("_GA_ID_RE.fullmatch(_ga_id)", APP)
        self.assertIn('js.replace("__GA_ID__", json.dumps(ga_id))', APP)

    def test_injected_payload_is_valid_and_script_safe(self):
        tree = ast.parse(APP)
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                  and n.name == "_ga_consent_html")
        ns = {"json": json, "_APP_DIR": ROOT}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "app.py", "exec"), ns)
        html = ns["_ga_consent_html"]("G-TEST1234")
        self.assertEqual(html.count("</script>"), 1)  # payload can't close the tag early
        self.assertIn("if(p.__cordisConsentInit)return;", html)
        self.assertIn('\\"G-TEST1234\\"', html)
        self.assertNotIn("__GA_ID__", html)

    def test_notes_mention_consent(self):
        self.assertEqual(APP.count("This site uses Google Analytics only if you accept cookies."), 2)
        self.assertNotIn("This site uses Google Analytics.<", APP)


if __name__ == "__main__":
    unittest.main(verbosity=2)
