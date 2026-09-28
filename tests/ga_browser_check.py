"""
Real-browser check of GA consent (Playwright). Not part of the unittest suite.

Starts the app locally (GA_MEASUREMENT_ID from .env; Anthropic/Upstash blanked), then:
  (a) first visit: no Google requests, no _ga cookie
  (b) Accept: gtag loads, _ga set, page_location keeps only utm_* (no hash)
  (c) Decline + reload: still no Google requests
  (d) navigator.webdriver = true: no Google requests even after Accept
  (e) Cookie settings: _ga cookies deleted, choice cleared, banner back
  (f) a Streamlit rerun doesn't inject GA or the consent script twice
GA collect hits are read and then aborted, so no test traffic reaches GA.

Run from the repo root:  python tests/ga_browser_check.py
"""

import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
PORT = 8599
BASE = f"http://localhost:{PORT}/"
GOOGLE = re.compile(r"googletagmanager\.com|google-analytics\.com|analytics\.google\.com|doubleclick\.net")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36")

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))


def start_app():
    env = dict(os.environ, ANTHROPIC_API_KEY="", UPSTASH_REDIS_REST_URL="",
               UPSTASH_REDIS_REST_TOKEN="")
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py", "--server.headless", "true",
         "--server.port", str(PORT), "--browser.gatherUsageStats", "false"],
        cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(60):
        try:
            if urllib.request.urlopen(BASE + "_stcore/health", timeout=2).status == 200:
                return proc
        except Exception:
            time.sleep(1)
    proc.kill()
    raise RuntimeError("app did not start")


class Visit:
    """A browser context that records Google requests and aborts GA collect hits."""

    def __init__(self, browser, webdriver=False):
        self.google, self.collect = [], []
        self.ctx = browser.new_context(user_agent=UA)
        if webdriver:
            self.ctx.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => true});")
        self.ctx.on("request", self._on_request)
        self.ctx.route(re.compile(r".*/g/collect.*"), lambda route: route.abort())
        self.page = self.ctx.new_page()

    def _on_request(self, req):
        if GOOGLE.search(req.url):
            self.google.append(req.url)
            if "/g/collect" in req.url:
                self.collect.append(req.url)

    def open(self, url=BASE):
        self.page.goto(url)
        self.page.wait_for_selector("#cordis-cookie-settings", state="attached", timeout=90000)

    def ga_cookies(self):
        return [c["name"] for c in self.ctx.cookies() if c["name"].startswith("_ga")]

    def consent(self):
        return self.page.evaluate("localStorage.getItem('cordis_ga_consent')")


def main():
    proc = start_app()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(args=["--disable-blink-features=AutomationControlled"])

            # (a) first visit
            v = Visit(browser)
            v.open(BASE + "?email=test%40example.com&q=secret&utm_source=newsletter#frag")
            banner = v.page.locator("#cordis-consent-banner")
            webdriver = v.page.evaluate("navigator.webdriver")
            v.page.wait_for_timeout(4000)
            check("(a) first visit: banner shown, no Google requests, no _ga",
                  banner.is_visible() and not v.google and not v.ga_cookies()
                  and webdriver is False,
                  f"banner={banner.is_visible()}, google={len(v.google)}, "
                  f"_ga={v.ga_cookies()}, webdriver={webdriver}")

            # Buttons equally prominent (same computed style)
            styles = [v.page.locator(f"#cordis-consent-banner button:text-is('{t}')").evaluate(
                "b => { const s = getComputedStyle(b); return [s.backgroundColor, s.color, "
                "s.fontWeight, s.padding, s.borderRadius].join('|'); }") for t in ("Accept", "Decline")]
            check("    Accept/Decline identical styling", styles[0] == styles[1], styles[0])

            # (b) Accept
            v.page.click("#cordis-consent-banner button:text-is('Accept')")
            v.page.wait_for_function(
                "document.cookie.split('; ').some(c => c.startsWith('_ga='))", timeout=20000)
            for _ in range(30):
                if v.collect:
                    break
                v.page.wait_for_timeout(500)
            gtag = any("googletagmanager.com/gtag/js" in u for u in v.google)
            dl = parse_qs(urlparse(v.collect[0]).query).get("dl", [""])[0] if v.collect else ""
            cfg = v.page.evaluate(
                "(() => { for (const e of window.dataLayer || []) "
                "if (e[0] === 'config') return e[2].page_location; })()")
            expected = BASE + "?utm_source=newsletter"
            check("(b) Accept: gtag loaded, _ga set, clean page_location",
                  gtag and v.ga_cookies() and cfg == expected and (not v.collect or dl == expected),
                  f"gtag={gtag}, _ga={v.ga_cookies()}, page_location={cfg}, "
                  f"collect dl={dl or '(no hit captured)'}, stored={v.consent()}")

            # (f) rerun: press "r" (Streamlit rerun hotkey), then count injected scripts
            v.page.locator("body").press("r")
            v.page.wait_for_timeout(4000)
            counts = v.page.evaluate(
                "[document.querySelectorAll('#ga-script-tag').length, "
                "document.querySelectorAll('#cordis-consent-script').length, "
                "document.querySelectorAll('#cordis-cookie-settings').length]")
            check("(f) rerun: GA / consent script / link injected once", counts == [1, 1, 1],
                  f"gtag, consent script, settings link = {counts}")

            # (e) Cookie settings: withdraw
            v.page.click("#cordis-cookie-settings")
            v.page.wait_for_timeout(1000)
            check("(e) Cookie settings: _ga deleted, choice cleared, banner back",
                  not v.ga_cookies() and v.consent() is None and banner.is_visible(),
                  f"_ga={v.ga_cookies()}, stored={v.consent()}, banner={banner.is_visible()}")
            v.ctx.close()

            # (c) Decline + reload (with a _ga cookie left over from the old always-on GA)
            v = Visit(browser)
            v.ctx.add_cookies([{"name": "_ga", "value": "GA1.1.123.456", "url": BASE}])
            v.open()
            v.page.click("#cordis-consent-banner button:text-is('Decline')")
            v.page.reload()
            v.page.wait_for_selector("#cordis-cookie-settings", state="attached", timeout=90000)
            v.page.wait_for_timeout(4000)
            shown = v.page.locator("#cordis-consent-banner").count()
            check("(c) Decline + reload: no Google requests, old _ga deleted, banner hidden",
                  not v.google and not v.ga_cookies() and v.consent() == "denied" and shown == 0,
                  f"google={len(v.google)}, _ga={v.ga_cookies()}, stored={v.consent()}, banner={shown}")
            v.ctx.close()

            # Close (x) counts as Decline
            v = Visit(browser)
            v.open()
            v.page.click("#cordis-consent-banner button[aria-label='Close (decline)']")
            check("    Close (x) = Decline", v.consent() == "denied" and not v.google,
                  f"stored={v.consent()}")
            v.ctx.close()

            # (d) webdriver = true, Accept
            v = Visit(browser, webdriver=True)
            v.open()
            v.page.click("#cordis-consent-banner button:text-is('Accept')")
            v.page.wait_for_timeout(6000)
            wd = v.page.evaluate("navigator.webdriver")
            check("(d) webdriver=true + Accept: no Google requests",
                  wd is True and not v.google and not v.ga_cookies(),
                  f"webdriver={wd}, google={len(v.google)}, _ga={v.ga_cookies()}")
            v.ctx.close()

            # Keep-awake job's browser: default headless launch, as in wake_app.py
            bot = p.chromium.launch()
            pg = bot.new_page()
            hits = []
            pg.on("request", lambda r: GOOGLE.search(r.url) and hits.append(r.url))
            pg.goto(BASE)
            pg.wait_for_selector("#cordis-cookie-settings", state="attached", timeout=90000)
            ua, wd = pg.evaluate("[navigator.userAgent, navigator.webdriver]")
            pg.evaluate("localStorage.setItem('cordis_ga_consent', 'granted')")  # even if granted
            pg.reload()
            pg.wait_for_selector("#cordis-cookie-settings", state="attached", timeout=90000)
            pg.wait_for_timeout(4000)
            rules = [r for r, hit in (("navigator.webdriver", wd is True),
                                      ("HeadlessChrome UA", "HeadlessChrome" in ua)) if hit]
            check("    keep-awake browser excluded (even with consent stored)",
                  not hits and bool(rules), f"caught by: {', '.join(rules)}")
            bot.close()
            browser.close()
    finally:
        proc.kill()

    width = max(len(n) for n, _, _ in results)
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name.ljust(width)}  {detail}")
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)


if __name__ == "__main__":
    main()
