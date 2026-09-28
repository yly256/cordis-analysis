# Security & privacy rules — CORDIS Analytics

Baseline set by the security audit of 2026-09-28 (tag `security-audit-2026-09-28`).
Tests in `tests/` enforce most of these rules; keep them passing.

## Visitor SQL
- Visitor SQL (SQL tab), AI-generated SQL and replayed history SQL run **only** through
  `sql_safety.run_user_query()`: locked read-only DuckDB connection (no file/network access,
  no extension loading, `lock_configuration`), single-`SELECT` guard, 15 s timeout,
  10,000-row cap.
- Open DuckDB only via `sql_safety.connect_readonly()`. Never open another DuckDB connection
  or pass visitor/AI/history SQL to `con.execute()`; `con.execute()` is for SQL written in
  `app.py` only.
- Never change `_HARDENED_CONFIG` without a reboot (see Deploys).

## Ask Claude limits
Constants at the top of `app.py`:
- Global daily cap (`AI_DAILY_QUESTION_CAP`, 200/UTC day) in Upstash Redis
  (`cordis_ai_questions:<date>`, 48 h expiry), counted **before** any API call.
- Per-session limit (`AI_SESSION_QUESTION_LIMIT`, 10) and cooldown (`AI_COOLDOWN_S`, 5 s).
  If Redis is unreachable, only the session limit applies.
- Questions over `AI_MAX_QUESTION_CHARS` (500) are rejected; every API call sets `max_tokens`;
  at most one SQL correction retry.
- Only aggregate counts are stored — no IPs or user identifiers. The "queries run" counter adds
  at most one increment per distinct query and 30 per session.

## Google Analytics (`ga_consent.js`)
- Nothing loads from Google until the visitor clicks **Accept**; the choice is stored in
  `localStorage` (`cordis_ga_consent`). Decline, close and "Cookie settings" withdrawal delete
  `_ga` cookies.
- Consent Mode v2 defaults all denied; only `analytics_storage` is granted after Accept;
  `ad_storage`, `ad_user_data`, `ad_personalization` stay denied.
- `page_location` keeps only `utm_*` parameters (no hash); `page_referrer` is the referrer's
  origin only.
- GA never loads for automated browsers (`navigator.webdriver`, Headless/bot user agents).
- Visitor notes must stay accurate: shared query history is visible to all visitors; SQL-tab
  queries are not saved.

## Errors and logs
- Visitors see generic messages only (startup: "temporarily unavailable"; queries: one line,
  no query echo); `client.showErrorDetails = "none"` in `.streamlit/config.toml` and `app.py`.
- Full details go to the server log (`cordis` logger). Never log secret values; Redis errors are
  logged with the Upstash URL, host and token redacted.

## Deploys
- After changing DuckDB connection settings or `.streamlit/config.toml`, **reboot the app** in
  Streamlit Cloud (Manage app → ⋮ → Reboot). Streamlit Cloud hot-reloads code in the running
  process. A reboot clears the shared query history.
- `app.py` reloads `sql_safety` when its file changes, so other code pushes need no reboot.
- After a deploy, open the live app and check it loads.
- Run the suite before pushing: `python -m unittest discover -s tests`.
  Browser check for GA consent: `python tests/ga_browser_check.py`.

## Secrets
- `ANTHROPIC_API_KEY`, `UPSTASH_REDIS_REST_URL`/`_TOKEN` and `GA_MEASUREMENT_ID` live only in
  Streamlit Cloud secrets and the local `.env` (git-ignored, as is `.streamlit/secrets.toml`).
  Never commit, print or log them.
- If a secret may have been exposed, rotate it, revoke the old one, update Streamlit secrets,
  and reboot.
