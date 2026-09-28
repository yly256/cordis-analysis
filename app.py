"""
CORDIS Analytics Dashboard
Run: streamlit run app.py
"""

import os
import re
import json
import sqlite3
import hashlib
import time
import tempfile
import base64
from datetime import datetime, timezone
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import urllib.request
from pathlib import Path
import anthropic
import importlib
import sql_safety
# Streamlit Cloud hot-reloads app.py but keeps stale local modules in sys.modules: reload
# sql_safety only when its file changed since it was loaded (i.e. after a deploy), so its
# exception classes aren't redefined on every run. Cached connections are unaffected.
if getattr(sql_safety, "_LOADED_MTIME", None) != os.path.getmtime(sql_safety.__file__):
    importlib.reload(sql_safety)
from sql_safety import (
    connect_readonly, run_user_query, UnsafeQueryError, QueryTimeoutError, MAX_ROWS,
    query_error_message, log,
)
from urllib.parse import urlparse
from dotenv import load_dotenv
import streamlit.components.v1 as _components

load_dotenv()

print("[BOOT] imports OK")

_APP_DIR  = Path(__file__).parent
DB_PATH   = str(_APP_DIR / "cordis.duckdb")
DB_URL    = "https://github.com/yly256/cordis-analysis/releases/download/v1.0/cordis.duckdb"
HISTORY_DB = str(Path(tempfile.gettempdir()) / "query_history.db")

# ── Ask Claude cost limits (each question ≈ 4 API calls) ──────────────────────
AI_DAILY_QUESTION_CAP     = 200   # global, per UTC day (Upstash Redis) — the real limit
AI_SESSION_QUESTION_LIMIT = 10    # per browser session — a speed bump (reload resets it)
AI_COOLDOWN_S             = 5     # minimum seconds between questions in a session
AI_MAX_QUESTION_CHARS     = 500

_logo_file = _APP_DIR / "orientos_logo.png"
_LOGO_SRC  = (
    "data:image/png;base64," + base64.b64encode(_logo_file.read_bytes()).decode()
    if _logo_file.exists() else ""
)

print(f"[BOOT] DB_PATH   = {DB_PATH}  exists={Path(DB_PATH).exists()}")
print(f"[BOOT] HISTORY_DB= {HISTORY_DB}")

_UNAVAILABLE_MSG = "The app is temporarily unavailable, please try again later."
_AI_UNAVAILABLE_MSG = "The AI assistant is temporarily unavailable, please try again later."

def _get_ai_client():
    key = os.getenv("ANTHROPIC_API_KEY", "")
    if not key:
        try:
            key = st.secrets["ANTHROPIC_API_KEY"]
        except Exception as _e:
            log.error("Could not read ANTHROPIC_API_KEY from st.secrets: %s", type(_e).__name__)
            st.error(_AI_UNAVAILABLE_MSG)
            st.stop()
    if not key:
        log.error("ANTHROPIC_API_KEY is empty (check Streamlit Cloud → Settings → Secrets)")
        st.error(_AI_UNAVAILABLE_MSG)
        st.stop()
    return anthropic.Anthropic(api_key=key)

st.set_page_config(
    page_title="CORDIS Analytics",
    page_icon="🇪🇺",
    layout="wide",
)

# Uncaught exceptions: generic message in the browser, details only in the server log
st.set_option("client.showErrorDetails", "none")

def _get_secret(name: str) -> str:
    val = os.getenv(name, "")
    if not val:
        try:
            val = st.secrets[name]
        except Exception:
            val = ""
    return val

# ── Anonymous, aggregate-only usage counter (Upstash Redis) ─────────────────
# Tracks a single global integer — no per-user, session, or IP data is ever
# read or stored here.
_QUERIES_RUN_KEY  = "cordis_analytics_queries_run"
_QUERIES_RUN_SEED = 122

@st.cache_resource
def _get_counter_redis():
    url = _get_secret("UPSTASH_REDIS_REST_URL")
    token = _get_secret("UPSTASH_REDIS_REST_TOKEN")
    if not url or not token:
        return None
    from upstash_redis import Redis
    return Redis(url=url, token=token)

def _log_redis_error(what: str, exc: Exception):
    """Log type + message only, with the Upstash URL, host and token redacted."""
    msg = str(exc)
    url = _get_secret("UPSTASH_REDIS_REST_URL")
    token = _get_secret("UPSTASH_REDIS_REST_TOKEN")
    for s in (url, token, urlparse(url).hostname if url else ""):
        if s:
            msg = msg.replace(s, "[redacted]")
    log.error("%s failed: %s: %s", what, type(exc).__name__, msg)

def _log_counter_error(action: str, exc: Exception):
    _log_redis_error(f"Queries-run counter {action}", exc)

def get_queries_run_count():
    try:
        r = _get_counter_redis()
        if r is None:
            return None
        # Atomic "set if not exists" so redeploys never reset an existing count.
        r.setnx(_QUERIES_RUN_KEY, _QUERIES_RUN_SEED)
        return int(r.get(_QUERIES_RUN_KEY))
    except Exception as e:
        _log_counter_error("read", e)
        return None

QUERIES_RUN_SESSION_CAP = 30  # most increments one browser session can add

def increment_queries_run(sql: str):
    """Call exactly once per actual query execution (a Run/Ask button
    succeeding), never on tab switches or filter changes.

    Anti-inflation: each distinct query counts at most once per session, and a
    session adds at most QUERIES_RUN_SESSION_CAP. Only query hashes are kept in
    the session — no IPs or identifiers.
    """
    counted = st.session_state.setdefault("_counted_query_hashes", set())
    h = _sql_hash(sql)
    if h in counted or len(counted) >= QUERIES_RUN_SESSION_CAP:
        return None
    counted.add(h)
    try:
        r = _get_counter_redis()
        if r is None:
            return None
        r.setnx(_QUERIES_RUN_KEY, _QUERIES_RUN_SEED)
        return r.incr(_QUERIES_RUN_KEY)
    except Exception as e:
        _log_counter_error("increment", e)
        return None

# ── Ask Claude limits: global daily cap (Redis) + per-session limit and cooldown ─
# Only aggregate counts are stored — no IPs or user identifiers.
_AI_DAILY_KEY_PREFIX = "cordis_ai_questions"
_AI_DAILY_CAP_MSG = ("The AI assistant has reached today's limit, please try again tomorrow. "
                     "The SQL tab is still available.")

def _ai_daily_increment():
    """INCR today's (UTC) global question count. Returns the new count, or None if
    Redis is unavailable (the caller then relies on the per-session limit only)."""
    try:
        r = _get_counter_redis()
        if r is None:
            return None
        key = f"{_AI_DAILY_KEY_PREFIX}:{datetime.now(timezone.utc):%Y-%m-%d}"
        n = int(r.incr(key))
        if n == 1:
            r.expire(key, 48 * 3600)
        return n
    except Exception as e:
        _log_redis_error("AI daily cap", e)
        return None

def _ai_admit_question(question: str):
    """Apply the Ask Claude limits. Returns a message if the question is refused, else None.

    Counts are taken here, before any API call, so failed or aborted calls still count.
    """
    if len(question) > AI_MAX_QUESTION_CHARS:
        return f"Please keep questions under {AI_MAX_QUESTION_CHARS} characters."
    used = st.session_state.get("_ai_questions_used", 0)
    if used >= AI_SESSION_QUESTION_LIMIT:
        return (f"You've used all {AI_SESSION_QUESTION_LIMIT} questions for this session. "
                "The SQL tab is still available.")
    now = time.monotonic()
    last = st.session_state.get("_ai_last_question_at")
    if last is not None and now - last < AI_COOLDOWN_S:
        return f"Please wait {AI_COOLDOWN_S} seconds between questions."
    n = _ai_daily_increment()
    if n is not None and n > AI_DAILY_QUESTION_CAP:
        return _AI_DAILY_CAP_MSG
    st.session_state["_ai_questions_used"] = used + 1
    st.session_state["_ai_last_question_at"] = now
    return None

# ── Analytics: consent banner + GA only after Accept (logic in ga_consent.js) ──
_GA_ID_RE = re.compile(r"G-[A-Z0-9]{4,20}")

def _ga_consent_html(ga_id: str) -> str:
    """components.html payload that injects ga_consent.js into the parent (Streamlit)
    document once per page load; the script's own flag stops duplicates on reruns."""
    js = (_APP_DIR / "ga_consent.js").read_text(encoding="utf-8")
    js = js.replace("__GA_ID__", json.dumps(ga_id))
    payload = json.dumps(js).replace("</", "<\\/")
    return (
        "<script>(function(){var p=window.parent;if(p.__cordisConsentInit)return;"
        "var s=p.document.createElement('script');s.id='cordis-consent-script';"
        f"s.textContent={payload};p.document.head.appendChild(s);}})();</script>"
    )

# First script run of each session (i.e. each page load), not on every rerun
if "_analytics_sent" not in st.session_state:
    st.session_state._analytics_sent = True
    _ga_id = _get_secret("GA_MEASUREMENT_ID")
    if _ga_id and _GA_ID_RE.fullmatch(_ga_id):
        _components.html(_ga_consent_html(_ga_id), height=0, width=0)
    elif _ga_id:
        log.warning("GA_MEASUREMENT_ID has an unexpected format; analytics disabled")

st.markdown(f"""
<style>
/* ── Blue banner ─────────────────────────────────────────────────────────── */
.orientos-banner {{
    background: linear-gradient(135deg, #003399 0%, #1a56db 100%);
    padding: 20px 32px;
    text-align: center;
    border-radius: 8px;
    margin-bottom: 1.2rem;
}}
/* ── EU blue headers ─────────────────────────────────────────────────────── */
h1, h2, h3 {{ color: #003399 !important; }}
/* ── Blue buttons (primary + form submit) ───────────────────────────────── */
.stButton > button,
.stFormSubmitButton > button {{
    background-color: #003399 !important;
    color: white !important;
    border: 1px solid #002277 !important;
    border-radius: 6px !important;
}}
.stButton > button:hover,
.stFormSubmitButton > button:hover {{
    background-color: #0044cc !important;
    border-color: #0044cc !important;
    color: white !important;
}}
.stButton > button:active,
.stFormSubmitButton > button:active {{
    background-color: #002277 !important;
}}
</style>
<div class="orientos-banner">
  <img src="{_LOGO_SRC}" style="height:60px; max-width:320px;">
</div>
""", unsafe_allow_html=True)

_col_title, _col_fb = st.columns([9, 1])
with _col_title:
    st.title("🇪🇺 CORDIS Project Analytics")
    st.caption("FP7 · H2020 · Horizon Europe — unified database")
    _queries_run = get_queries_run_count()
    if _queries_run is not None:
        st.caption(f"{_queries_run:,} queries run — anonymous, aggregate count only.")
with _col_fb:
    st.markdown(
        "<div style='display:flex;justify-content:flex-end;align-items:center;height:100%;padding-top:1.2rem;'>"
        "<a href='https://www.orientos.com/feedback-form-cordis-analysis' target='_blank' rel='noopener noreferrer' "
        "style='background:#003399;color:white;padding:8px 18px;border-radius:6px;"
        "text-decoration:none;font-weight:600;font-size:0.88em;white-space:nowrap;'>"
        "📝 Feedback</a></div>",
        unsafe_allow_html=True,
    )

print("[BOOT] page config OK")

try:
    if not Path(DB_PATH).exists():
        print("[BOOT] cordis.duckdb missing — downloading…")
        with st.spinner("Downloading database (first run, ~145 MB)…"):
            urllib.request.urlretrieve(DB_URL, DB_PATH)
        print("[BOOT] download complete")

    @st.cache_resource
    def get_con():
        print(f"[DB] opening cordis.duckdb read-only from {DB_PATH}")
        return connect_readonly(DB_PATH)

    @st.cache_resource
    def get_hcon():
        print(f"[DB] opening history sqlite3 at {HISTORY_DB}")
        conn = sqlite3.connect(HISTORY_DB, check_same_thread=False)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS query_log (
                id           INTEGER PRIMARY KEY,
                description  TEXT,
                question     TEXT,
                sql_hash     TEXT,
                sql_text     TEXT,
                summary      TEXT,
                run_count    INTEGER DEFAULT 1,
                first_run_at TEXT,
                last_run_at  TEXT
            )
        """)
        conn.commit()
        print("[DB] history db ready")
        return conn

    print("[BOOT] connecting to cordis.duckdb…")
    con = get_con()
    print("[BOOT] cordis.duckdb connected")

    print("[BOOT] connecting to history db…")
    try:
        hcon = get_hcon()
        print("[BOOT] history db connected")
    except Exception:
        log.warning("History DB unavailable", exc_info=True)
        st.warning("Query history is unavailable right now.")
        hcon = None

except Exception:
    log.exception("Startup failed")
    st.error(_UNAVAILABLE_MSG)
    st.stop()

# ── Sidebar filters ────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    fps = con.execute(
        "SELECT DISTINCT FP FROM projects ORDER BY FP"
    ).df()["FP"].tolist()
    sel_fp = st.multiselect("Framework Programme", fps, default=fps)

    statuses = con.execute(
        "SELECT DISTINCT status FROM projects WHERE status IS NOT NULL AND status != '' ORDER BY 1"
    ).df()["status"].tolist()
    sel_status = st.multiselect("Status", statuses, default=statuses)

    year_min, year_max = con.execute(
        "SELECT MIN(YEAR(startDate)), MAX(YEAR(startDate)) FROM projects WHERE startDate IS NOT NULL"
    ).fetchone()
    year_range = st.slider(
        "Start Year", int(year_min or 2000), int(year_max or 2026),
        (int(year_min or 2007), int(year_max or 2026))
    )

    st.divider()
    st.subheader("Scheme filter (optional)")
    schemes = con.execute(
        "SELECT DISTINCT fundingScheme FROM projects WHERE fundingScheme IS NOT NULL ORDER BY 1"
    ).df()["fundingScheme"].tolist()
    sel_scheme = st.multiselect("Funding Scheme", schemes, default=[])

    st.divider()
    st.markdown(
        "<p style='font-size:0.78em;color:#555;margin-bottom:2px;'><b>CORDIS data as of</b></p>"
        "<p style='font-size:0.75em;color:#777;margin:0;'>FP7: Dec 2018 · H2020: Jan 2022 · HEU: Jun 2023</p>",
        unsafe_allow_html=True,
    )

# ── WHERE clause builder ───────────────────────────────────────────────────────
def W():
    clauses = []
    if sel_fp:
        fp_list = ",".join(f"'{x}'" for x in sel_fp)
        clauses.append(f"FP IN ({fp_list})")
    if sel_status:
        st_list = ",".join(f"'{x}'" for x in sel_status)
        clauses.append(f"status IN ({st_list})")
    if sel_scheme:
        sc_list = ",".join(f"'{x}'" for x in sel_scheme)
        clauses.append(f"fundingScheme IN ({sc_list})")
    clauses.append(f"YEAR(startDate) BETWEEN {year_range[0]} AND {year_range[1]}")
    return " AND ".join(clauses) if clauses else "1=1"

# ── AI Query helpers ───────────────────────────────────────────────────────────
@st.cache_resource
def _build_schema_context():
    """Read actual column names from DuckDB so the prompt is always accurate."""
    tables = ["projects", "organizations", "topics", "legal_basis", "euro_sci_voc", "policy_priorities"]
    lines = ["Tables in the CORDIS DuckDB database:\n"]
    for t in tables:
        try:
            cols = con.execute(f"DESCRIBE {t}").df()
            col_list = ", ".join(
                f"{r['column_name']} ({r['column_type']})" for _, r in cols.iterrows()
            )
            lines.append(f"  {t}: {col_list}")
        except Exception:
            pass
    return "\n".join(lines)

_INJECTION_PATTERNS = [
    # SQL mutations
    r"\b(drop|delete|insert|update|truncate|alter|create|replace)\b",
    # Prompt injection
    r"ignore (previous|all|your) (instructions?|rules?|prompt)",
    r"you are now",
    r"forget (everything|all|your)",
    r"new (role|persona|instructions?)",
    r"system\s*prompt",
    r"disregard",
]

def _check_relevance(question: str) -> dict:
    """Returns {"relevant": bool, "reason": str}. Uses regex — no LLM call."""
    q = question.strip().lower()
    if len(q) < 3:
        return {"relevant": False, "reason": "Question is too short."}
    for pattern in _INJECTION_PATTERNS:
        if re.search(pattern, q):
            return {"relevant": False, "reason": "Input contains disallowed patterns."}
    return {"relevant": True, "reason": "ok"}


_SQL_SYSTEM = (
    "You are a DuckDB SQL expert for a CORDIS EU research-funding database.\n"
    "Schema:\n{schema}\n"
    "Active sidebar filters (MUST be applied to the projects table): {where_clause}\n"
    "Rules:\n"
    "- Return ONLY the raw SQL query — no markdown fences, no explanation.\n"
    "- Always apply the filter above. If you use a table alias for projects (e.g. FROM projects p), "
    "qualify every filter column with that alias (e.g. p.FP, p.status, p.startDate).\n"
    "- Use ROUND(x/1e6, 2) for EUR millions. Limit to 100 rows unless asked otherwise.\n"
    "- Use YEAR(startDate) for year extraction. SELECT only — no mutations.\n"
    "- JOIN KEYS: the project key is projects.id (there is NO projects.projectID column). "
    "organizations, topics, legal_basis, euro_sci_voc and policy_priorities each have a projectID "
    "column that references projects.id — always join as <table>.projectID = p.id.\n"
    "- COUNTRY PARTICIPATION: when counting projects per country, always count ALL projects "
    "where that country appears in ANY role (coordinator or participant). Do this by joining "
    "the organizations table and counting DISTINCT project ids, e.g.: "
    "SELECT o.country, COUNT(DISTINCT p.id) AS projects, ROUND(SUM(o.ecContribution)/1e6,2) AS eu_contribution_M "
    "FROM projects p JOIN organizations o ON o.projectID = p.id "
    "WHERE <filters on p> AND o.country IS NOT NULL "
    "GROUP BY o.country ORDER BY projects DESC. "
    "Only use coordinator_country when the user explicitly asks about coordinators only.\n"
    "- NO DOUBLE COUNTING: projects has one row per project but organizations has one row per "
    "participant, so after joining organizations NEVER SUM/AVG project-level columns "
    "(p.totalCost, p.ecMaxContribution) — each project would be counted once per partner. "
    "For funding per country/organisation use the participant-level o.ecContribution "
    "(EU contribution to that participant); for counts use COUNT(DISTINCT p.id).\n"
    "- NULL COUNTRIES: always exclude rows where the country/coordinator_country column IS NULL "
    "by adding the appropriate IS NOT NULL filter.\n"
    "- TOP COORDINATORS: when ranking coordinators, always GROUP BY coordinator_name only (not by country). "
    "Use ANY_VALUE(coordinator_country) AS coordinator_country if you want to show country alongside name. "
    "Grouping by both coordinator_name and coordinator_country splits counts and distorts rankings."
)

def _generate_sql(question: str, where_clause: str) -> str:
    resp = _get_ai_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=600,
        system=_SQL_SYSTEM.format(schema=_build_schema_context(), where_clause=where_clause),
        messages=[{"role": "user", "content": question}],
    )
    return resp.content[0].text.strip()


def _fix_sql(question: str, bad_sql: str, error: str, where_clause: str) -> str:
    resp = _get_ai_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=600,
        system=_SQL_SYSTEM.format(schema=_build_schema_context(), where_clause=where_clause),
        messages=[
            {"role": "user", "content": question},
            {"role": "assistant", "content": bad_sql},
            {"role": "user", "content": (
                f"That query failed with: {error}\n"
                "Please fix it and return only the corrected SQL."
            )},
        ],
    )
    return resp.content[0].text.strip()


def _summarize(question: str, df) -> str:
    sample = df.head(15).to_string(index=False)
    resp = _get_ai_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=250,
        system=(
            "You are a research-funding analyst. Write a concise 2-3 sentence summary "
            "of the query results. Be specific with numbers and country/scheme names."
        ),
        messages=[{"role": "user", "content": (
            f"Question: {question}\n\n"
            f"Results ({len(df)} rows, showing up to 15):\n{sample}"
        )}],
    )
    return resp.content[0].text.strip()


def _distill_description(question: str) -> str:
    resp = _get_ai_client().messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=20,
        system=(
            "Summarize this EU research data question in 5–7 words. "
            "No punctuation at the end. "
            "Never include value judgements such as high, low, good, bad, strong, weak — "
            "describe only what is being measured, not the result."
        ),
        messages=[{"role": "user", "content": question}],
    )
    return resp.content[0].text.strip()


def _sql_hash(sql: str) -> str:
    normalized = re.sub(r"\s+", " ", sql.strip().lower().rstrip(";"))
    return hashlib.sha256(normalized.encode()).hexdigest()[:12]


def _save_query(description: str, question: str, sql_hash: str, sql_text: str, summary: str):
    if hcon is None:
        return
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    existing = pd.read_sql_query(
        "SELECT id, run_count FROM query_log WHERE sql_hash = ?",
        hcon, params=(sql_hash,)
    )
    if not existing.empty:
        row_id = int(existing.iloc[0]["id"])
        new_count = int(existing.iloc[0]["run_count"]) + 1
        hcon.execute(
            "UPDATE query_log SET run_count=?, last_run_at=?, summary=? WHERE id=?",
            (new_count, now, summary, row_id),
        )
    else:
        new_id = hcon.execute("SELECT COALESCE(MAX(id),0)+1 FROM query_log").fetchone()[0]
        hcon.execute(
            "INSERT INTO query_log VALUES (?,?,?,?,?,?,1,?,?)",
            (new_id, description, question, sql_hash, sql_text, summary, now, now),
        )
    hcon.commit()


def _render_query_table(rows_df: pd.DataFrame, key_prefix: str, height: int = 320):
    """Compact scrollable table + selectbox to pick, run, and display a query.

    Runs entirely inside an st.fragment so clicking ▶ Run only reruns this widget,
    not the whole app — a full-script rerun triggered by a button click resets
    st.tabs back to Overview.
    """
    if rows_df.empty:
        st.caption("No queries recorded yet.")
        return

    disp = pd.DataFrame({
        "#":           range(1, len(rows_df) + 1),
        "Description": rows_df["description"].str[:35],
        "Question":    rows_df["question"].str[:60],
        "Runs":        rows_df["run_count"].astype(int),
        "Last run":    rows_df["last_run_at"].str[:10],
    })

    @st.fragment
    def _fragment():
        st.dataframe(disp, width="stretch", hide_index=True, height=height)

        options = [f"{i+1}. {row['description']}" for i, (_, row) in enumerate(rows_df.iterrows())]
        with st.form(key=f"{key_prefix}_form"):
            fc1, fc2 = st.columns([5, 1])
            sel_idx = fc1.selectbox(
                "Select", range(len(options)),
                format_func=lambda i: options[i],
                label_visibility="collapsed",
                key=f"{key_prefix}_sel",
            )
            submitted = fc2.form_submit_button("▶ Run")

        if submitted:
            sel = rows_df.iloc[sel_idx].to_dict()
            with st.spinner("Running query…"):
                try:
                    r, _trunc = run_user_query(con, sel["sql_text"])
                    increment_queries_run(sel["sql_text"])
                    st.success(f"{len(r):,} rows returned" + (f" (capped at {MAX_ROWS:,})" if _trunc else ""))
                    st.dataframe(r, width="stretch", hide_index=True)
                    st.download_button("⬇ Download CSV", r.to_csv(index=False),
                                       f"{key_prefix}_result.csv", "text/csv")
                    if sel.get("summary"):
                        st.info(sel["summary"])
                    _save_query(sel["description"], sel["question"],
                                sel["sql_hash"], sel["sql_text"], sel.get("summary", ""))
                except (UnsafeQueryError, QueryTimeoutError) as e:
                    st.info(f"The cached query couldn't run. {e}")
                except Exception as e:
                    st.info(f"The cached query couldn't run. {query_error_message(e)}")

    _fragment()


# ── Tabs ───────────────────────────────────────────────────────────────────────
if "run_count" not in st.session_state:
    st.session_state.run_count = 0
st.session_state.run_count += 1

_badge_visible = "block" if st.session_state.run_count == 1 else "none"
st.markdown(f"""
<style>
@keyframes cordis-bounce {{
  0%, 100% {{ transform: translateY(0); }}
  50%       {{ transform: translateY(5px); }}
}}
.start-here-arrow {{
  display: inline-block;
  animation: cordis-bounce 1.2s ease-in-out infinite;
}}
</style>
<div style="display:{_badge_visible};margin-bottom:4px;">
  <span style="background:linear-gradient(135deg,#003399,#0066cc);
               color:white;padding:5px 18px;border-radius:20px;
               font-weight:600;font-size:.9em;letter-spacing:.02em;">
    <span class="start-here-arrow">&#x1F447;</span>&nbsp; Choose a view to start exploring
  </span>
</div>
""", unsafe_allow_html=True)

_PRIVACY_NOTE_HTML = (
    "<p style='font-size:0.82em;color:#555;margin-top:0;'>"
    "Questions and queries you run are shown to all visitors in the Query History tab. "
    "Don't enter personal or confidential information. History is cleared from time to time. "
    "This site uses Google Analytics only if you accept cookies.</p>"
)
# SQL-tab queries are never written to the history log
_SQL_NOTE_HTML = (
    "<p style='font-size:0.82em;color:#555;margin-top:0;'>"
    "Queries you run here are not saved. This site uses Google Analytics only if you accept cookies.</p>"
)

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Overview", "🔬 Deep Dive", "🌍 Geography",
    "💻 SQL", "🤖 AI Query", "📋 Query History / Log",
])

# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    kpis = con.execute(f"""
        SELECT
            COUNT(*)                             AS total_projects,
            ROUND(AVG(totalCost)/1e6, 2)         AS avg_budget_m,
            ROUND(MEDIAN(totalCost)/1e6, 2)      AS median_budget_m,
            ROUND(AVG(partner_count), 1)          AS avg_partners,
            ROUND(AVG(duration_months), 1)        AS avg_duration,
            ROUND(AVG(sme_count), 2)              AS avg_smes,
            ROUND(AVG(country_count), 1)          AS avg_countries,
            SUM(totalCost)/1e9                    AS total_budget_b
        FROM projects WHERE {W()}
    """).df().iloc[0]

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Projects",   f"{int(kpis.total_projects):,}")
    c2.metric("Total Budget",     f"€{kpis.total_budget_b:.1f}B")
    c3.metric("Avg Budget",       f"€{kpis.avg_budget_m}M")
    c4.metric("Median Budget",    f"€{kpis.median_budget_m}M")

    c5,c6,c7,c8 = st.columns(4)
    c5.metric("Avg Partners",     str(kpis.avg_partners))
    c6.metric("Avg Duration",     f"{kpis.avg_duration} mo")
    c7.metric("Avg SMEs",         str(kpis.avg_smes))
    c8.metric("Avg Countries",    str(kpis.avg_countries))

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Projects per FP")
        df = con.execute(f"""
            SELECT FP, COUNT(*) AS projects,
                   ROUND(AVG(totalCost)/1e6,2) AS avg_budget_M
            FROM projects WHERE {W()} GROUP BY FP ORDER BY FP
        """).df()
        st.plotly_chart(
            px.bar(df, x="FP", y="projects", color="FP",
                   text="projects", title="Project Count by FP"),
            width="stretch"
        )

    with col2:
        st.subheader("Average Budget by FP (€M)")
        st.plotly_chart(
            px.bar(df, x="FP", y="avg_budget_M", color="FP",
                   text="avg_budget_M", title="Avg Budget (€M) by FP"),
            width="stretch"
        )

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Budget Distribution by FP")
        df2 = con.execute(f"""
            SELECT FP, totalCost FROM projects
            WHERE {W()} AND totalCost > 0
        """).df()
        st.plotly_chart(
            px.box(df2, x="FP", y="totalCost", log_y=True, color="FP",
                   title="Budget Distribution (log scale)"),
            width="stretch"
        )

    with col4:
        st.subheader("Projects Over Time")
        df3 = con.execute(f"""
            SELECT YEAR(startDate) AS year, FP, COUNT(*) AS n
            FROM projects WHERE {W()} AND startDate IS NOT NULL
            GROUP BY 1,2 ORDER BY 1
        """).df()
        st.plotly_chart(
            px.line(df3, x="year", y="n", color="FP",
                    title="Projects Started per Year"),
            width="stretch"
        )

# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Partner Count Distribution")
    col1, col2 = st.columns(2)

    with col1:
        df = con.execute(f"""
            SELECT FP, partner_count FROM projects
            WHERE {W()} AND partner_count > 0 AND partner_count <= 60
        """).df()
        st.plotly_chart(
            px.histogram(df, x="partner_count", color="FP", nbins=40,
                         barmode="overlay", opacity=0.7,
                         title="Partner Count Distribution"),
            width="stretch"
        )

    with col2:
        df2 = con.execute(f"""
            SELECT FP,
                   ROUND(AVG(partner_count),1) AS avg_partners,
                   ROUND(MEDIAN(partner_count),1) AS median_partners,
                   MAX(partner_count) AS max_partners
            FROM projects WHERE {W()} AND partner_count > 0
            GROUP BY FP ORDER BY FP
        """).df()
        st.dataframe(df2, width="stretch", hide_index=True)

    st.divider()
    st.subheader("Top 20 Funding Schemes by Project Count")
    df3 = con.execute(f"""
        SELECT fundingScheme, FP, COUNT(*) AS n,
               ROUND(AVG(totalCost)/1e6,2) AS avg_budget_M
        FROM projects WHERE {W()} AND fundingScheme IS NOT NULL
        GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20
    """).df()
    st.plotly_chart(
        px.bar(df3, x="n", y="fundingScheme", color="FP", orientation="h",
               title="Top Funding Schemes"),
        width="stretch"
    )

    st.divider()
    st.subheader("Large Projects (top 1% by budget)")
    df4 = con.execute(f"""
        SELECT acronym, title, FP, fundingScheme,
               ROUND(totalCost/1e6,2) AS budget_M,
               partner_count, coordinator_country, YEAR(startDate) AS year
        FROM projects
        WHERE {W()} AND totalCost IS NOT NULL
        ORDER BY totalCost DESC
        LIMIT 100
    """).df()
    st.dataframe(df4, width="stretch", hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Projects by Coordinator Country (Top 25)")
        df = con.execute(f"""
            SELECT coordinator_country AS country, COUNT(*) AS projects,
                   ROUND(AVG(totalCost)/1e6,2) AS avg_budget_M
            FROM projects
            WHERE {W()} AND coordinator_country IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 25
        """).df()
        st.plotly_chart(
            px.bar(df, x="projects", y="country", orientation="h",
                   color="avg_budget_M", color_continuous_scale="Blues",
                   title="Coordinator Country Ranking"),
            width="stretch"
        )

    with col2:
        st.subheader("Avg Budget by Coordinator Country (Top 25)")
        st.plotly_chart(
            px.bar(df.sort_values("avg_budget_M", ascending=False).head(25),
                   x="avg_budget_M", y="country", orientation="h",
                   title="Avg Budget (€M) by Coordinator Country"),
            width="stretch"
        )

    st.subheader("Country Participation Map")
    # Sidebar filters apply via the projects subquery (organizations has its own FP column)
    map_df = con.execute(f"""
        SELECT country, COUNT(DISTINCT projectID) AS projects
        FROM organizations
        WHERE country IS NOT NULL
          AND projectID IN (SELECT id FROM projects WHERE {W()})
        GROUP BY 1 ORDER BY 2 DESC
    """).df()
    # Convert ISO alpha-2 → alpha-3 (plotly choropleth requires ISO-3)
    _a2_to_a3 = {
        "AT":"AUT","BE":"BEL","BG":"BGR","CY":"CYP","CZ":"CZE","DE":"DEU",
        "DK":"DNK","EE":"EST","ES":"ESP","FI":"FIN","FR":"FRA","GR":"GRC",
        "HR":"HRV","HU":"HUN","IE":"IRL","IT":"ITA","LT":"LTU","LU":"LUX",
        "LV":"LVA","MT":"MLT","NL":"NLD","PL":"POL","PT":"PRT","RO":"ROU",
        "SE":"SWE","SI":"SVN","SK":"SVK","GB":"GBR","NO":"NOR","CH":"CHE",
        "IS":"ISL","TR":"TUR","IL":"ISR","RS":"SRB","UA":"UKR","ME":"MNE",
        "MK":"MKD","AL":"ALB","BA":"BIH","MD":"MDA","GE":"GEO","AM":"ARM",
        "TN":"TUN","EG":"EGY","MA":"MAR","ZA":"ZAF","CA":"CAN","US":"USA",
        "AU":"AUS","NZ":"NZL","JP":"JPN","KR":"KOR","CN":"CHN","IN":"IND",
        "BR":"BRA","MX":"MEX","AR":"ARG","RU":"RUS",
    }
    map_df["iso3"] = map_df["country"].map(_a2_to_a3)
    map_df = map_df.dropna(subset=["iso3"])
    st.plotly_chart(
        px.choropleth(map_df, locations="iso3", locationmode="ISO-3",
                      color="projects", color_continuous_scale="Blues",
                      scope="europe", title="Organisation Participation by Country"),
        width="stretch"
    )

# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("Ad-hoc SQL Query")
    st.markdown(
        "<p style='font-size:0.95em;color:#003399;font-weight:600;margin-bottom:0.5rem;'>"
        "Tables: <code>projects</code> · <code>organizations</code> · <code>topics</code> · "
        "<code>legal_basis</code> · <code>euro_sci_voc</code> · <code>policy_priorities</code></p>"
        + _SQL_NOTE_HTML,
        unsafe_allow_html=True,
    )

    example_queries = {
        "Avg budget & partners by FP": f"SELECT FP, COUNT(*) AS n, ROUND(AVG(totalCost)/1e6,2) AS avg_M, ROUND(AVG(partner_count),1) AS avg_partners FROM projects WHERE {W()} GROUP BY FP",
        "Top 10 coordinators": f"SELECT coordinator_name, ANY_VALUE(coordinator_country) AS coordinator_country, COUNT(*) AS projects FROM projects WHERE {W()} AND coordinator_name IS NOT NULL GROUP BY coordinator_name ORDER BY projects DESC LIMIT 10",
        "SME participation rate by FP": f"SELECT FP, ROUND(100.0*SUM(CASE WHEN sme_count>0 THEN 1 END)/COUNT(*),1) AS pct_with_sme FROM projects WHERE {W()} GROUP BY FP",
        "Budget by funding scheme (top 20)": f"SELECT fundingScheme, COUNT(*) AS n, ROUND(AVG(totalCost)/1e6,2) AS avg_M FROM projects WHERE {W()} AND fundingScheme IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT 20",
        "Projects > €50M": f"SELECT acronym, FP, ROUND(totalCost/1e6,1) AS budget_M, partner_count, coordinator_country FROM projects WHERE {W()} AND totalCost > 50000000 ORDER BY totalCost DESC",
    }

    sel_example = st.selectbox("Load example query", ["(custom)"] + list(example_queries.keys()))
    default_q = example_queries.get(sel_example, f"SELECT * FROM projects WHERE {W()} LIMIT 10")

    @st.fragment
    def _run_sql_fragment(default_sql):
        """Isolated so clicking Run Query only reruns this block, not the whole app
        (a full rerun triggered by a button click resets st.tabs back to Overview)."""
        with st.form(key="sql_form"):
            q = st.text_area("SQL", value=default_sql, height=100)
            run_clicked = st.form_submit_button("▶ Run Query")

        if run_clicked:
            try:
                result, _trunc = run_user_query(con, q)
                increment_queries_run(q)
                st.success(f"{len(result):,} rows returned" + (f" (capped at {MAX_ROWS:,})" if _trunc else ""))
                st.dataframe(result, width="stretch", hide_index=True)
                csv = result.to_csv(index=False)
                st.download_button("⬇ Download CSV", csv, "result.csv", "text/csv")
            except (UnsafeQueryError, QueryTimeoutError) as e:
                st.error(str(e))
            except Exception as e:
                st.error(query_error_message(e))

    _run_sql_fragment(default_q)

# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader("Ask a Question in Plain English")
    st.markdown(
        "<p style='font-size:0.95em;color:#003399;font-weight:600;margin-bottom:0.2rem;'>"
        "Claude translates your question into SQL, runs it, and summarises the results. "
        "Sidebar filters apply automatically.</p>" + _PRIVACY_NOTE_HTML,
        unsafe_allow_html=True,
    )

    @st.fragment
    def _ai_ask_fragment():
        """Isolated so Ask Claude only reruns this block, not the whole app
        (a full rerun triggered by a button click resets st.tabs back to Overview)."""
        with st.form(key="ai_ask_form"):
            question = st.text_input(
                "Your question",
                placeholder="e.g. Which countries received the most Horizon Europe funding?",
                # No max_chars: Streamlit would silently truncate; _ai_admit_question rejects instead
            )
            ask_clicked = st.form_submit_button("Ask Claude")

        if ask_clicked and question.strip():
            try:
                guard = _check_relevance(question)
                if not guard.get("relevant", False):
                    st.warning(
                        f"That question doesn't seem related to CORDIS data — "
                        f"{guard.get('reason', 'please ask about EU research projects, budgets, or organisations.')} "
                        "Try rephrasing."
                    )
                elif (refusal := _ai_admit_question(question)) is not None:
                    st.warning(refusal)
                else:
                    with st.spinner("Generating SQL…"):
                        sql = _generate_sql(question, W())
                    with st.expander("Generated SQL", expanded=False):
                        st.code(sql, language="sql")

                    result, _trunc = None, False
                    with st.spinner("Running query…"):
                        try:
                            result, _trunc = run_user_query(con, sql)
                        except QueryTimeoutError as e:
                            st.info(f"{e} Try a narrower question.")
                        except Exception as e:
                            log.info("Generated SQL failed, requesting correction: %s", type(e).__name__)
                            with st.spinner("Fixing query…"):
                                sql = _fix_sql(question, sql, str(e), W())
                            with st.expander("Corrected SQL", expanded=False):
                                st.code(sql, language="sql")
                            try:
                                result, _trunc = run_user_query(con, sql)
                            except (UnsafeQueryError, QueryTimeoutError):
                                st.info(
                                    "Sorry, I wasn't able to generate a working query for that question. "
                                    "Try rephrasing, or use the SQL tab for full control."
                                )
                            except Exception as e2:
                                st.info(
                                    "Sorry, I wasn't able to generate a working query for that question. "
                                    "Try rephrasing, or use the SQL tab for full control.\n\n"
                                    f"_{query_error_message(e2)}_"
                                )

                    if result is not None:
                        increment_queries_run(sql)
                        st.success(f"{len(result):,} rows returned" + (f" (capped at {MAX_ROWS:,})" if _trunc else ""))
                        st.dataframe(result, width="stretch", hide_index=True)
                        st.download_button("⬇ Download CSV", result.to_csv(index=False),
                                           "ai_query_result.csv", "text/csv")
                        with st.spinner("Summarising…"):
                            summary = _summarize(question, result)
                        st.info(summary)
                        with st.spinner("Saving to history…"):
                            desc = _distill_description(question)
                            _save_query(desc, question, _sql_hash(sql), sql, summary)
            except anthropic.APIStatusError as _api_err:
                if _api_err.status_code == 529:
                    st.warning("Claude is overloaded right now — please wait a moment and try again.")
                elif _api_err.status_code == 429:
                    st.warning("Rate limit reached — please wait a moment and try again.")
                else:
                    log.error("Anthropic API error: HTTP %s", _api_err.status_code)
                    st.warning(f"AI API error (HTTP {_api_err.status_code}) — please try again shortly.")
            except Exception:
                log.exception("Ask Claude failed")
                st.warning(_AI_UNAVAILABLE_MSG)

        _left = max(0, AI_SESSION_QUESTION_LIMIT - st.session_state.get("_ai_questions_used", 0))
        st.caption(f"{_left} of {AI_SESSION_QUESTION_LIMIT} questions left this session")

    _ai_ask_fragment()

    # ── Last 10 recent queries ───────────────────────────────────────────────
    st.divider()
    st.markdown("**Recent queries** — select and click ▶ Run to replay without an API call")
    if hcon is not None:
        last10 = pd.read_sql_query(
            "SELECT * FROM query_log ORDER BY last_run_at DESC LIMIT 10", hcon
        )
        if last10.empty:
            st.caption("No queries run yet.")
        else:
            _disp = pd.DataFrame({
                "#":           range(1, len(last10) + 1),
                "Description": last10["description"].str[:35],
                "Question":    last10["question"].str[:60],
                "Runs":        last10["run_count"].astype(int),
                "Last run":    last10["last_run_at"].str[:10],
            })
            st.dataframe(_disp, width="stretch", hide_index=True, height=280)
            _opts = [f"{i+1}. {row['description']}" for i, (_, row) in enumerate(last10.iterrows())]

            @st.fragment
            def _ai_recent_fragment():
                """Isolated so clicking Run only reruns this block, not the whole app
                (a full rerun triggered by a button click resets st.tabs back to Overview)."""
                with st.form(key="ai5_form"):
                    _fc1, _fc2 = st.columns([5, 1])
                    _ai5_idx = _fc1.selectbox("Select", range(len(_opts)),
                                              format_func=lambda i: _opts[i],
                                              label_visibility="collapsed")
                    _ai5_submitted = _fc2.form_submit_button("▶ Run")
                if _ai5_submitted:
                    _ai5_sel = last10.iloc[_ai5_idx].to_dict()
                    with st.spinner("Running query…"):
                        try:
                            _r, _trunc = run_user_query(con, _ai5_sel["sql_text"])
                            increment_queries_run(_ai5_sel["sql_text"])
                            st.success(f"{len(_r):,} rows returned" + (f" (capped at {MAX_ROWS:,})" if _trunc else ""))
                            st.dataframe(_r, width="stretch", hide_index=True)
                            st.download_button("⬇ Download CSV", _r.to_csv(index=False),
                                               "ai_query_result.csv", "text/csv")
                            if _ai5_sel.get("summary"):
                                st.info(_ai5_sel["summary"])
                            _save_query(_ai5_sel["description"], _ai5_sel["question"],
                                        _ai5_sel["sql_hash"], _ai5_sel["sql_text"],
                                        _ai5_sel.get("summary", ""))
                        except (UnsafeQueryError, QueryTimeoutError) as _e:
                            st.info(f"The cached query couldn't run. {_e}")
                        except Exception as _e:
                            st.info(f"The cached query couldn't run — try typing the question again. {query_error_message(_e)}")

            _ai_recent_fragment()
    else:
        st.caption("Query history unavailable.")

# ═══════════════════════════════════════════════════════════════════════════════
with tab6:
    st.subheader("Query History / Log")
    st.markdown(
        "<p style='font-size:0.95em;color:#003399;font-weight:600;margin-bottom:0.5rem;'>"
        "All unique queries ever run, sorted by popularity. "
        "Select one and click ▶ Run to replay.</p>" + _PRIVACY_NOTE_HTML,
        unsafe_allow_html=True,
    )

    if hcon is None:
        st.caption("Query history unavailable.")
    else:
        total = hcon.execute("SELECT COUNT(*) FROM query_log").fetchone()[0]
        st.caption(f"{total} unique {'query' if total == 1 else 'queries'} on record.")
        all_queries = pd.read_sql_query(
            "SELECT * FROM query_log ORDER BY run_count DESC, last_run_at DESC", hcon
        )
        _render_query_table(all_queries, "h6", height=min(80 + total * 38, 520))
