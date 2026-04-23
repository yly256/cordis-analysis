"""
CORDIS Analytics Dashboard
Run: streamlit run app.py
"""

import os
import re
import sqlite3
import hashlib
import tempfile
from datetime import datetime
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import urllib.request
from pathlib import Path
import anthropic
from dotenv import load_dotenv
import streamlit.components.v1 as _components

load_dotenv()

print("[BOOT] imports OK")

_APP_DIR  = Path(__file__).parent
DB_PATH   = str(_APP_DIR / "cordis.duckdb")
DB_URL    = "https://github.com/yly256/cordis-analysis/releases/download/v1.0/cordis.duckdb"
HISTORY_DB = str(Path(tempfile.gettempdir()) / "query_history.db")

print(f"[BOOT] DB_PATH   = {DB_PATH}  exists={Path(DB_PATH).exists()}")
print(f"[BOOT] HISTORY_DB= {HISTORY_DB}")

def _get_ai_client():
    key = os.getenv("ANTHROPIC_API_KEY", "")
    if not key:
        try:
            key = st.secrets["ANTHROPIC_API_KEY"]
        except Exception as _e:
            st.error(f"st.secrets[\"ANTHROPIC_API_KEY\"] raised {type(_e).__name__}: {_e}")
            st.stop()
    if not key:
        st.error("ANTHROPIC_API_KEY is empty. Check Streamlit Cloud → Settings → Secrets.")
        st.stop()
    return anthropic.Anthropic(api_key=key)

st.set_page_config(
    page_title="CORDIS Analytics",
    page_icon="🇪🇺",
    layout="wide",
)

st.title("🇪🇺 CORDIS Project Analytics")
st.caption("FP7 · H2020 · Horizon Europe — unified database")

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
        return duckdb.connect(DB_PATH, read_only=True)

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
    except Exception as e:
        print(f"[BOOT WARNING] history db failed: {e}")
        st.warning(f"Query history unavailable: {e}")
        hcon = None

except Exception as _boot_err:
    import traceback
    st.error("### Startup error — please share this with the developer")
    st.code(traceback.format_exc())
    st.stop()

# ── Sidebar filters ────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
<div style="background:linear-gradient(135deg,#003399,#0066cc);
            color:white;padding:12px 16px;border-radius:10px;
            text-align:center;margin-bottom:8px;">
  <div style="font-size:1.25em;font-weight:700;letter-spacing:.03em;">
    &#x1F449; Start here
  </div>
  <div style="font-size:.82em;opacity:.88;margin-top:4px;">
    Pick programmes, status &amp; years
  </div>
</div>
<style>
@keyframes cordis-pulse {
  0%   { box-shadow: 0 0 0 0   rgba(0,102,204,.7); }
  70%  { box-shadow: 0 0 0 10px rgba(0,102,204,0); }
  100% { box-shadow: 0 0 0 0   rgba(0,102,204,0); }
}
section[data-testid="stSidebar"] > div:first-child > div:first-child > div:first-child {
  animation: cordis-pulse 2s infinite;
  border-radius: 10px;
}
</style>
""", unsafe_allow_html=True)
    st.header("Filters")

    fps = con.execute(
        "SELECT DISTINCT FP FROM projects ORDER BY FP"
    ).df()["FP"].tolist()
    sel_fp = st.multiselect("Framework Programme", fps, default=fps)

    statuses = con.execute(
        "SELECT DISTINCT status FROM projects WHERE status IS NOT NULL ORDER BY 1"
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
    "- COUNTRY PARTICIPATION: when counting projects per country, always count ALL projects "
    "where that country appears in ANY role (coordinator or participant). Do this by joining "
    "the organizations table and counting DISTINCT projectIDs, e.g.: "
    "SELECT o.country, COUNT(DISTINCT p.projectID) AS projects, ROUND(SUM(p.totalCost)/1e6,2) AS total_funding_M "
    "FROM projects p JOIN organizations o ON p.projectID = o.projectID "
    "WHERE <filters on p> AND o.country IS NOT NULL "
    "GROUP BY o.country ORDER BY projects DESC. "
    "Only use coordinator_country when the user explicitly asks about coordinators only.\n"
    "- NULL COUNTRIES: always exclude rows where the country/coordinator_country column IS NULL "
    "by adding the appropriate IS NOT NULL filter."
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
    """Compact scrollable table + selectbox to pick and run a query."""
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
    st.dataframe(disp, width="stretch", hide_index=True, height=height)

    options = [f"{i+1}. {row['description']}" for i, (_, row) in enumerate(rows_df.iterrows())]
    c1, c2 = st.columns([5, 1])
    sel_idx = c1.selectbox(
        "Select", range(len(options)),
        format_func=lambda i: options[i],
        label_visibility="collapsed",
        key=f"{key_prefix}_sel",
    )
    sel = rows_df.iloc[sel_idx]

    if c2.button("▶ Run", key=f"{key_prefix}_run"):
        st.session_state["pending_run"] = {
            "question":    sel["question"],
            "sql":         sel["sql_text"],
            "hash":        sel["sql_hash"],
            "desc":        sel["description"],
            "summary":     sel["summary"],
            "last_run_at": sel["last_run_at"],
        }
        st.rerun()

    with st.expander("Summary for selected query"):
        st.write(sel["summary"])


# ── Auto-switch to AI Query tab when a history run is pending ──────────────────
if "pending_run" in st.session_state:
    _components.html("""
        <script>
        setTimeout(function () {
            var tabs = window.parent.document.querySelectorAll('button[role="tab"]');
            for (var i = 0; i < tabs.length; i++) {
                if (tabs[i].textContent.includes('AI Query')) {
                    tabs[i].click();
                    window.parent.scrollTo({ top: 0, behavior: 'smooth' });
                    break;
                }
            }
        }, 150);
        </script>
    """, height=0)

# ── Tabs ───────────────────────────────────────────────────────────────────────
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
    map_df = con.execute(f"""
        SELECT country, COUNT(DISTINCT projectID) AS projects
        FROM organizations
        WHERE country IS NOT NULL
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
    st.caption("Tables: `projects`, `organizations`, `topics`, `legal_basis`, `euro_sci_voc`, `policy_priorities`")

    example_queries = {
        "Avg budget & partners by FP": f"SELECT FP, COUNT(*) AS n, ROUND(AVG(totalCost)/1e6,2) AS avg_M, ROUND(AVG(partner_count),1) AS avg_partners FROM projects WHERE {W()} GROUP BY FP",
        "Top 10 coordinators": f"SELECT coordinator_name, coordinator_country, COUNT(*) AS projects FROM projects WHERE {W()} AND coordinator_name IS NOT NULL GROUP BY 1,2 ORDER BY 3 DESC LIMIT 10",
        "SME participation rate by FP": f"SELECT FP, ROUND(100.0*SUM(CASE WHEN sme_count>0 THEN 1 END)/COUNT(*),1) AS pct_with_sme FROM projects WHERE {W()} GROUP BY FP",
        "Budget by funding scheme (top 20)": f"SELECT fundingScheme, COUNT(*) AS n, ROUND(AVG(totalCost)/1e6,2) AS avg_M FROM projects WHERE {W()} AND fundingScheme IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT 20",
        "Projects > €50M": f"SELECT acronym, FP, ROUND(totalCost/1e6,1) AS budget_M, partner_count, coordinator_country FROM projects WHERE {W()} AND totalCost > 50000000 ORDER BY totalCost DESC",
    }

    sel_example = st.selectbox("Load example query", ["(custom)"] + list(example_queries.keys()))
    default_q = example_queries.get(sel_example, f"SELECT * FROM projects WHERE {W()} LIMIT 10")

    q = st.text_area("SQL", value=default_q, height=100)

    if st.button("▶ Run Query"):
        try:
            result = con.execute(q).df()
            st.success(f"{len(result):,} rows returned")
            st.dataframe(result, width="stretch", hide_index=True)
            csv = result.to_csv(index=False)
            st.download_button("⬇ Download CSV", csv, "result.csv", "text/csv")
        except Exception as e:
            st.error(f"SQL error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader("Ask a Question in Plain English")
    st.caption(
        "Claude translates your question into SQL, runs it, and summarises the results. "
        "Sidebar filters apply automatically."
    )

    # ── Pre-fill from history Run button ──────────────────────────────────────
    _pending = st.session_state.pop("pending_run", None)
    _prefill_q = _pending["question"] if _pending else ""

    question = st.text_input(
        "Your question",
        value=_prefill_q,
        placeholder="e.g. Which countries received the most Horizon Europe funding?",
    )

    ask_clicked = st.button("Ask Claude")
    _auto_run   = _pending is not None   # triggered by a history Run button

    if (ask_clicked or _auto_run) and question.strip():
        # ── If coming from history, use cached SQL — unless it's stale (>30 days) ──
        if _auto_run and _pending:
            st.toast("Query run from history — results below in the AI Query tab", icon="✅")
            summary = _pending["summary"]
            desc    = _pending["desc"]

            _last_run = _pending.get("last_run_at", "")
            try:
                _days_old = (datetime.utcnow() - datetime.strptime(_last_run, "%Y-%m-%d %H:%M")).days
            except Exception:
                _days_old = 999

            if _days_old > 30:
                with st.spinner(f"SQL is {_days_old} days old — regenerating…"):
                    sql = _generate_sql(question, W())
                with st.expander("Regenerated SQL", expanded=False):
                    st.code(sql, language="sql")
            else:
                sql = _pending["sql"]
                with st.expander("SQL (from history)", expanded=False):
                    st.code(sql, language="sql")
            with st.spinner("Running query…"):
                try:
                    result = con.execute(sql).df()
                except Exception as e:
                    st.info(
                        "Sorry, the cached query failed against the current data. "
                        "Try typing the question again to regenerate SQL.\n\n"
                        f"_Technical detail: {e}_"
                    )
                    result = None
            if result is not None:
                _save_query(desc, question, _pending["hash"], sql, summary)
                st.success(f"{len(result):,} rows returned")
                st.dataframe(result, width="stretch", hide_index=True)
                st.download_button("⬇ Download CSV", result.to_csv(index=False),
                                   "ai_query_result.csv", "text/csv")
                st.info(summary)
        else:
            # ── Normal typed query path ───────────────────────────────────────
            guard = _check_relevance(question)
            if not guard.get("relevant", False):
                st.warning(
                    f"That question doesn't seem related to CORDIS data — "
                    f"{guard.get('reason', 'please ask about EU research projects, budgets, or organisations.')} "
                    "Try rephrasing."
                )
            else:
                with st.spinner("Generating SQL…"):
                    sql = _generate_sql(question, W())
                with st.expander("Generated SQL", expanded=False):
                    st.code(sql, language="sql")

                result = None
                with st.spinner("Running query…"):
                    try:
                        result = con.execute(sql).df()
                    except Exception as e:
                        with st.spinner("Fixing query…"):
                            sql = _fix_sql(question, sql, str(e), W())
                        with st.expander("Corrected SQL", expanded=False):
                            st.code(sql, language="sql")
                        try:
                            result = con.execute(sql).df()
                        except Exception as e2:
                            st.info(
                                "Sorry, I wasn't able to generate a working query for that question. "
                                "Try rephrasing, or use the SQL tab for full control.\n\n"
                                f"_Technical detail: {e2}_"
                            )

                if result is not None:
                    st.success(f"{len(result):,} rows returned")
                    st.dataframe(result, width="stretch", hide_index=True)
                    st.download_button("⬇ Download CSV", result.to_csv(index=False),
                                       "ai_query_result.csv", "text/csv")
                    with st.spinner("Summarising…"):
                        summary = _summarize(question, result)
                    st.info(summary)
                    with st.spinner("Saving to history…"):
                        desc = _distill_description(question)
                        _save_query(desc, question, _sql_hash(sql), sql, summary)

    # ── Last 10 recent queries ────────────────────────────────────────────────
    st.divider()
    st.markdown("**Recent queries** — select and click ▶ Run to replay without an API call")
    if hcon is not None:
        last10 = pd.read_sql_query(
            "SELECT * FROM query_log ORDER BY last_run_at DESC LIMIT 10", hcon
        )
        _render_query_table(last10, "ai5", height=280)
    else:
        st.caption("Query history unavailable.")

# ═══════════════════════════════════════════════════════════════════════════════
with tab6:
    st.subheader("Query History / Log")
    st.caption("All unique queries ever run, sorted by popularity. Select one and click ▶ Run to replay — no API call needed.")

    if hcon is None:
        st.caption("Query history unavailable.")
    else:
        total = hcon.execute("SELECT COUNT(*) FROM query_log").fetchone()[0]
        st.caption(f"{total} unique {'query' if total == 1 else 'queries'} on record.")
        all_queries = pd.read_sql_query(
            "SELECT * FROM query_log ORDER BY run_count DESC, last_run_at DESC", hcon
        )
        _render_query_table(all_queries, "h6", height=min(80 + total * 38, 520))
