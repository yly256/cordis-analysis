"""
CORDIS Analytics Dashboard
Run: streamlit run app.py
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

DB_PATH = "cordis.duckdb"

st.set_page_config(
    page_title="CORDIS Analytics",
    page_icon="🇪🇺",
    layout="wide",
)

st.title("🇪🇺 CORDIS Project Analytics")
st.caption("FP7 · H2020 · Horizon Europe — unified database")

@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

try:
    con = get_con()
except Exception as e:
    st.error(f"Cannot open database: {e}\nRun `python ingest.py` first.")
    st.stop()

# ── Sidebar filters ────────────────────────────────────────────────────────────
with st.sidebar:
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

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🔬 Deep Dive", "🌍 Geography", "💻 SQL"])

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
            use_container_width=True
        )

    with col2:
        st.subheader("Average Budget by FP (€M)")
        st.plotly_chart(
            px.bar(df, x="FP", y="avg_budget_M", color="FP",
                   text="avg_budget_M", title="Avg Budget (€M) by FP"),
            use_container_width=True
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
            use_container_width=True
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
            use_container_width=True
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
            use_container_width=True
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
        st.dataframe(df2, use_container_width=True, hide_index=True)

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
        use_container_width=True
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
    st.dataframe(df4, use_container_width=True, hide_index=True)

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
            use_container_width=True
        )

    with col2:
        st.subheader("Avg Budget by Coordinator Country (Top 25)")
        st.plotly_chart(
            px.bar(df.sort_values("avg_budget_M", ascending=False).head(25),
                   x="avg_budget_M", y="country", orientation="h",
                   title="Avg Budget (€M) by Coordinator Country"),
            use_container_width=True
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
        use_container_width=True
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
            st.dataframe(result, use_container_width=True, hide_index=True)
            csv = result.to_csv(index=False)
            st.download_button("⬇ Download CSV", csv, "result.csv", "text/csv")
        except Exception as e:
            st.error(f"SQL error: {e}")
