"""
CORDIS Data Ingestion Script
Reads FP7 / H2020 / HORIZON JSON zips → builds cordis.duckdb
Run: python ingest.py
"""

import zipfile, json, os
from pathlib import Path
import duckdb
import pandas as pd

DATA_DIR = Path("data")
DB_PATH  = "cordis.duckdb"

FP_MAP = {
    "fp7":     "FP7",
    "h2020":   "H2020",
    "horizon": "HEU",
}

def detect_fp(zip_name: str) -> str:
    z = zip_name.lower()
    for key, label in FP_MAP.items():
        if key in z:
            return label
    raise ValueError(f"Cannot detect FP from zip name: {zip_name}")

def load_json_from_zip(zf: zipfile.ZipFile, inner_name: str) -> list:
    matches = [n for n in zf.namelist()
               if Path(n).name == inner_name and not n.endswith("/")]
    if not matches:
        return []
    with zf.open(matches[0]) as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]

def normalise_projects(records: list, fp: str) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df["FP"] = fp
    if "frameworkProgramme" in df.columns:
        df["frameworkProgramme"] = df["frameworkProgramme"].replace("HORIZON", "HEU")
    for col in ["totalCost", "ecMaxContribution"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["startDate", "endDate", "ecSignatureDate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "startDate" in df.columns and "endDate" in df.columns:
        df["duration_months"] = (
            (df["endDate"] - df["startDate"]).dt.days / 30.44
        ).round(1)
    return df

def normalise_organizations(records: list, fp: str) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df["FP"] = fp
    # Replace empty strings with NaN before numeric conversion
    for col in ["ecContribution", "netEcContribution", "totalCost"]:
        if col in df.columns:
            df[col] = df[col].replace("", None)
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # Ensure SME is a clean string so DuckDB doesn't mix bool/str types
    if "SME" in df.columns:
        df["SME"] = df["SME"].astype(str).str.lower().replace("nan", "")
    # Ensure endOfParticipation and active are strings too
    for col in ["endOfParticipation", "active"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", "")
    return df

def main():
    zip_files = sorted(DATA_DIR.glob("*json*.zip"))
    if not zip_files:
        print("ERROR: No JSON zips found in data/  — check the folder.")
        return

    all_projects   = []
    all_orgs       = []
    all_topics     = []
    all_legal      = []
    all_eurosci    = []
    all_policy     = []

    for zpath in zip_files:
        fp = detect_fp(zpath.name)
        print(f"\n{'='*50}")
        print(f"Processing: {zpath.name}  →  {fp}")
        with zipfile.ZipFile(zpath) as zf:
            inner_files = [Path(n).name for n in zf.namelist()]
            print(f"Files inside: {inner_files}")

            proj = load_json_from_zip(zf, "project.json")
            if proj:
                all_projects.append(normalise_projects(proj, fp))
                print(f"  project.json        : {len(proj):>8,} rows")

            orgs = load_json_from_zip(zf, "organization.json")
            if orgs:
                all_orgs.append(normalise_organizations(orgs, fp))
                print(f"  organization.json   : {len(orgs):>8,} rows")

            topics = load_json_from_zip(zf, "topics.json")
            if topics:
                df = pd.DataFrame(topics); df["FP"] = fp
                all_topics.append(df)
                print(f"  topics.json         : {len(topics):>8,} rows")

            legal = load_json_from_zip(zf, "legalBasis.json")
            if legal:
                df = pd.DataFrame(legal); df["FP"] = fp
                all_legal.append(df)
                print(f"  legalBasis.json     : {len(legal):>8,} rows")

            esv = load_json_from_zip(zf, "euroSciVoc.json")
            if esv:
                df = pd.DataFrame(esv); df["FP"] = fp
                all_eurosci.append(df)
                print(f"  euroSciVoc.json     : {len(esv):>8,} rows")

            pp = load_json_from_zip(zf, "policyPriorities.json")
            if pp:
                df = pd.DataFrame(pp); df["FP"] = fp
                all_policy.append(df)
                print(f"  policyPriorities.json:{len(pp):>7,} rows")

    # ── Concatenate ────────────────────────────────────────────────────────────
    projects_df = pd.concat(all_projects, ignore_index=True) if all_projects else pd.DataFrame()
    orgs_df     = pd.concat(all_orgs,     ignore_index=True) if all_orgs     else pd.DataFrame()
    topics_df   = pd.concat(all_topics,   ignore_index=True) if all_topics   else pd.DataFrame()
    legal_df    = pd.concat(all_legal,    ignore_index=True) if all_legal    else pd.DataFrame()
    esv_df      = pd.concat(all_eurosci,  ignore_index=True) if all_eurosci  else pd.DataFrame()
    policy_df   = pd.concat(all_policy,   ignore_index=True) if all_policy   else pd.DataFrame()

    print(f"\n{'='*50}")
    print(f"TOTALS BEFORE ENRICHMENT")
    print(f"  projects      : {len(projects_df):,}")
    print(f"  org rows      : {len(orgs_df):,}")

    # ── Derive partner/SME/country counts from organizations ─────────────────
    if not orgs_df.empty and "role" in orgs_df.columns:
        consortium_roles = ["coordinator", "participant"]

        partner_counts = (
            orgs_df[orgs_df["role"].isin(consortium_roles)]
            .groupby("projectID").size()
            .reset_index(name="partner_count")
        )
        sme_counts = (
            orgs_df[orgs_df["SME"].astype(str).str.lower() == "true"]
            .groupby("projectID").size()
            .reset_index(name="sme_count")
        )
        country_counts = (
            orgs_df[orgs_df["role"].isin(consortium_roles)]
            .groupby("projectID")["country"].nunique()
            .reset_index(name="country_count")
        )
        coord_country = (
            orgs_df[orgs_df["role"] == "coordinator"][["projectID", "country"]]
            .drop_duplicates("projectID")
            .rename(columns={"country": "coordinator_country"})
        )
        coord_name = (
            orgs_df[orgs_df["role"] == "coordinator"][["projectID", "name"]]
            .drop_duplicates("projectID")
            .rename(columns={"name": "coordinator_name"})
        )

        for df_right, key in [
            (partner_counts, "projectID"),
            (sme_counts,     "projectID"),
            (country_counts, "projectID"),
            (coord_country,  "projectID"),
            (coord_name,     "projectID"),
        ]:
            projects_df = projects_df.merge(
                df_right, left_on="id", right_on=key, how="left"
            ).drop(columns=[key], errors="ignore")

        projects_df["partner_count"] = projects_df["partner_count"].fillna(0).astype(int)
        projects_df["sme_count"]     = projects_df["sme_count"].fillna(0).astype(int)
        projects_df["country_count"] = projects_df["country_count"].fillna(0).astype(int)

    # ── Write DuckDB ───────────────────────────────────────────────────────────
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"\nRemoved existing {DB_PATH}")

    con = duckdb.connect(DB_PATH)

    tables = {
        "projects":      projects_df,
        "organizations": orgs_df,
        "topics":        topics_df,
        "legal_basis":   legal_df,
        "euro_sci_voc":  esv_df,
        "policy_priorities": policy_df,
    }

    print(f"\nWriting tables to {DB_PATH}:")
    for tname, df in tables.items():
        if not df.empty:
            # Stringify all object columns to avoid DuckDB mixed-type errors
            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].astype(str).replace("nan", "").replace("None", "")
            con.execute(f"CREATE TABLE {tname} AS SELECT * FROM df")
            print(f"  {tname:<22}: {len(df):,} rows")

    # Quick sanity check
    print("\nSanity check — average budget by FP:")
    print(con.execute("""
        SELECT FP,
               COUNT(*) AS projects,
               ROUND(AVG(totalCost)/1e6, 2) AS avg_budget_M,
               ROUND(AVG(partner_count), 1) AS avg_partners
        FROM projects
        GROUP BY FP ORDER BY FP
    """).df().to_string(index=False))

    con.close()
    print(f"\n✓ Done. Database → {DB_PATH}")
    print("Next: streamlit run app.py")

if __name__ == "__main__":
    main()