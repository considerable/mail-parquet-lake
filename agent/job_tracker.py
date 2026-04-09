"""
Job search tracker — scans Gmail Parquet lake for interview/recruiter/invite emails,
groups by company, and shows a pipeline dashboard.

Config: agent/job_tracker.yaml (keywords, stages, ignore domains)

Usage:
    python3 agent/job_tracker.py                  # last 30 days, terminal
    python3 agent/job_tracker.py --days 90        # last 90 days
    python3 agent/job_tracker.py --md             # output markdown
"""

import os
import re
import argparse
from collections import defaultdict
from pathlib import Path

import yaml
import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "gmail")
S3_BASE = f"s3://{S3_BUCKET}/{S3_PREFIX}"
SELF_EMAIL = os.getenv("GMAIL_ACCOUNT", "")

CONFIG_PATH = Path(__file__).parent / "job_tracker.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


CFG = _load_config()
KEYWORDS = CFG["keywords"]
STAGES = [(s["name"], s["triggers"]) for s in CFG["stages"]]
STAGE_ORDER = {name: i for i, (name, _) in enumerate(STAGES)}
IGNORE_DOMAINS = set(CFG.get("ignore_domains", []))


def _escape_sql(s: str) -> str:
    return s.replace("'", "''")


def _is_ignored_domain(addr: str) -> bool:
    match = re.search(r"@([\w.-]+)", str(addr))
    if not match:
        return False
    domain = match.group(1).lower()
    return any(domain == d or domain.endswith("." + d) for d in IGNORE_DOMAINS)


def _build_keyword_filter() -> str:
    clauses = []
    for kw in KEYWORDS:
        esc = _escape_sql(kw)
        clauses.append(f"subject ILIKE '%{esc}%'")
        clauses.append(f"body_text ILIKE '%{esc}%'")
    return " OR ".join(clauses)


def _detect_stage(subject: str, body: str) -> str:
    text = f"{subject} {body}".lower()
    for stage_name, triggers in STAGES:
        if any(t in text for t in triggers):
            return stage_name
    return "outreach"


def _extract_company(from_addr: str) -> str:
    match = re.search(r"@([\w.-]+)", from_addr)
    if not match:
        return from_addr
    domain = match.group(1).lower()
    generic = {
        "gmail.com",
        "yahoo.com",
        "outlook.com",
        "hotmail.com",
        "google.com",
        "calendar.google.com",
        "googlemail.com",
    }
    if domain in generic:
        name_match = re.match(r'"?([^"<]+)"?\s*<', from_addr)
        return name_match.group(1).strip() if name_match else domain
    for prefix in (
        "mail.",
        "email.",
        "no-reply.",
        "noreply.",
        "trans.",
        "workflow.mail.",
    ):
        if domain.startswith(prefix):
            domain = domain[len(prefix) :]
    return domain.split(".")[0].replace("-", " ").title()


def fetch_job_emails(days: int) -> pd.DataFrame:
    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("CALL load_aws_credentials();")

    keyword_filter = _build_keyword_filter()
    df = con.sql(f"""
        SELECT gmail_id, thread_id, date, from_addr, to_addr, subject, body_text
        FROM read_parquet('{S3_BASE}/**/*.parquet')
        WHERE date > now() - INTERVAL '{days} days'
          AND ({keyword_filter})
        ORDER BY date DESC
    """).df()

    if SELF_EMAIL:
        df = df[
            ~(
                (df["from_addr"].str.contains(SELF_EMAIL, case=False, na=False))
                & (~df["subject"].str.lower().str.startswith("re:"))
            )
        ]

    df = df[~df["from_addr"].apply(_is_ignored_domain)]
    return df


def build_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    companies = defaultdict(
        lambda: {
            "company": "",
            "stage": "outreach",
            "last_date": None,
            "emails": 0,
            "latest_subject": "",
            "contacts": set(),
        }
    )

    for _, row in df.iterrows():
        company = _extract_company(row["from_addr"])
        rec = companies[company]
        rec["company"] = company
        rec["emails"] += 1
        rec["contacts"].add(row["from_addr"])

        row_date = pd.to_datetime(row["date"])
        if rec["last_date"] is None or row_date > rec["last_date"]:
            rec["last_date"] = row_date
            rec["latest_subject"] = row["subject"]

        stage = _detect_stage(row["subject"] or "", row["body_text"] or "")
        if STAGE_ORDER.get(stage, 99) < STAGE_ORDER.get(rec["stage"], 99):
            rec["stage"] = stage

    rows = []
    for rec in companies.values():
        rows.append(
            {
                "Company": rec["company"],
                "Stage": rec["stage"],
                "Emails": rec["emails"],
                "Last Activity": rec["last_date"].strftime("%Y-%m-%d")
                if rec["last_date"]
                else "",
                "Latest Subject": (rec["latest_subject"] or "")[:80],
                "Contacts": len(rec["contacts"]),
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        return result

    result["_stage_rank"] = result["Stage"].map(STAGE_ORDER).fillna(99)
    result = result.sort_values(
        ["Last Activity", "_stage_rank"], ascending=[False, True]
    )
    return result.drop(columns=["_stage_rank"])


def main():
    parser = argparse.ArgumentParser(description="Job search pipeline tracker")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--md", action="store_true", help="Output markdown")
    args = parser.parse_args()

    print(f"Scanning last {args.days} days for job-related emails...")
    df = fetch_job_emails(args.days)
    print(f"Found {len(df)} job-related emails\n")

    pipeline = build_pipeline(df)
    if pipeline.empty:
        print("No job-related emails found.")
        return

    if args.md:
        print(pipeline.to_markdown(index=False))
    else:
        print(pipeline.to_string(index=False))


if __name__ == "__main__":
    main()
