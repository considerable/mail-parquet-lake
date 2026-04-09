"""
Export emails from S3 Parquet to .md files for AI agents.

Usage:
    python3 export/to_markdown.py --days 30 --filter "fitch" --output export/output/
"""

import os
import re
import argparse
import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "gmail")
S3_BASE = f"s3://{S3_BUCKET}/{S3_PREFIX}"


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    return text[:60]


def export_to_markdown(days: int, filter_text: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    where_clauses = [f"date > now() - INTERVAL '{days} days'"]
    if filter_text:
        where_clauses.append(
            f"(body_text ILIKE '%{filter_text}%' OR subject ILIKE '%{filter_text}%' OR from_addr ILIKE '%{filter_text}%')"
        )
    where = " AND ".join(where_clauses)

    df = duckdb.sql(f"""
        SELECT date, from_addr, to_addr, subject, body_text, labels
        FROM read_parquet('{S3_BASE}/**/*.parquet')
        WHERE {where}
        ORDER BY date DESC
    """).df()

    print(f"Exporting {len(df)} emails to {output_dir}/")

    for _, row in df.iterrows():
        date_str = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
        domain = (
            row["from_addr"].split("@")[-1].split(">")[0]
            if "@" in row["from_addr"]
            else "unknown"
        )
        subject_slug = slugify(row["subject"] or "no-subject")
        fname = f"{date_str}-{slugify(domain)}-{subject_slug}.md"
        fpath = os.path.join(output_dir, fname)

        with open(fpath, "w") as f:
            f.write(f"# {row['subject']}\n\n")
            f.write(f"**Date:** {date_str}  \n")
            f.write(f"**From:** {row['from_addr']}  \n")
            f.write(f"**To:** {row['to_addr']}  \n")
            f.write(f"**Labels:** {row['labels']}  \n\n")
            f.write("---\n\n")
            f.write(row["body_text"] or "")

    print(f"Done — {len(df)} files in {output_dir}/")
    print(f'Use with Amazon Q: q chat "@{output_dir} ..."')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--filter", default="")
    parser.add_argument("--output", default="export/output")
    args = parser.parse_args()

    export_to_markdown(args.days, args.filter, args.output)
