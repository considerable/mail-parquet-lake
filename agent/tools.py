"""
Tool definitions for AI agents — Ollama, Bedrock, Kiro.
Each tool queries the Gmail Parquet lake on S3 via DuckDB.
"""

import os
import duckdb
from dotenv import load_dotenv

load_dotenv()
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "gmail")
S3_BASE = f"s3://{S3_BUCKET}/{S3_PREFIX}"


def query_gmail(
    query_text: str, date_from: str = None, date_to: str = None, limit: int = 20
) -> str:
    """
    Search Gmail lake by keyword. Returns markdown table.

    Args:
        query_text: keyword to search in subject + body + from
        date_from:  start date YYYY-MM-DD (optional)
        date_to:    end date YYYY-MM-DD (optional)
        limit:      max results (default 20)
    """
    where = [
        f"(body_text ILIKE '%{query_text}%' OR subject ILIKE '%{query_text}%' OR from_addr ILIKE '%{query_text}%')"
    ]
    if date_from:
        where.append(f"date >= '{date_from}'")
    if date_to:
        where.append(f"date <= '{date_to}'")

    df = duckdb.sql(f"""
        SELECT date, from_addr, subject
        FROM read_parquet('{S3_BASE}/**/*.parquet')
        WHERE {" AND ".join(where)}
        ORDER BY date DESC
        LIMIT {limit}
    """).df()

    if df.empty:
        return f"No emails found matching '{query_text}'"
    return df.to_markdown(index=False)


def get_email_body(gmail_id: str) -> str:
    """
    Retrieve full email body by gmail_id.

    Args:
        gmail_id: Gmail message ID
    """
    df = duckdb.sql(f"""
        SELECT date, from_addr, subject, body_text
        FROM read_parquet('{S3_BASE}/**/*.parquet')
        WHERE gmail_id = '{gmail_id}'
        LIMIT 1
    """).df()

    if df.empty:
        return f"Email {gmail_id} not found"

    row = df.iloc[0]
    return f"**From:** {row['from_addr']}\n**Date:** {row['date']}\n**Subject:** {row['subject']}\n\n{row['body_text']}"


def list_recent_senders(days: int = 7, limit: int = 20) -> str:
    """
    List most frequent senders in the last N days.

    Args:
        days:  lookback window (default 7)
        limit: max senders (default 20)
    """
    df = duckdb.sql(f"""
        SELECT from_addr, COUNT(*) as email_count
        FROM read_parquet('{S3_BASE}/**/*.parquet')
        WHERE date > now() - INTERVAL '{days} days'
        GROUP BY from_addr
        ORDER BY email_count DESC
        LIMIT {limit}
    """).df()

    return df.to_markdown(index=False)


# Tool registry for agent frameworks
TOOLS = [
    {
        "name": "query_gmail",
        "description": "Search Gmail by keyword across subject, body, and sender. Returns matching emails.",
        "function": query_gmail,
        "parameters": {
            "query_text": {"type": "string", "description": "keyword to search"},
            "date_from": {
                "type": "string",
                "description": "start date YYYY-MM-DD",
                "optional": True,
            },
            "date_to": {
                "type": "string",
                "description": "end date YYYY-MM-DD",
                "optional": True,
            },
            "limit": {
                "type": "integer",
                "description": "max results",
                "optional": True,
            },
        },
    },
    {
        "name": "get_email_body",
        "description": "Get full email body by Gmail message ID.",
        "function": get_email_body,
        "parameters": {
            "gmail_id": {"type": "string", "description": "Gmail message ID"},
        },
    },
    {
        "name": "list_recent_senders",
        "description": "List most frequent email senders in the last N days.",
        "function": list_recent_senders,
        "parameters": {
            "days": {
                "type": "integer",
                "description": "lookback days",
                "optional": True,
            },
            "limit": {
                "type": "integer",
                "description": "max senders",
                "optional": True,
            },
        },
    },
]
