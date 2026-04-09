"""
Gmail → Parquet on S3 sync.

Usage:
    python3 sync/gmail_sync.py --full          # initial import
    python3 sync/gmail_sync.py --incremental   # delta sync via history API
"""

import os
import json
import argparse
import base64
import time
import boto3
import duckdb
import pandas as pd
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "gmail")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BASE = f"s3://{S3_BUCKET}/{S3_PREFIX}"
STATE_KEY = f"{S3_PREFIX}/sync_state.json"

# DuckDB needs AWS creds for S3 reads
_db = duckdb.connect()
_db.execute("CALL load_aws_credentials()")


def get_gmail_service():
    creds = None
    token_file = os.getenv("GMAIL_TOKEN_FILE", "token.json")
    creds_file = os.getenv("GMAIL_CREDENTIALS_FILE", "credentials.json")

    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_file, "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds)


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def _fetch_message(service, msg_id, retries=3):
    for attempt in range(retries):
        try:
            return (
                service.users()
                .messages()
                .get(userId="me", id=msg_id, format="full")
                .execute()
            )
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2**attempt)
            else:
                print(f"  skipping {msg_id}: {e}")
                return None


def load_sync_state(s3):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=STATE_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {"last_history_id": None}


def save_sync_state(s3, state):
    s3.put_object(Bucket=S3_BUCKET, Key=STATE_KEY, Body=json.dumps(state))


def parse_message(msg):
    """Extract fields from Gmail API message object. Returns None if date is missing."""
    internal_date = int(msg.get("internalDate", 0))
    if internal_date == 0:
        print(f"  skipping {msg['id']}: internalDate=0")
        return None

    headers = {
        h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])
    }

    # decode body
    body = ""
    payload = msg.get("payload", {})
    if "body" in payload and payload["body"].get("data"):
        body = base64.urlsafe_b64decode(payload["body"]["data"]).decode(
            "utf-8", errors="replace"
        )
    elif "parts" in payload:
        for part in payload["parts"]:
            if part.get("mimeType") == "text/plain" and part.get("body", {}).get(
                "data"
            ):
                body = base64.urlsafe_b64decode(part["body"]["data"]).decode(
                    "utf-8", errors="replace"
                )
                break

    return {
        "gmail_id": msg["id"],
        "thread_id": msg.get("threadId", ""),
        "date": pd.to_datetime(internal_date, unit="ms", utc=True),
        "from_addr": headers.get("from", ""),
        "to_addr": headers.get("to", ""),
        "subject": headers.get("subject", ""),
        "body_text": body[:10000],  # cap at 10KB per email
        "labels": json.dumps(msg.get("labelIds", [])),
        "history_id": int(msg.get("historyId", 0)),
    }


def write_parquet(records, s3):
    """Write records to S3 Parquet, partitioned by year/month."""
    if not records:
        return
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"], utc=True)

    for (year, month), group in df.groupby([df["date"].dt.year, df["date"].dt.month]):
        key = f"{S3_PREFIX}/year={year}/month={month:02d}/emails.parquet"
        # merge with existing partition if it exists
        try:
            existing = _db.sql(
                f"SELECT * FROM read_parquet('s3://{S3_BUCKET}/{key}')"
            ).df()
            group = pd.concat([existing, group]).drop_duplicates(
                subset="gmail_id", keep="last"
            )
        except Exception:
            pass  # partition doesn't exist yet

        buf = group.to_parquet(index=False)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf)
        print(f"  wrote {len(group)} rows → s3://{S3_BUCKET}/{key}")


def sync_full(service, s3):
    """Initial full import — fetches all messages."""
    print("Starting full sync...")
    page_token = None
    all_records = []
    batch = []

    while True:
        kwargs = {"userId": "me", "maxResults": 500}
        if page_token:
            kwargs["pageToken"] = page_token

        result = service.users().messages().list(**kwargs).execute()
        messages = result.get("messages", [])

        for msg_ref in messages:
            msg = _fetch_message(service, msg_ref["id"])
            if msg:
                rec = parse_message(msg)
                if rec:
                    batch.append(rec)

            if len(batch) >= 1000:
                write_parquet(batch, s3)
                all_records.extend(batch)
                batch = []
                print(f"  synced {len(all_records)} emails so far...")

        page_token = result.get("nextPageToken")
        if not page_token:
            break

    if batch:
        write_parquet(batch, s3)
        all_records.extend(batch)

    # save history_id from most recent message
    if all_records:
        latest_history_id = max(r["history_id"] for r in all_records)
        save_sync_state(s3, {"last_history_id": latest_history_id})

    print(f"Full sync complete: {len(all_records)} emails")


def sync_incremental(service, s3):
    """Delta sync via Gmail history API — adds and deletes only."""
    state = load_sync_state(s3)
    last_history_id = state.get("last_history_id")

    if not last_history_id:
        print("No history ID found — run --full first")
        return

    print(f"Incremental sync from history_id={last_history_id}...")
    new_records = []
    deleted_ids = []
    page_token = None

    while True:
        kwargs = {
            "userId": "me",
            "startHistoryId": last_history_id,
            "historyTypes": ["messageAdded", "messageDeleted"],
        }
        if page_token:
            kwargs["pageToken"] = page_token

        try:
            result = service.users().history().list(**kwargs).execute()
        except Exception as e:
            print(f"History API error: {e} — consider running --full")
            return

        for history in result.get("history", []):
            for added in history.get("messagesAdded", []):
                msg = _fetch_message(service, added["message"]["id"])
                if msg:
                    rec = parse_message(msg)
                    if rec:
                        new_records.append(rec)

            for deleted in history.get("messagesDeleted", []):
                deleted_ids.append(deleted["message"]["id"])

        page_token = result.get("nextPageToken")
        if not page_token:
            last_history_id = result.get("historyId", last_history_id)
            break

    if new_records:
        write_parquet(new_records, s3)
        print(f"  added {len(new_records)} new emails")

    if deleted_ids:
        delete_from_parquet(deleted_ids, s3)
        print(f"  deleted {len(deleted_ids)} emails")

    save_sync_state(s3, {"last_history_id": last_history_id})
    print("Incremental sync complete")


def delete_from_parquet(gmail_ids, s3):
    """Remove deleted emails from affected Parquet partitions."""
    ids_set = set(gmail_ids)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/year="):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            df = _db.sql(f"SELECT * FROM read_parquet('s3://{S3_BUCKET}/{key}')").df()
            before = len(df)
            df = df[~df["gmail_id"].isin(ids_set)]
            if len(df) < before:
                s3.put_object(
                    Bucket=S3_BUCKET, Key=key, Body=df.to_parquet(index=False)
                )
                print(f"  removed {before - len(df)} rows from {key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--incremental", action="store_true")
    args = parser.parse_args()

    service = get_gmail_service()
    s3 = get_s3_client()

    if args.full:
        sync_full(service, s3)
    elif args.incremental:
        sync_incremental(service, s3)
    else:
        print("Use --full or --incremental")
