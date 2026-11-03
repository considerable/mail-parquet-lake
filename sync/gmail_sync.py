"""
Gmail → Parquet on S3 sync.

Usage:
    python3 sync/gmail_sync.py --full          # initial import
    python3 sync/gmail_sync.py --incremental   # delta sync via history API
    python3 sync/gmail_sync.py --attachment-backfill  # resumable historical files
"""

import argparse
import json
import os
import time
from datetime import UTC, datetime
from io import BytesIO

from botocore.exceptions import ClientError

try:
    from .attachments import attachment_parts, decode_data, read_json, store_attachments
    from .gmail_requests import execute_batch, execute_request, retry_transport
    from .telemetry import telemetry
except ImportError:
    from attachments import attachment_parts, decode_data, read_json, store_attachments
    from gmail_requests import execute_batch, execute_request, retry_transport
    from telemetry import telemetry
import boto3
import pandas as pd
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "gmail")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BASE = f"s3://{S3_BUCKET}/{S3_PREFIX}"
STATE_KEY = f"{S3_PREFIX}/sync_state.json"

BACKFILL_KEY = f"{S3_PREFIX}/attachment_backfill_state.json"


def get_gmail_service():
    creds = None
    token_file = os.getenv("GMAIL_TOKEN_FILE", "token.json")
    creds_file = os.getenv("GMAIL_CREDENTIALS_FILE", "credentials.json")

    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            retry_transport(lambda: creds.refresh(Request()))
        else:
            if os.getenv("GMAIL_NONINTERACTIVE") == "1":
                raise RuntimeError("Gmail token requires renewed authorization")
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
            return execute_request(
                service.users().messages().get(userId="me", id=msg_id, format="full")
            )
        except HttpError as e:
            if e.resp.status == 404:
                return None  # Permanently deleted since Gmail returned the reference.
            if attempt < retries - 1:
                telemetry.count("lake_gmail_fetch_retries", 1)
                time.sleep(2**attempt)
            else:
                raise RuntimeError(
                    "Gmail message fetch failed; checkpoint not advanced"
                ) from e


def _fetch_messages(service, references):
    """Batch up to 50 Gmail gets; retry individual failures without dropping mail."""
    fetched = {}
    errors = set()

    def receive(request_id, response, exception):
        if exception:
            errors.add(request_id)
        else:
            fetched[request_id] = response

    for offset in range(0, len(references), 50):
        subset = references[offset : offset + 50]
        batch = service.new_batch_http_request(callback=receive)
        for ref in subset:
            batch.add(
                service.users()
                .messages()
                .get(userId="me", id=ref["id"], format="full"),
                request_id=ref["id"],
            )
        execute_batch(batch)
        for ref in subset:
            if ref["id"] in errors or ref["id"] not in fetched:
                fetched[ref["id"]] = _fetch_message(service, ref["id"])
        # Keep batched reads below the Gmail per-user quota; retries back off too.
        time.sleep(1)
    return [fetched[ref["id"]] for ref in references if fetched[ref["id"]] is not None]


def load_sync_state(s3):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=STATE_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {"last_history_id": None}


def save_sync_state(s3, state):
    state = {**state, "last_success_at": datetime.now(UTC).isoformat()}
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
        body = decode_data(payload["body"]["data"]).decode("utf-8", errors="replace")
    elif "parts" in payload:
        for part in payload["parts"]:
            if part.get("mimeType") == "text/plain" and part.get("body", {}).get(
                "data"
            ):
                body = decode_data(part["body"]["data"]).decode(
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


def prepare_message(service, s3, message):
    record = parse_message(message)
    if record is None:
        raise ValueError("Message date missing; checkpoint not advanced")
    attachments = store_attachments(service, s3, S3_BUCKET, S3_PREFIX, message)
    record["attachment_count"] = len(attachments)
    record["attachments_json"] = json.dumps(attachments)
    return record


def backfill_attachments(service, s3, max_seconds=14400):
    """Scan all MIME trees (including Trash/Spam); resume at committed page boundaries.

    Gmail's has:attachment filter misses some inline/unnamed calendar parts, so
    scan the mailbox but update only messages that actually contain attachments.
    Never modify the incremental Gmail history checkpoint.
    """
    state = read_json(s3, S3_BUCKET, BACKFILL_KEY) or {
        "scanned": 0,
        "emails_with_attachments": 0,
        "attachments": 0,
    }
    if state.get("complete"):
        print("Attachment backfill already complete; new mail uses regular sync.")
        return state
    started = time.monotonic()
    while True:
        kwargs = {"userId": "me", "maxResults": 100, "includeSpamTrash": True}
        if state.get("next_page_token"):
            kwargs["pageToken"] = state["next_page_token"]
        result = execute_request(service.users().messages().list(**kwargs))
        records = []
        count = 0
        for message in _fetch_messages(service, result.get("messages", [])):
            if list(attachment_parts(message.get("payload", {}))):
                record = prepare_message(service, s3, message)
                records.append(record)
                count += record["attachment_count"]
        with telemetry.operation("backfill.commit_page"):
            write_parquet(records, s3)
        telemetry.count("lake_messages_scanned", len(result.get("messages", [])))
        state["scanned"] += len(result.get("messages", []))
        state["emails_with_attachments"] += len(records)
        state["attachments"] += count
        state["next_page_token"] = result.get("nextPageToken")
        state["complete"] = not bool(state["next_page_token"])
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=BACKFILL_KEY,
            Body=json.dumps(state).encode(),
            ContentType="application/json",
        )
        print(
            f"Attachment backfill: scanned={state['scanned']}, "
            f"emails={state['emails_with_attachments']}, files={state['attachments']}, "
            f"complete={state['complete']}",
            flush=True,
        )
        telemetry.event(
            "backfill_progress",
            scanned=state["scanned"],
            emails_with_attachments=state["emails_with_attachments"],
            attachments=state["attachments"],
            complete=state["complete"],
        )
        if state["complete"] or time.monotonic() - started >= max_seconds:
            return state


def resume_pending_backfill(service, s3, max_seconds):
    state = read_json(s3, S3_BUCKET, BACKFILL_KEY)
    if state and not state.get("complete"):
        return backfill_attachments(service, s3, max_seconds)
    return state


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
            existing = pd.read_parquet(
                BytesIO(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
            )
            group = pd.concat([existing, group]).drop_duplicates(
                subset="gmail_id", keep="last"
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
                raise

        buf = group.to_parquet(index=False)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf)
        print(f"  wrote {len(group)} rows → s3://{S3_BUCKET}/{key}")

    telemetry.count("lake_email_rows_upserted", len(records))


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

        result = execute_request(service.users().messages().list(**kwargs))
        messages = result.get("messages", [])

        for msg_ref in messages:
            msg = _fetch_message(service, msg_ref["id"])
            if msg:
                rec = prepare_message(service, s3, msg)
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
        raise RuntimeError("No history ID found — run --full first")

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
            result = execute_request(service.users().history().list(**kwargs))
        except Exception as e:
            raise RuntimeError(
                "History API failed; checkpoint not advanced. A full sync may be needed."
            ) from e

        for history in result.get("history", []):
            for added in history.get("messagesAdded", []):
                msg = _fetch_message(service, added["message"]["id"])
                if msg:
                    rec = prepare_message(service, s3, msg)
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
            df = pd.read_parquet(
                BytesIO(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
            )
            before = len(df)
            df = df[~df["gmail_id"].isin(ids_set)]
            if len(df) < before:
                s3.put_object(
                    Bucket=S3_BUCKET, Key=key, Body=df.to_parquet(index=False)
                )
                print(f"  removed {before - len(df)} rows from {key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--full", action="store_true")
    mode.add_argument("--incremental", action="store_true")
    mode.add_argument("--attachment-backfill", action="store_true")
    mode.add_argument("--telemetry-check", action="store_true")
    parser.add_argument("--max-seconds", type=int, default=14400)
    parser.add_argument("--resume-pending-backfill", action="store_true")
    args = parser.parse_args()

    selected_mode = (
        "attachment-backfill"
        if args.attachment_backfill
        else "full"
        if args.full
        else "telemetry-check"
        if args.telemetry_check
        else "incremental"
    )
    telemetry.configure(selected_mode)
    try:
        with telemetry.operation("sync.run"):
            if args.telemetry_check:
                telemetry.event("telemetry_check_passed")
                telemetry.count("lake_telemetry_checks", 1)
            else:
                service = get_gmail_service()
                s3 = get_s3_client()
                if args.attachment_backfill:
                    progress = backfill_attachments(service, s3, args.max_seconds)
                    if os.getenv("GITHUB_STEP_SUMMARY"):
                        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
                            summary.write(
                                f"Attachment backfill: {progress['scanned']} messages scanned; "
                                f"{progress['emails_with_attachments']} emails with attachments; "
                                f"{progress['attachments']} files. "
                                + (
                                    "**Complete.**"
                                    if progress["complete"]
                                    else "**Paused at a checkpoint; re-run attachment-backfill to continue.**"
                                )
                                + "\n"
                            )
                elif args.full:
                    sync_full(service, s3)
                elif args.incremental:
                    if args.resume_pending_backfill:
                        resume_pending_backfill(service, s3, args.max_seconds)
                    sync_incremental(service, s3)
                else:
                    print("Use --full or --incremental")
        telemetry.event("run_finished")
        telemetry.count("lake_runs_finished", 1)
    except Exception:
        telemetry.event("run_failed")
        telemetry.count("lake_runs_failed", 1)
        raise
    finally:
        telemetry.close()
