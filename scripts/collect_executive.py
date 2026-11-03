"""Collect aggregate evidence for GitHub Pages and four CloudWatch health alarms."""

import argparse
import io
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from botocore.exceptions import BotoCoreError, ClientError

try:
    from .executive_dashboard import NAME, NAMESPACE, REGION, classify, metrics
    from .public_executive import project
except ImportError:
    from executive_dashboard import NAME, NAMESPACE, REGION, classify, metrics
    from public_executive import project


MODES = {"full", "incremental", "attachment-backfill", "telemetry-check"}


def stamp(value):
    return value.astimezone(UTC).isoformat()


def read_json(s3, bucket, key):
    try:
        result = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return None, None
        raise
    return json.loads(result["Body"].read()), result["LastModified"]


def inventory(s3, bucket, prefix, now):
    objects = [
        o
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix + "/"
        )
        for o in page.get("Contents", [])
    ]
    partitions = [
        o
        for o in objects
        if o["Key"].startswith(prefix + "/year=") and o["Key"].endswith(".parquet")
    ]
    if not partitions:
        raise ValueError("No lake partitions available")

    def read_partition(obj):
        data = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
        parquet = pq.ParquetFile(io.BytesIO(data))
        total = parquet.metadata.num_rows
        linked = 0
        if "attachment_count" in parquet.schema_arrow.names:
            counts = parquet.read(columns=["attachment_count"])["attachment_count"]
            linked = int(pc.sum(counts).as_py() or 0)
        return total, linked

    with ThreadPoolExecutor(max_workers=8) as pool:
        counts = list(pool.map(read_partition, partitions))
    stored = sum(
        o["Size"]
        for o in objects
        if o["Key"].startswith(prefix + "/attachments/") or o in partitions
    )
    return {
        "measured_at": stamp(now),
        "emails": sum(x[0] for x in counts),
        "attachments": sum(x[1] for x in counts),
        "storage_gib": stored / 1024**3,
        "partitions": len(partitions),
    }


def github(path, params=None):
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = "Bearer " + token
    response = requests.get(
        "https://api.github.com/" + path,
        params=params,
        headers=headers,
        timeout=30,
        allow_redirects=False,
    )
    if 300 <= response.status_code < 400:
        raise ValueError("Unexpected GitHub redirect")
    response.raise_for_status()
    return response.json()


def run_modes(logs, now):
    result = {}
    kwargs = {
        "logGroupName": "/mail-parquet-lake/otel",
        "filterPattern": '{ $.body = "run_started" }',
        "startTime": int((now - timedelta(days=30)).timestamp() * 1000),
    }
    for page in logs.get_paginator("filter_log_events").paginate(**kwargs):
        for event in page.get("events", []):
            item = json.loads(event["message"]).get("attributes", {})
            if item.get("mode") in MODES and item.get("github_run_id"):
                result[str(item["github_run_id"])] = item["mode"]
    return result


def reliability(repository, modes, now):
    runs = []
    for page in range(1, 11):
        response = github(
            f"repos/{repository}/actions/workflows/gmail-sync.yml/runs",
            {
                "per_page": 100,
                "page": page,
                "created": ">=" + (now - timedelta(days=30)).date().isoformat(),
            },
        )
        batch = response.get("workflow_runs", [])
        runs.extend(batch)
        if len(batch) < 100:
            break
    else:
        raise ValueError("Run history truncated; reliability unavailable")
    totals = {"completed": 0, "successful": 0, "failed": 0, "unclassified": 0}
    for run in runs:
        if datetime.fromisoformat(run["created_at"]) < now - timedelta(days=30):
            continue
        mode = modes.get(str(run["id"]))
        if not mode:
            title = run.get("display_title", "")
            mode = next((m for m in MODES if title == f"Gmail Sync ({m})"), None)
        if not mode:
            totals["unclassified"] += 1
            continue
        if (
            mode == "telemetry-check"
            or run.get("status") != "completed"
            or run.get("conclusion") in {"cancelled", "skipped", "neutral"}
        ):
            continue
        if run.get("conclusion") not in {
            "success",
            "failure",
            "timed_out",
            "action_required",
            "startup_failure",
            "stale",
        }:
            continue
        totals["completed"] += 1
        totals["successful" if run["conclusion"] == "success" else "failed"] += 1
    return totals


def collect(s3, logs, bucket, prefix, repository, now, force_inventory=False):
    snapshot = {"schema_version": 1, "observed_at": stamp(now), "source_errors": []}

    def source(name, operation):
        try:
            return operation()
        except (
            BotoCoreError,
            ClientError,
            requests.RequestException,
            ValueError,
            KeyError,
            TypeError,
            OSError,
        ):
            # Never publish raw exception messages, bucket paths or API payloads.
            snapshot["source_errors"].append(name)
            return None

    def sync_state():
        data, modified = read_json(s3, bucket, prefix + "/sync_state.json")
        if data is None or modified is None or not data.get("last_history_id"):
            raise ValueError("No sync checkpoint")
        value = data.get("last_success_at") or stamp(modified)
        when = datetime.fromisoformat(value)
        if when.tzinfo is None or when > now + timedelta(minutes=5):
            raise ValueError("Invalid sync timestamp")
        return {
            "last_success_at": value,
            "timestamp_source": "explicit"
            if data.get("last_success_at")
            else "checkpoint_modified",
        }

    snapshot["sync"] = source("sync_checkpoint", sync_state)

    def backfill_state():
        data, modified = read_json(
            s3, bucket, prefix + "/attachment_backfill_state.json"
        )
        if data is None or modified is None:
            raise ValueError("No backfill state")
        return {
            k: data[k]
            for k in ["scanned", "emails_with_attachments", "attachments", "complete"]
        } | {"updated_at": stamp(modified)}

    snapshot["backfill"] = source("backfill_checkpoint", backfill_state)
    snapshot["workflow"] = source(
        "github_workflow",
        lambda: {
            "state": github(f"repos/{repository}/actions/workflows/gmail-sync.yml")[
                "state"
            ]
        },
    )
    modes = source("run_classification", lambda: run_modes(logs, now))
    snapshot["reliability"] = source(
        "github_history", lambda: reliability(repository, modes or {}, now)
    )

    def lake_inventory():
        prior, _ = read_json(s3, bucket, prefix + "/reports/executive-inventory.json")
        # Daily inventory keeps observation reads bounded in the low-cost profile.
        max_age = timedelta(hours=24)
        if prior:
            measured = datetime.fromisoformat(prior["measured_at"])
            if measured.tzinfo is None or measured > now + timedelta(minutes=5):
                raise ValueError("Invalid inventory timestamp")
            if any(
                not isinstance(prior.get(k), (int, float)) or prior[k] < 0
                for k in ["emails", "attachments", "storage_gib"]
            ):
                raise ValueError("Invalid inventory counts")
        if (
            force_inventory
            or not prior
            or now - datetime.fromisoformat(prior["measured_at"]) >= max_age
        ):
            return inventory(s3, bucket, prefix, now)
        return prior

    snapshot["inventory"] = source("lake_inventory", lake_inventory)
    previous, _ = source(
        "executive_cache",
        lambda: read_json(s3, bucket, prefix + "/reports/executive-status.json"),
    ) or (None, None)
    snapshot["daily_history"] = (previous or {}).get("daily_history", [])

    return snapshot


def publish(s3, cw, bucket, prefix, snapshot):
    now = datetime.fromisoformat(snapshot["observed_at"])
    health = classify(snapshot, now)
    data = [{**m, "Timestamp": now} for m in metrics(snapshot, health)]
    public = project(snapshot, snapshot.get("daily_history"))
    snapshot = {**snapshot, "daily_history": public["history"]}
    # Snapshot includes only explicitly selected aggregate evidence, never mail.
    if snapshot.get("inventory"):
        s3.put_object(
            Bucket=bucket,
            Key=prefix + "/reports/executive-inventory.json",
            Body=json.dumps(snapshot["inventory"]).encode(),
            ContentType="application/json",
        )
    s3.put_object(
        Bucket=bucket,
        Key=prefix + "/reports/executive-status.json",
        Body=json.dumps(snapshot).encode(),
        ContentType="application/json",
    )
    # Only four independent alarm signals remain in CloudWatch. Business cards
    # and trends are rendered from the public-safe artifact in GitHub Pages.
    cw.put_metric_data(Namespace=NAMESPACE, MetricData=data)
    return health


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--refresh-inventory", action="store_true")
    parser.add_argument("--output", default="/tmp/mail-lake-executive.json")
    parser.add_argument("--public-output", default="/tmp/executive-public.json")
    args = parser.parse_args()
    bucket, prefix = (
        os.environ["S3_BUCKET"],
        os.environ.get("S3_PREFIX", "gmail").rstrip("/"),
    )
    repository = os.getenv("GITHUB_REPOSITORY", "considerable/mail-parquet-lake")
    now = datetime.now(UTC)
    s3, logs, cw = (
        boto3.client(s, region_name=REGION) for s in ["s3", "logs", "cloudwatch"]
    )
    snapshot = collect(
        s3, logs, bucket, prefix, repository, now, args.refresh_inventory
    )
    health = classify(snapshot, now)
    Path(args.output).write_text(
        json.dumps(
            {
                "snapshot": snapshot,
                "health": health,
                "public": project(snapshot, snapshot.get("daily_history")),
            },
            indent=2,
        )
        + "\n"
    )
    public = project(snapshot, snapshot.get("daily_history"))
    Path(args.public_output).write_text(json.dumps(public, allow_nan=False) + "\n")
    if args.publish:
        publish(s3, cw, bucket, prefix, snapshot)
    print(
        json.dumps(
            {
                "dashboard": NAME,
                "health": health["status"],
                "unavailable_sources": snapshot["source_errors"],
                "published": args.publish,
            }
        )
    )
    # Dashboard/alarms expose unhealthy lake state; it does not make the observer
    # itself fail if it successfully collected and reported that state.


if __name__ == "__main__":
    main()
