"""Executive policy and native CloudWatch widgets; aggregate data only."""

from datetime import datetime, timedelta

NAME = "MailLake-Executive"
NAMESPACE = "MailParquetLake/Executive"
REGION = "us-west-2"
ALARMS = {
    "heartbeat": "MailLake-Executive-ObserverMissing",
    "freshness": "MailLake-Executive-SyncOverdue",
    "sources": "MailLake-Executive-SourceUnavailable",
    "workflow": "MailLake-Executive-SyncDisabled",
}


def latest_due(now):
    """Existing UTC Gmail cron slots, with 90 minutes to finish a pass."""
    deadline = now - timedelta(minutes=90)
    candidates = []
    for offset in range(8):
        day = deadline - timedelta(days=offset)
        hours = {10}
        if day.weekday() < 5:
            hours.update([14, 18, 22])
        if 1 <= day.weekday() <= 5:
            hours.add(0)
        candidates.extend(
            day.replace(hour=h, minute=0, second=0, microsecond=0) for h in hours
        )
    return max(t for t in candidates if t <= deadline)


def classify(snapshot, now):
    reasons = []
    critical = False
    sync = snapshot.get("sync")
    if sync:
        age = max(
            0,
            (now - datetime.fromisoformat(sync["last_success_at"])).total_seconds()
            / 3600,
        )
        overdue = datetime.fromisoformat(sync["last_success_at"]) < latest_due(now)
        if overdue:
            critical = True
            reasons.append(
                "Mailbox freshness is overdue; attachment backfill progress does not establish a current mailbox."
            )
    else:
        age, overdue = None, None
        reasons.append("No usable successful-sync checkpoint is available.")
    workflow = snapshot.get("workflow")
    if workflow and workflow.get("state") != "active":
        critical = True
        reasons.append("The Gmail Sync workflow is disabled or removed.")
    backfill = snapshot.get("backfill")
    if backfill and not backfill.get("complete"):
        reasons.append("Historical attachment backfill is incomplete.")
    inventory = snapshot.get("inventory")
    inventory_age = None
    if inventory:
        inventory_age = max(
            0,
            (now - datetime.fromisoformat(inventory["measured_at"])).total_seconds()
            / 3600,
        )
        if inventory_age > 26:
            reasons.append("The lake inventory is older than 26 hours.")
    unknown = (
        bool(snapshot.get("source_errors"))
        or not sync
        or not workflow
        or not inventory
        or not backfill
    )
    if unknown:
        reasons.append(
            "Some operational evidence is unavailable; healthy status cannot be established."
        )
    status = (
        "CRITICAL"
        if critical
        else "UNKNOWN"
        if unknown
        else "ATTENTION"
        if reasons
        else "HEALTHY"
    )
    return {
        "status": status,
        "reasons": reasons,
        "sync_age_hours": age,
        "sync_overdue": overdue,
        "inventory_age_hours": inventory_age,
    }


def metrics(snapshot, health):
    values = {
        "ObserverHeartbeat": (1, "Count"),
        "SourceErrors": (len(snapshot.get("source_errors", [])), "Count"),
    }
    if snapshot.get("workflow"):
        values["SyncWorkflowActive"] = (
            int(snapshot["workflow"]["state"] == "active"),
            "Count",
        )
    if health["sync_age_hours"] is not None:
        values["SyncAgeHours"] = (health["sync_age_hours"], "None")
        values["SyncOverdue"] = (int(health["sync_overdue"]), "Count")
    inventory = snapshot.get("inventory")
    if inventory:
        for name, field in [
            ("EmailsStored", "emails"),
            ("AttachmentsLinked", "attachments"),
            ("LakeStorageGiB", "storage_gib"),
        ]:
            values[name] = (inventory[field], "None")
        values["InventoryAgeHours"] = (health["inventory_age_hours"], "None")
    if snapshot.get("backfill"):
        for name, field in [
            ("BackfillMessagesScanned", "scanned"),
            ("BackfillAttachmentsCaptured", "attachments"),
        ]:
            values[name] = (snapshot["backfill"][field], "Count")
    reliability = snapshot.get("reliability")
    if reliability and reliability["completed"]:
        values["ObservedSyncSuccessPercent"] = (
            100 * reliability["successful"] / reliability["completed"],
            "Percent",
        )
        values["ObservedSyncFailures30d"] = (reliability["failed"], "Count")
    return [
        {"MetricName": k, "Value": v, "Unit": unit} for k, (v, unit) in values.items()
    ]


def build(snapshot, health, account):
    widgets = []

    def text(title, body, x, y, width, height):
        widgets.append(
            {
                "type": "text",
                "x": x,
                "y": y,
                "width": width,
                "height": height,
                "properties": {"markdown": f"## {title}\n\n{body}"},
            }
        )

    def number(title, metric, x, unit=None):
        properties = {
            "title": title,
            "region": REGION,
            "view": "singleValue",
            "period": 900,
            "stat": "Maximum",
            "metrics": [[NAMESPACE, metric]],
            "sparkline": False,
            "setPeriodToTimeRange": False,
            "start": "-PT1H",
            "end": "P0D",
        }
        if unit:
            properties["metrics"][0].append({"label": unit})
        widgets.append(
            {
                "type": "metric",
                "x": x,
                "y": 8,
                "width": 6,
                "height": 4,
                "properties": properties,
            }
        )

    icons = {"HEALTHY": "🟢", "ATTENTION": "🟠", "CRITICAL": "🔴", "UNKNOWN": "⚪"}
    text(
        f"Mail Lake — {icons[health['status']]} {health['status'].title()}",
        f"**Observed at {snapshot['observed_at']} UTC.** Refreshes every 15 minutes.\n\n"
        + (
            health["reasons"][0]
            if health["reasons"]
            else "Mailbox sync is current, inventory is available, and attachment backfill is complete."
        )
        + "\n\nCheck the observer alarm below: a stopped collector leaves older cards on screen.",
        0,
        0,
        24,
        5,
    )
    widgets.append(
        {
            "type": "alarm",
            "x": 0,
            "y": 5,
            "width": 24,
            "height": 3,
            "properties": {
                "title": "Independent health checks",
                "alarms": [
                    f"arn:aws:cloudwatch:{REGION}:{account}:alarm:{n}"
                    for n in ALARMS.values()
                ],
                "sortBy": "stateUpdatedTimestamp",
            },
        }
    )
    inventory = snapshot.get("inventory")
    if inventory:
        number("Emails in the lake", "EmailsStored", 0)
        number("Attachments linked to emails", "AttachmentsLinked", 6)
        number("Lake storage (GiB)", "LakeStorageGiB", 12)
    else:
        for x, title in [
            (0, "Emails in the lake"),
            (6, "Linked attachments"),
            (12, "Lake storage"),
        ]:
            text(title, "**Unknown** — no usable inventory snapshot.", x, 8, 6, 4)
    if health["sync_age_hours"] is not None:
        number("Hours since successful mailbox sync", "SyncAgeHours", 18)
    else:
        text(
            "Mailbox freshness", "**Unknown** — no successful checkpoint.", 18, 8, 6, 4
        )
    backfill = snapshot.get("backfill")
    if backfill:
        body = f"**{'Complete' if backfill['complete'] else 'Incomplete'}**\n\n{backfill['scanned']:,} messages scanned · {backfill['attachments']:,} attachments captured.\n\nLast committed progress: {backfill['updated_at']} UTC. No percentage is estimated without a verified total."
    else:
        body = "**Unknown** — backfill state could not be read."
    text("Attachment completeness", body, 0, 12, 12, 5)
    reliability = snapshot.get("reliability")
    if reliability and reliability["completed"]:
        rate = 100 * reliability["successful"] / reliability["completed"]
        body = f"**{rate:.1f}%** of {reliability['completed']} classified, completed sync passes succeeded; {reliability['failed']} failed.\n\nConnection tests, cancelled and running jobs are excluded. {reliability['unclassified']} legacy runs lack a reliable mode and are excluded. A successful pass can still leave backfill unfinished."
    else:
        body = "**Unknown** — no classified, completed real-sync runs in the last 30 days. Connection tests never count as successful mail synchronization."
    text("Observed execution reliability — 30 days", body, 12, 12, 12, 5)
    for x, title, metric in [
        (0, "Daily email indexing activity", "lake_email_rows_upserted"),
        (12, "Daily attachment capture", "lake_attachments_stored"),
    ]:
        widgets.append(
            {
                "type": "metric",
                "x": x,
                "y": 17,
                "width": 12,
                "height": 6,
                "properties": {
                    "title": title,
                    "region": REGION,
                    "view": "timeSeries",
                    "stat": "Sum",
                    "period": 86400,
                    "metrics": [
                        ["MailParquetLake", metric, "mode", mode, {"label": label}]
                        for mode, label in [
                            ("incremental", "New mail"),
                            ("full", "Full import"),
                            ("attachment-backfill", "Historical attachments"),
                        ]
                    ],
                    "yAxis": {"left": {"min": 0}},
                },
            }
        )
    cost = snapshot.get("cost", {})
    if cost.get("known"):
        body = f"**${cost['amount']:.2f} USD** tagged AWS spend this month through {cost['through']}.\n\nProject=mail-parquet-lake only. Shared and untagged charges are excluded. AWS billing may be delayed or estimated; this is not the complete bill."
    else:
        body = (
            "**Not yet attributable**\n\n"
            + cost.get("reason", "No verified project billing allocation is available.")
            + "\n\nNo $0 is inferred. No account-wide billing settings are changed by this dashboard."
        )
    text("Month-to-date allocated cost", body, 0, 23, 12, 5)
    text(
        "Needs attention",
        "\n".join("- " + r for r in health["reasons"])
        or "No operational action indicated by the available evidence.",
        12,
        23,
        12,
        5,
    )
    measured = inventory["measured_at"] if inventory else "unavailable"
    text(
        "Evidence and ownership",
        f"Inventory measured: **{measured} UTC**. Inventory may lag a running backfill; counts are not an estimated mailbox coverage percentage. Indexing charts count operations and may include reprocessing.\n\n[Sync runs](https://github.com/considerable/mail-parquet-lake/actions/workflows/gmail-sync.yml) · [Setup and definitions](https://github.com/considerable/mail-parquet-lake/blob/main/docs/observability.md)\n\nManaged from the repository; console edits are replaced at the next refresh. Private aggregate view; no email subjects, addresses or filenames.",
        0,
        28,
        24,
        4,
    )
    return {"start": "-P30D", "periodOverride": "inherit", "widgets": widgets}
