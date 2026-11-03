"""Executive health policy and four budget-limited CloudWatch alarm signals."""

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
    if health["sync_overdue"] is not None:
        values["SyncOverdue"] = (int(health["sync_overdue"]), "Count")

    return [
        {"MetricName": k, "Value": v, "Unit": unit} for k, (v, unit) in values.items()
    ]
