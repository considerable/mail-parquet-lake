import io
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import collect_executive as collector
from scripts.executive_dashboard import build, classify, latest_due, metrics

NOW = datetime(2026, 11, 3, 19, 0, tzinfo=UTC)


def snapshot():
    return {
        "observed_at": NOW.isoformat(),
        "source_errors": [],
        "sync": {"last_success_at": (NOW - timedelta(minutes=20)).isoformat()},
        "workflow": {"state": "active"},
        "backfill": {
            "scanned": 80000,
            "attachments": 23000,
            "complete": True,
            "updated_at": NOW.isoformat(),
        },
        "inventory": {
            "measured_at": NOW.isoformat(),
            "emails": 98000,
            "attachments": 22000,
            "storage_gib": 2.5,
        },
        "reliability": {
            "completed": 2,
            "successful": 1,
            "failed": 1,
            "unclassified": 5,
        },
        "cost": {"known": False, "reason": "Project tag inactive"},
    }


def test_old_checkpoint_is_critical_even_while_backfill_is_progressing():
    data = snapshot()
    data["sync"]["last_success_at"] = (NOW - timedelta(days=13)).isoformat()
    data["backfill"]["complete"] = False
    health = classify(data, NOW)
    assert health["status"] == "CRITICAL"
    assert health["sync_overdue"] is True
    assert health["sync_age_hours"] == 13 * 24


def test_missing_sources_never_show_healthy_or_zero_freshness():
    data = snapshot()
    data["sync"] = None
    data["source_errors"] = ["sync_checkpoint"]
    health = classify(data, NOW)
    assert health["status"] == "UNKNOWN"
    assert health["sync_age_hours"] is None
    assert "SyncAgeHours" not in {m["MetricName"] for m in metrics(data, health)}
    assert "Unknown" in json.dumps(build(data, health, "123456789012"))


def test_healthy_requires_current_sync_complete_backfill_and_inventory():
    assert classify(snapshot(), NOW)["status"] == "HEALTHY"
    data = snapshot()
    data["backfill"]["complete"] = False
    assert classify(data, NOW)["status"] == "ATTENTION"
    data["workflow"]["state"] = "disabled_inactivity"
    assert classify(data, NOW)["status"] == "CRITICAL"


def test_schedule_accounts_for_weekend_and_grace_period():
    # Sunday has only the 10:00 UTC slot. Monday has no 00:00 UTC slot.
    monday = datetime(2026, 11, 2, 10, 30, tzinfo=UTC)
    assert latest_due(monday) == datetime(2026, 11, 1, 10, tzinfo=UTC)
    assert latest_due(monday + timedelta(hours=2)) == datetime(
        2026, 11, 2, 10, tzinfo=UTC
    )


def test_no_completed_sync_runs_is_unknown_not_100_percent():
    data = snapshot()
    data["reliability"] = {
        "completed": 0,
        "successful": 0,
        "failed": 0,
        "unclassified": 10,
    }
    body = json.dumps(build(data, classify(data, NOW), "123456789012"))
    assert "no classified, completed" in body
    assert "ObservedSyncSuccessPercent" not in {
        m["MetricName"] for m in metrics(data, classify(data, NOW))
    }


def test_dashboard_does_not_expose_unselected_private_fields_or_fake_cost():
    data = snapshot()
    data["email_subject"] = "VERY-PRIVATE-SUBJECT"
    body = json.dumps(build(data, classify(data, NOW), "123456789012"))
    assert "VERY-PRIVATE-SUBJECT" not in body
    assert "Not yet attributable" in body
    assert "$0.00" not in body


def test_inactive_billing_tag_does_not_query_or_invent_zero_spend():
    ce = MagicMock()
    ce.list_cost_allocation_tags.return_value = {
        "CostAllocationTags": [{"Status": "Inactive"}]
    }
    assert collector.project_cost(ce, NOW)["known"] is False
    ce.get_cost_and_usage.assert_not_called()


def test_reliability_excludes_probes_cancelled_unknown_and_old_runs(monkeypatch):
    def run(number, conclusion, title="Gmail Sync", when=NOW):
        return {
            "id": number,
            "status": "completed",
            "conclusion": conclusion,
            "display_title": title,
            "created_at": when.isoformat(),
        }

    runs = [
        run(1, "success"),
        run(2, "failure"),
        run(3, "success"),
        run(4, "cancelled"),
        run(5, "success"),
        run(6, "success", "Gmail Sync (incremental)"),
        run(7, "success", "Gmail Sync (incremental)", NOW - timedelta(days=31)),
    ]
    monkeypatch.setattr(collector, "github", lambda *a, **kw: {"workflow_runs": runs})
    data = collector.reliability(
        "owner/repo",
        {
            "1": "incremental",
            "2": "attachment-backfill",
            "3": "telemetry-check",
            "4": "full",
        },
        NOW,
    )
    assert data == {"completed": 3, "successful": 2, "failed": 1, "unclassified": 1}


def test_inventory_counts_legacy_and_new_partitions_without_exporting_mail():
    def parquet(table):
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return buffer.getvalue()

    old = parquet(pa.table({"body_text": ["PRIVATE"]}))
    new = parquet(
        pa.table({"body_text": ["SECRET", "SECRET"], "attachment_count": [2, 3]})
    )
    objects = {
        "gmail/year=2020/month=01/emails.parquet": old,
        "gmail/year=2026/month=11/emails.parquet": new,
        "gmail/attachments/mail/part/blob": b"file",
        "gmail/reports/not-data.json": b"operational",
    }
    s3 = MagicMock()
    s3.get_paginator().paginate.return_value = [
        {"Contents": [{"Key": k, "Size": len(v)} for k, v in objects.items()]}
    ]
    s3.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(objects[kw["Key"]])}
    result = collector.inventory(s3, "private", "gmail", NOW)
    assert result["emails"] == 3 and result["attachments"] == 5
    assert result["storage_gib"] == (len(old) + len(new) + 4) / 1024**3
    assert "PRIVATE" not in json.dumps(result) and "SECRET" not in json.dumps(result)


def test_dashboard_validation_failure_cannot_publish_success_heartbeat():
    cw, s3 = MagicMock(), MagicMock()
    cw.put_dashboard.return_value = {
        "DashboardValidationMessages": [{"Message": "bad widget"}]
    }
    with pytest.raises(ValueError):
        collector.publish(s3, cw, "private", "gmail", snapshot(), "123456789012")
    cw.put_metric_data.assert_not_called()
