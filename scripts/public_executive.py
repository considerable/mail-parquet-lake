"""Strict public projection: aggregate evidence, never mail or AWS identifiers."""

import math
from datetime import UTC, date, datetime, timedelta

try:
    from .executive_dashboard import classify
except ImportError:
    from executive_dashboard import classify

STATES = {
    "active",
    "disabled_inactivity",
    "disabled_manually",
    "disabled_fork",
    "deleted",
    "unknown",
}
HEALTH = {"HEALTHY", "ATTENTION", "CRITICAL", "UNKNOWN"}


def timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value)
        return dt.astimezone(UTC).isoformat() if dt.tzinfo else None
    except ValueError:
        return None


def number(value):
    return (
        value
        if type(value) in {int, float}
        and math.isfinite(value)
        and 0 <= value <= 2**53 - 1
        else None
    )


def selected(data, counts=(), times=(), booleans=()):
    if not isinstance(data, dict):
        return None
    result = {k: number(data.get(k)) for k in counts}
    result.update({k: timestamp(data.get(k)) for k in times})
    result.update(
        {k: data.get(k) if type(data.get(k)) is bool else None for k in booleans}
    )
    return result if all(v is not None for v in result.values()) else None


def daily_history(previous, snapshot):
    today = datetime.fromisoformat(snapshot["observed_at"]).date()
    rows = {}
    for row in previous[:100] if isinstance(previous, list) else []:
        if not isinstance(row, dict) or not isinstance(row.get("date"), str):
            continue
        try:
            day = date.fromisoformat(row["date"])
        except ValueError:
            continue
        if not today - timedelta(days=29) <= day <= today:
            continue
        values = {
            k: number(row.get(k)) for k in ["emails", "attachments", "storage_gib"]
        }
        if all(v is not None for v in values.values()):
            rows[day.isoformat()] = {"date": day.isoformat(), **values}
    inventory = snapshot.get("inventory")
    if (
        inventory
        and today - timedelta(days=29)
        <= datetime.fromisoformat(inventory["measured_at"]).date()
        <= today
    ):
        measured_day = datetime.fromisoformat(inventory["measured_at"]).date()
        rows[measured_day.isoformat()] = {
            "date": measured_day.isoformat(),
            **{k: inventory[k] for k in ["emails", "attachments", "storage_gib"]},
        }
    return [rows[k] for k in sorted(rows)]


def project(snapshot, previous=None):
    observed = timestamp(snapshot.get("observed_at"))
    if not observed:
        raise ValueError("Missing observation timestamp")
    clean = {
        "observed_at": observed,
        "source_errors": ["unavailable"] if snapshot.get("source_errors") else [],
        "sync": selected(snapshot.get("sync"), times=["last_success_at"]),
        "inventory": selected(
            snapshot.get("inventory"),
            counts=["emails", "attachments", "storage_gib"],
            times=["measured_at"],
        ),
        "backfill": selected(
            snapshot.get("backfill"),
            counts=["scanned", "attachments"],
            times=["updated_at"],
            booleans=["complete"],
        ),
        "reliability": selected(
            snapshot.get("reliability"),
            counts=["completed", "successful", "failed", "unclassified"],
        ),
    }
    state = (snapshot.get("workflow") or {}).get("state")
    clean["workflow"] = {"state": state} if state in STATES else None
    reliability = clean["reliability"]
    if (
        reliability
        and reliability["successful"] + reliability["failed"]
        != reliability["completed"]
    ):
        clean["reliability"] = None
        clean["source_errors"].append("unavailable")
    observed_time = datetime.fromisoformat(observed)
    for key, field in [
        ("sync", "last_success_at"),
        ("inventory", "measured_at"),
        ("backfill", "updated_at"),
    ]:
        if clean.get(key) and datetime.fromisoformat(
            clean[key][field]
        ) > observed_time + timedelta(minutes=5):
            clean[key] = None
    if any(
        clean.get(k) is None
        for k in ["sync", "inventory", "backfill", "workflow", "reliability"]
    ):
        clean["source_errors"].append("unavailable")
    health = classify(clean, datetime.fromisoformat(observed))
    return {
        "schema_version": 1,
        "observed_at": observed,
        "evidence_available": not bool(clean["source_errors"]),
        "health": {"status": health["status"], "reasons": health["reasons"]},
        **{
            k: clean[k]
            for k in ["sync", "inventory", "backfill", "workflow", "reliability"]
        },
        "history": daily_history(previous, clean),
        "billing_queries_enabled": False,
    }


def sanitize_public(data):
    """Re-project downloaded artifacts; never trust their free-form strings."""
    if not isinstance(data, dict) or data.get("schema_version") != 1:
        raise ValueError("Unsupported public snapshot")
    observed = timestamp(data.get("observed_at"))
    if not observed or datetime.fromisoformat(observed) > datetime.now(UTC) + timedelta(
        minutes=5
    ):
        raise ValueError("Invalid observation timestamp")
    source = {
        k: data.get(k)
        for k in [
            "observed_at",
            "sync",
            "inventory",
            "backfill",
            "workflow",
            "reliability",
        ]
    }
    source["source_errors"] = (
        [] if data.get("evidence_available") is True else ["unavailable"]
    )
    return project(source, data.get("history"))
