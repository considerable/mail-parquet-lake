"""Provision only the executive dashboard's publishing policy and four alarms."""

import argparse
import json
from pathlib import Path

import boto3

try:
    from .executive_dashboard import ALARMS, NAMESPACE, REGION
except ImportError:
    from executive_dashboard import ALARMS, NAMESPACE, REGION

SPECS = {
    "heartbeat": (
        "ObserverHeartbeat",
        "LessThanThreshold",
        0.5,
        2,
        "Executive observer has not published its heartbeat for two 15-minute periods. Older dashboard cards may be stale.",
    ),
    "freshness": (
        "SyncOverdue",
        "GreaterThanThreshold",
        0.5,
        1,
        "No successful mailbox-sync checkpoint satisfies the Gmail schedule plus its 90-minute completion allowance, or freshness evidence is missing.",
    ),
    "sources": (
        "SourceErrors",
        "GreaterThanThreshold",
        0.5,
        1,
        "Executive observer could not verify one or more operational sources. Missing evidence must not look healthy.",
    ),
    "workflow": (
        "SyncWorkflowActive",
        "LessThanThreshold",
        0.5,
        1,
        "Gmail Sync is disabled/removed, or its enablement state is unavailable.",
    ),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    account = boto3.client("sts").get_caller_identity()["Account"]
    policy = (
        (Path(__file__).resolve().parents[1] / "observability/executive-policy.json")
        .read_text()
        .replace("ACCOUNT_ID", account)
    )
    alarms = []
    for key, (metric, operator, threshold, periods, description) in SPECS.items():
        alarms.append(
            {
                "AlarmName": ALARMS[key],
                "AlarmDescription": description,
                "Namespace": NAMESPACE,
                "MetricName": metric,
                "Statistic": "Maximum",
                "Period": 900,
                "EvaluationPeriods": periods,
                "DatapointsToAlarm": periods,
                "Threshold": threshold,
                "ComparisonOperator": operator,
                "TreatMissingData": "breaching",
                "ActionsEnabled": False,
                "AlarmActions": [],
                "OKActions": [],
                "InsufficientDataActions": [],
            }
        )
    if args.apply:
        boto3.client("iam").put_role_policy(
            RoleName="job-tracker-ci",
            PolicyName="mail-lake-executive",
            PolicyDocument=policy,
        )
        cw = boto3.client("cloudwatch", region_name=REGION)
        for alarm in alarms:
            cw.put_metric_alarm(**alarm)
    print(
        json.dumps(
            {
                "applied": args.apply,
                "role_policy": "mail-lake-executive",
                "policy": json.loads(policy),
                "alarms": alarms,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
