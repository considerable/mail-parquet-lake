"""Retire only the old CloudWatch view and trace-write permission after migration."""

import argparse
import json

import boto3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    changes = {
        "delete_dashboard": "MailLake-Executive",
        "remove_trace_writer_permission": True,
        "preserve": [
            "mail and attachments",
            "CloudWatch logs",
            "historical metrics/traces",
            "four health alarms",
        ],
    }
    if args.apply:
        iam = boto3.client("iam")
        policy = iam.get_role_policy(
            RoleName="job-tracker-ci", PolicyName="mail-lake-opentelemetry"
        )["PolicyDocument"]
        policy["Statement"] = [
            s for s in policy["Statement"] if s.get("Sid") != "WriteLakeTraces"
        ]
        iam.put_role_policy(
            RoleName="job-tracker-ci",
            PolicyName="mail-lake-opentelemetry",
            PolicyDocument=json.dumps(policy),
        )
        boto3.client("cloudwatch", region_name="us-west-2").delete_dashboards(
            DashboardNames=["MailLake-Executive"]
        )
    print(json.dumps({"applied": args.apply, **changes}))


if __name__ == "__main__":
    main()
