# OpenTelemetry setup for mail-parquet-lake

Gmail Sync emits explicit OpenTelemetry signals over OTLP/HTTP to a Collector
on its GitHub runner. The Collector uses the workflow's temporary OIDC role
credentials. There are no new access keys or bearer tokens.

This setup instruments **Gmail Sync**: full imports, incremental sync and
attachment backfill. It does not automatically instrument Job Tracker, the
GitHub Pages collector, or GitHub Actions platform events.

## Contents

- [Architecture](#architecture)
- [Executive dashboard](#executive-dashboard)
- [Destinations](#destinations)
- [Workflow lifecycle and configuration](#workflow-lifecycle-and-configuration)
- [Signals and meaning](#signals-and-meaning)
- [Reliability and limitations](#reliability-and-limitations)
- [Verify and query](#verify-and-query)
- [Troubleshooting](#troubleshooting)
- [Infrastructure](#infrastructure)
- [Implementation files](#implementation-files)

## Architecture

```mermaid
flowchart LR
    SYNC["Python Gmail Sync"] -->|OTLP HTTP on localhost:4318| COL[OpenTelemetry Collector]
    COL -->|Signed OTLP logs| LOGS[CloudWatch Logs]
    COL -->|Embedded Metric Format| EMF[CloudWatch Logs: metrics stream]
    EMF --> METRICS[CloudWatch Metrics: MailParquetLake]
    COL -->|PutTraceSegments| XRAY[AWS X-Ray]
    OIDC[GitHub OIDC: job-tracker-ci role] -->|Temporary AWS credentials| COL
```

The Python SDK produces the telemetry. The Collector batches and routes it;
CloudWatch and X-Ray store and query it. The Collector is not the dashboard or
the long-term store. The email lake itself continues to store mail and attachment
bytes in S3, independently of telemetry.

## Executive dashboard

[Open **MailLake-Executive** in CloudWatch](https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#dashboards/dashboard/MailLake-Executive).
Select `us-west-2`. The private dashboard contains aggregate counts and health
evidence, without email subjects, addresses, filenames, attachment bytes or S3 keys.

```mermaid
flowchart LR
    S3["Private S3 checkpoints and inventory"] --> OBS["Separate GitHub executive observer"]
    GH["Workflow state and run metadata"] --> OBS
    LOGS["Content-free run-mode events"] --> OBS
    BILL["Billing allocation and tagged spend"] --> OBS
    OBS --> DASH["MailLake-Executive dashboard"]
    OBS --> METRICS["MailParquetLake/Executive metrics"]
    METRICS --> ALARMS["CloudWatch evaluates alarms independently"]
    STOP["Sync or observer stops"] -.-> ALARMS
```

### What the indicators mean

| Indicator | Evidence and interpretation |
|---|---|
| Overall status | `CRITICAL` for overdue sync or a disabled workflow; `UNKNOWN` for unavailable operational evidence; `ATTENTION` for unfinished backfill or inventory older than 26 hours; `HEALTHY` requires current sync, active workflow, recent inventory and completed backfill |
| Emails stored | Row counts across monthly Parquet files, not an estimate of Gmail mailbox size |
| Attachments linked | Sum of committed email rows' `attachment_count`; excludes partially uploaded/unlinked blobs. Legacy null fields contribute no known attachments, so the total can undercount eventual coverage while backfill is unfinished |
| Lake storage | S3 object bytes for email Parquet and attachments including manifests, in GiB; excludes reports and other operational files |
| Mailbox freshness | Hours since successful full/incremental sync; neither backfill progress nor a connection test resets it |
| Attachment completeness | Persisted scanned/file counts, completion flag and last progress timestamp; no percentage is estimated without a verified denominator |
| Observed reliability | Classified real-sync successes/failures over 30 days; excludes probes, cancelled/running/skipped jobs and legacy runs with unknown mode |
| Daily activity | OpenTelemetry indexing/capture counters by mode; reprocessing can count work again, and missing periods are not filled with healthy zeros |
| Allocated cost | Tagged `Project=mail-parquet-lake` unblended spend through the last complete billing day; shared/untagged charges are excluded and AWS data may be delayed or estimated |

The freshness deadline follows the existing Gmail UTC schedule plus a **90-minute
completion allowance**: every day at 10:00 UTC, weekdays at 14:00/18:00/22:00 UTC,
and Tuesday–Saturday at 00:00 UTC. This accommodates overnight/weekend gaps. Change
`latest_due()` and its tests if the Gmail schedule changes.

New successful full/incremental checkpoints include `last_success_at`. For legacy
checkpoints, the S3 last-modified timestamp is used as an explicitly inferred
fallback; a manual rewrite would affect that inference. No timestamp is reset to
make the dashboard green. New GitHub run titles include their mode; older runs are
classified only when `run_started` telemetry supplies a run ID/mode. The dashboard
reports excluded legacy runs. No completed real runs means **unknown**, not 100%.
A successful backfill pass can still leave historical work unfinished.

### Scheduled collection and alarms

[Executive Observability](../.github/workflows/executive.yml) runs every 15 minutes
at UTC minutes 11/26/41/56, after Gmail Sync finishes, and manually. It has a
separate concurrency group and always runs trusted `main` source. It can detect
Gmail being disabled without depending on the sync process. If GitHub or the
observer itself stops, CloudWatch still evaluates the missing-heartbeat alarms.
No permanent AWS compute server is required.

Inventory refreshes hourly during unfinished backfill, daily afterward, after a
Gmail completion, or on an explicit refresh request. The measurement spans multiple
S3 reads while writers can advance; its timestamp is shown and it is not an atomic
bucket snapshot. Private caches are `<prefix>/reports/executive-inventory.json`
and `<prefix>/reports/executive-status.json`. The observer never modifies mail,
attachment objects or ingestion checkpoints and never accesses Gmail.

Four console-only alarms use `MailParquetLake/Executive`, 15-minute periods and
**missing data as breaching**:

| Alarm | Condition |
|---|---|
| `MailLake-Executive-ObserverMissing` | Missing/below-one heartbeat for two periods |
| `MailLake-Executive-SyncOverdue` | Scheduled freshness deadline missed, or freshness evidence missing |
| `MailLake-Executive-SourceUnavailable` | Required source reads fail, or observer evidence disappears |
| `MailLake-Executive-SyncDisabled` | Gmail workflow not active, or its enablement evidence missing |

**No SNS subscriptions, email or other alarm actions are configured.** Alarm state
is visible on the dashboard; a notification route needs a separate explicit
choice. Scheduling/evaluation can delay alarms; this is not instantaneous detection.
If collection stops, old cards remain visible with their observation timestamp;
the independent alarms expose that staleness. Failed dashboard validation or
publishing cannot emit a new successful heartbeat. Source failures publish an
unavailable-source count without raw exceptions or private API payloads.

### Cost and permissions

At initial setup, the `Project` billing allocation tag is inactive and the bucket
has no verified project-allocation tags. Cost therefore shows **Not yet attributable**,
not $0. The collector does not activate account-wide billing tags, tag unrelated
resources, reconstruct historical charges or claim the tagged subtotal is a
complete project bill. Billing status/results are cached daily. Custom metrics,
alarms, dashboard/API usage, inventory reads and any Cost Explorer calls follow
normal AWS pricing; no zero-cost guarantee is made.

[executive-policy.json](../observability/executive-policy.json) adds a role policy
named `mail-lake-executive`. Metrics writes are restricted to the executive
namespace, dashboard writes to this one dashboard, and telemetry reads to the
mail-lake group. It also permits billing-allocation/usage reads and reuses existing
scoped S3 permissions. No Gmail permission or new access key is added. The observer
does not have alarm-creation permissions; alarms are a deployment action.

### Deployment and verification

Implementation: [collect_executive.py](../scripts/collect_executive.py),
[executive_dashboard.py](../scripts/executive_dashboard.py),
[deploy_executive.py](../scripts/deploy_executive.py) and
[tests/test_executive.py](../tests/test_executive.py).
From the repository root with the existing S3/AWS environment:

```sh
# Preview policy and alarm definitions without AWS changes.
python scripts/deploy_executive.py
# Provision only this feature's policy and console-only alarms.
python scripts/deploy_executive.py --apply
# Prepare a live aggregate preview without publishing.
python scripts/collect_executive.py --refresh-inventory --output /tmp/mail-lake-executive.json
# Publish dashboard, private snapshots and metrics.
python scripts/collect_executive.py --publish --refresh-inventory
```

Use **Actions → Executive Observability → Run workflow**, optionally selecting
`refresh_inventory`, for an immediate scheduled-role run. `GH_TOKEN` can be used
locally for GitHub API rate limits. Verify a successful observer job, fresh metric
datapoints, and alarm states separately; alarm evaluation may lag first publication.
Console edits are replaced on refresh. Do not place aggregate snapshots/dashboard
JSON into the public GitHub Pages directory.

[infra/executive.tf](../infra/executive.tf) declares the same resources. Terraform
creates an initial unknown-state placeholder and ignores subsequent body changes
because the observer owns the rendered evidence. Import directly provisioned
resources before managing them with Terraform. Do not apply all Terraform blindly:
the separately proposed KMS encryption change remains a different approval decision.

## Destinations

- **Logs:** `/mail-parquet-lake/otel` in `us-west-2`, stream
  `github/<run-id>/<attempt>`. Fixed event names plus counts, mode and run ID.
- **Metrics:** OpenTelemetry counters/histograms are converted to CloudWatch
  Embedded Metric Format (EMF) in the same group, stream `metrics`. CloudWatch
  extracts them into namespace `MailParquetLake`. This deliberately uses EMF
  rather than the separate native OTLP metrics store to keep metric source data
  in the requested log group and support classic CloudWatch dashboards.
- **Traces:** the Collector's AWS X-Ray exporter sends to the existing regional
  X-Ray destination. These are not stored in the chosen log group. Logs emitted
  within spans carry trace/span context for correlation. No account-wide
  Transaction Search destination change is required or made.

Log retention is 30 days. Terraform now declares a customer-managed KMS key
`alias/mail-parquet-lake-otel`, scoped to this group, with automatic key rotation.
The live group still uses AWS-managed encryption until the proposed key and IAM
changes receive explicit approval and are applied. Existing log events retain
their original encryption when a key is associated. This does not set metric or trace
retention. Ingestion, custom metrics and trace storage can incur AWS charges.

## Workflow lifecycle and configuration

The [Gmail Sync workflow](../.github/workflows/gmail-sync.yml) performs these steps:

1. Check out the code and assume `job-tracker-ci` through GitHub OIDC.
2. Request a six-hour AWS session and install the Python dependencies.
3. Create the run's CloudWatch log stream and start the Collector container.
4. Wait for the Collector's local health endpoint, then run the selected sync mode.
5. Flush Python telemetry on exit; stop the Collector with a 30-second grace
   period, print its diagnostic logs to the GitHub job log, and remove it.


```mermaid
sequenceDiagram
    participant GH as GitHub Actions
    participant IAM as AWS IAM / OIDC
    participant COL as Collector
    participant APP as Gmail Sync + OTel SDK
    participant AWS as CloudWatch / X-Ray
    GH->>IAM: Assume job-tracker-ci (6-hour session)
    IAM-->>GH: Temporary credentials
    GH->>AWS: Create run log stream
    GH->>COL: Start with temporary credentials
    GH->>COL: Check localhost health endpoint
    GH->>APP: Run selected sync mode
    APP->>COL: run_started and OTLP signals
    loop While sync processes mail
        APP->>COL: Metrics every 30 seconds, batched logs and finished spans
        COL->>AWS: Signed exports, batching and retries
    end
    APP->>COL: Final events and SDK flush
    GH->>COL: Stop with 30-second grace period
    COL->>AWS: Flush remaining batches
    GH->>GH: Keep Collector diagnostics in job log
```

The Collector is version `0.164.0`, pinned to an image digest in the workflow.
It receives OTLP/HTTP, not OTLP/gRPC. Although its listeners use `0.0.0.0`
inside the container, Docker publishes them only on the runner's loopback address.

| Setting | Current value / purpose |
|---|---|
| `LAKE_TELEMETRY_ENABLED` | `1` enables the Python exporters; otherwise instrumentation is inactive |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://127.0.0.1:4318`, the local Collector |
| `AWS_REGION` | `us-west-2` |
| `CLOUDWATCH_LOG_GROUP` | `/mail-parquet-lake/otel` |
| `CLOUDWATCH_LOG_STREAM` | `github/<run-id>/<attempt>`, assigned at startup |
| Service name | `mail-parquet-lake` |
| Service version | GitHub commit SHA, or `local` outside Actions |
| Metric export interval | 30 seconds, plus final flush |
| SDK log/span queues | Up to 512 entries each; export batches up to 64 |
| Collector batching | Five-second timeout; target 64, maximum 128 entries |
| Collector memory limiter | 192 MiB limit, 48 MiB spike allowance |
| Container memory limit | 384 MiB |

The resource includes the service name, version and deployment environment.
The dedicated logger adds the workflow run ID. Metric dimensions remain bounded:
`mode` and, for operation metrics, `operation`; run IDs and commit SHAs are not
configured as CloudWatch metric dimensions.

## Signals and meaning

| Signal | Meaning |
|---|---|
| `run_started`, `run_finished`, `run_failed` | Python process lifecycle; successful completion of a pass is not necessarily completion of the entire historical backfill |
| `backfill_progress` | Cumulative committed scanned messages, attachment-bearing emails, files and completion flag from the persisted checkpoint |
| `lake_messages_scanned` | Backfill references scanned this process |
| `lake_email_rows_upserted` | Email upserts after successful partition writes; reprocessing may count a row again |
| `lake_attachments_stored` | Files in newly committed manifests; cached manifests are not counted again |
| `lake_attachment_bytes_stored` | Bytes in those newly committed manifests |
| `lake_gmail_fetch_retries` | Retries in individual message fetches; not all SDK-internal retries |
| `lake_runs_started/finished/failed` | Process counts by mode |
| `lake_operation_failures` | Failed instrumented operations |
| `lake_operation_duration_seconds` | Instrumented run/page-commit duration |
| `lake_telemetry_checks` | Isolated connection tests, mode `telemetry-check` |

Root run spans finish when the pass ends; page-commit spans can arrive while a
long run is still active. The backfill's content fetch/upload work is within the
run span; page spans measure only its Parquet commit phase. No per-email spans
are created. Metrics use bounded `mode` and, where present, `operation`
dimensions. Run IDs are log properties, not metric dimensions.

Only the dedicated telemetry logger is exported. Email addresses, subjects,
bodies, filenames, attachment bytes, Gmail IDs, S3 keys, credentials and exception
messages/stacks are not captured in telemetry. Original GitHub stdout remains a
separate job log. Do not enable broad automatic HTTP/boto3/logging capture without
reviewing its attributes first.

## Reliability and limitations

Counters and histograms export deltas, so final flushes do not duplicate
cumulative duration observations. The Python SDK and Collector use bounded
memory queues, batching and retry backoff. SDK exporters time out after five
seconds; the HTTP logs exporter retries
for up to one minute. Shutdown flushes SDK queues and gives the Collector 30
seconds to stop. Runner termination can still lose buffered telemetry; this is
not a durable audit pipeline. Telemetry delivery errors are visible in Collector
logs. A healthy process does not guarantee successful downstream ingestion.


```mermaid
flowchart TD
    START["Workflow scheduled or dispatched"] --> RUN{"Runner reaches Python?"}
    RUN -->|No| GH["Inspect GitHub setup failures or disabled workflow state"]
    RUN -->|Yes| BEGIN["Emit run_started"]
    BEGIN --> WORK["Process mail and commit page checkpoints"]
    WORK --> EXIT{"How does the pass end?"}
    EXIT -->|Returns normally| DONE["Emit run_finished"]
    DONE --> COMPLETE{"Backfill complete flag?"}
    COMPLETE -->|true| FINISHED["Historical scan complete"]
    COMPLETE -->|false| PAUSED["Pass paused; resume from saved checkpoint"]
    EXIT -->|Caught exception| FAIL["Emit run_failed and operation_failed"]
    EXIT -->|Runner killed or forcibly interrupted| LOST["Final events or buffered telemetry may be missing"]
    BEGIN -.-> VERIFY["Verify arrival separately in AWS"]
    FAIL --> DIAG["Use error type, Collector diagnostics and GitHub logs"]
```

The completion-flag branch applies to attachment backfill. Full and incremental
sync use their own completion behavior. Telemetry describes the work; the S3
checkpoint determines where an interrupted backfill resumes.

The runner's OTLP and health ports bind to localhost only. The Collector image is
pinned by version and digest. `observability/collector.yaml` configures all three
pipelines. The role has log writes scoped to this one group and
`xray:PutTraceSegments` (which requires `Resource: "*"`). It does not get general
CloudWatch administrator permissions. EMF does not require `PutMetricData`.

The workflow requests six-hour credentials to cover its 330-minute job timeout.
`job-tracker-ci` must have `MaxSessionDuration=21600`; this is also in Terraform.
Sessions already issued before that change retain their original expiry.

## Verify and query

Use **Actions → Gmail Sync → Run workflow → telemetry-check** to test delivery
without reading Gmail or modifying S3. This mode has its own concurrency group
and does not block/cancel a live lake writer. A successful Python exit verifies
emission; verify CloudWatch arrival separately.

In Logs Insights select `/mail-parquet-lake/otel`:

```sql
fields @timestamp, @logStream, @message
| filter @message like /backfill_progress|run_failed|operation_failed/
| sort @timestamp desc
| limit 100
```

To see backfill checkpoints as columns:

```sql
fields @timestamp, attributes.github_run_id, attributes.scanned,
       attributes.emails_with_attachments, attributes.attachments,
       attributes.complete
| filter body = "backfill_progress"
| sort @timestamp desc
| limit 100
```

The fields above reflect the structured OTLP log records verified in this
setup. Use `fields @timestamp, @message | limit 10` to inspect the actual JSON
if field discovery or the ingestion format changes.

For metrics, open CloudWatch Metrics → All metrics → `MailParquetLake`. Compare
runs started/finished/failed, scanned messages, stored bytes and duration by mode.
A disabled GitHub workflow emits no telemetry: detecting missed runs needs an
independent schedule monitor/alarm; application instrumentation alone cannot
identify a workflow that never started.

Use **Sum** for counter metrics to count work within a time window. The stored
backfill checkpoint totals are in `backfill_progress` logs, not a counter to sum
across those events. `run_finished` means the requested pass returned normally;
check `complete` to distinguish a finished historical scan from a pass that
paused at its time budget. An interrupted process may have `run_started` without
`run_finished` or `run_failed`.

Live validation on November 3, 2026 confirmed log ingestion, extracted CloudWatch
metric datapoints and direct retrieval of a test trace from X-Ray. The isolated
[test run](https://github.com/considerable/mail-parquet-lake/actions/runs/41645704677)
was followed by an
[instrumented backfill run](https://github.com/considerable/mail-parquet-lake/actions/runs/41646224790).
These links are validation history, not a claim about their current status.

## Troubleshooting

| Symptom | Check |
|---|---|
| No telemetry at all | Confirm the workflow started, `LAKE_TELEMETRY_ENABLED=1`, Collector health, and its diagnostic logs in GitHub |
| A green connection test, but no AWS data | The test process can emit successfully while downstream delivery fails; inspect Collector errors and verify arrival in AWS separately |
| Log stream or group not found | The group must exist before startup; the workflow creates its run stream and the deployed setup pre-creates `metrics` |
| `AccessDenied` when exporting | Check the inline `mail-lake-opentelemetry` policy on `job-tracker-ci`, region, group ARN and X-Ray write permission |
| `ExpiredToken` in a long run | Check both role maximum session duration and the workflow's requested duration; changing the role does not extend a session already issued |
| Metrics present only as JSON logs | Inspect the records' `_aws.CloudWatchMetrics` declaration, namespace and dimensions; verify extracted datapoints separately |
| Trace search has no result yet | Root spans finish when the run ends; indexing may lag. Check Collector errors and retrieve by trace ID when available |
| A known GitHub failure has no telemetry | Setup failures, disabled workflows and jobs that never reached Python cannot emit this application's failure event; use GitHub run history too |
| Backfill totals differ from metrics | Checkpoints are cumulative mailbox-scan progress; counters cover work in the instrumented process and can include reprocessed upserts |

The application instrumentation does not replay old GitHub failures. The executive
observer adds a dashboard and independent missing-heartbeat/sync alarms. It does
not send notifications.

## Infrastructure

`infra/observability.tf` describes the group, customer-managed encryption key
and permissions. The KMS key is protected against Terraform deletion so retained
logs do not become unreadable. Keep the key enabled while encrypted logs exist.
`observability/iam-policy.json` is the equivalent inline policy template; replace
`ACCOUNT_ID` and `KEY_ID` at deployment. If the resources were provisioned directly, import
them into Terraform state before applying this repository's Terraform. Do not
apply default bucket/prefix variables to an existing lake without checking them.

Local instrumentation is opt-in:

```sh
LAKE_TELEMETRY_ENABLED=1 OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
  python sync/gmail_sync.py --telemetry-check
```

Run a configured Collector first. Without `LAKE_TELEMETRY_ENABLED=1`, sync remains
usable without a running telemetry destination.

## Implementation files

| File | Responsibility |
|---|---|
| [sync/telemetry.py](../sync/telemetry.py) | SDK providers, explicit events/counters/spans, privacy controls, final flush |
| [sync/gmail_sync.py](../sync/gmail_sync.py) | Run lifecycle, progress checkpoints, email upserts and fetch retry instrumentation |
| [sync/attachments.py](../sync/attachments.py) | Counts and bytes for newly committed attachment manifests |
| [observability/collector.yaml](../observability/collector.yaml) | OTLP receiver, batching, memory limits and AWS exporters |
| [.github/workflows/gmail-sync.yml](../.github/workflows/gmail-sync.yml) | OIDC session, Collector container lifecycle and instrumentation settings |
| [infra/observability.tf](../infra/observability.tf) | CloudWatch group, retention and role policy declaration |
| [infra/iam-github-oidc.tf](../infra/iam-github-oidc.tf) | Workflow role and maximum AWS session duration |
| [observability/iam-policy.json](../observability/iam-policy.json) | Equivalent IAM policy template for direct deployment |
| [requirements.txt](../requirements.txt) | Python SDK/exporter dependencies |
| [tests/test_telemetry.py](../tests/test_telemetry.py) | Disabled-by-default behavior, error privacy and Collector/IAM configuration checks |

For the separate attachment storage design and backfill recovery, see
[Attachments in the email lake](attachments.md).

## Repository checks and network recovery

Workflow action revisions are pinned to full commit hashes. Dependabot continues
to propose updated pins. Python dependency updates have a seven-day cooldown;
GitHub Actions does not support cooldown, so only that ecosystem's unsupported
cooldown scanner finding has a documented exception. The security checks remain
enabled. Cloud-backed AI reviews run only for trusted same-repository human pull
requests; Dependabot and fork PRs still run secret detection, Semgrep, dependency
audit, lint and tests without receiving AWS secrets.

Transient Gmail connection failures, including a broken pipe during automatic
access-token refresh, retry up to five times with exponential backoff and jitter.
Gmail HTTP rate-limit/server-error retries are also handled by the Google client.
Permanent OAuth denial is not retried. A failed page leaves its backfill checkpoint
unchanged, allowing the next invocation to resume. These retry attempts are not
all included in `lake_gmail_fetch_retries`, which counts the outer individual
message-fetch retry loop only.
