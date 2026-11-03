# OpenTelemetry → AWS CloudWatch

Gmail Sync emits explicit OpenTelemetry signals over OTLP/HTTP to a Collector
on its GitHub runner. The Collector uses the workflow's temporary OIDC role
credentials. There are no new access keys or bearer tokens.

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

Log retention is 30 days for this new group. This does not set metric or trace
retention. Ingestion, custom metrics and trace storage can incur AWS charges.

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

The Python SDK and Collector use bounded memory queues, batching and retry
backoff. SDK exporters time out after five seconds; the HTTP logs exporter retries
for up to one minute. Shutdown flushes SDK queues and gives the Collector 30
seconds to stop. Runner termination can still lose buffered telemetry; this is
not a durable audit pipeline. Telemetry delivery errors are visible in Collector
logs. A healthy process does not guarantee successful downstream ingestion.

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

For metrics, open CloudWatch Metrics → All metrics → `MailParquetLake`. Compare
runs started/finished/failed, scanned messages, stored bytes and duration by mode.
A disabled GitHub workflow emits no telemetry: detecting missed runs needs an
independent schedule monitor/alarm; application instrumentation alone cannot
identify a workflow that never started.

## Infrastructure

`infra/observability.tf` describes the group and permissions.
`observability/iam-policy.json` is the equivalent inline policy template; replace
`ACCOUNT_ID` at deployment. If the resources were provisioned directly, import
them into Terraform state before applying this repository's Terraform. Do not
apply default bucket/prefix variables to an existing lake without checking them.

Local instrumentation is opt-in:

```sh
LAKE_TELEMETRY_ENABLED=1 OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
  python sync/gmail_sync.py --telemetry-check
```

Run a configured Collector first. Without `LAKE_TELEMETRY_ENABLED=1`, sync remains
usable without a running telemetry destination.
