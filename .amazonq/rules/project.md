# mail-parquet-lake — Amazon Q Developer Rules

## Project Overview
Sync Gmail to Parquet on S3. Query with DuckDB. Export to Markdown for AI agents.

## Stack
- Python 3.13
- Gmail API (google-auth, google-api-python-client)
- DuckDB — query engine, reads S3 Parquet natively
- Pandas — result shaping and export
- Parquet on S3 — columnar storage, partitioned by year/month
- Makefile — all operations

## Key Files
```
sync/gmail_sync.py          # Gmail → Parquet sync (full + incremental via history API)
agent/job_tracker.py        # Job search pipeline tracker
agent/job_tracker.yaml      # Job tracker config (keywords, stages, ignore domains)
agent/tools.py              # DuckDB query tools for AI agents
export/to_markdown.py       # DuckDB → .md files for AI agents
Makefile                    # all targets
.env                        # S3_BUCKET, S3_PREFIX, Gmail OAuth credentials (never commit)
```

## Env Vars
```
S3_BUCKET=mail-parquet-lake
S3_PREFIX=your-prefix                 # S3 key prefix for this account
GMAIL_ACCOUNT=you@gmail.com       # OAuth identity
```

## S3 Structure
```
s3://{S3_BUCKET}/{S3_PREFIX}/
  year=2026/month=04/emails.parquet
  year=2026/month=03/emails.parquet
  sync_state.json                   ← stores last_history_id for delta sync
  reports/job-tracker-YYYY-MM-DD.md ← job tracker reports
```

## DuckDB Query Pattern
```python
import duckdb
con = duckdb.connect()
con.execute('CALL load_aws_credentials()')  # required for S3 access
con.sql(f"SELECT * FROM read_parquet('{S3_BASE}/**/*.parquet')")
```

## Delete Sync Pattern
Gmail deletes are synced via history.list API — NOT by re-scanning all emails.
Always use history API delta, never full re-scan after initial import.

## Critical Rules
- NEVER commit .env or credentials
- NEVER commit AWS account IDs, Gmail addresses, or S3 prefixes — use variables/placeholders
- NEVER do full re-scan after initial sync — always use history API delta
- NEVER store raw email in git — only Parquet on S3
- Partition by year/month — rewrite only affected partition on delete sync
- Export .md files go to export/output/ — gitignored
- Skip emails with internalDate=0 (epoch zero)
- DuckDB must call load_aws_credentials() before S3 reads
