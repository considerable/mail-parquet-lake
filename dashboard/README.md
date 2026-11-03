# Mail Lake dashboard

Published at https://considerable.github.io/mail-parquet-lake/.

The site combines public-safe executive evidence and GitHub workflow history.
`data.json` and `executive.json` are generated, never committed. The executive
artifact is downloaded only from the trusted main-branch observer and re-projected
before publication. No email content, raw reports, credentials or AWS identifiers
belong in this directory.

Executive data older than 45 minutes is visibly stale, with a separate inventory
measurement timestamp. Charts use dated inventory snapshots; missing days and
unknown values are not converted to zero. All text from data is inserted through
`textContent`. No third-party chart service, tracking or external font download
is required.

The observer publishes only four CloudWatch alarm signals. Business cards and
charts are rendered locally in the browser. See [observability](../docs/observability.md)
for the current architecture, privacy allowlist, remaining costs and limitations.
