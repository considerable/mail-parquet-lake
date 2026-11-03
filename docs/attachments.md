# Attachments in the email lake

Both full and incremental sync preserve attachment bytes in the existing private
S3 lake. This includes PDFs, photos, named files, nested MIME attachments, inline
images and `text/calendar` parts (including unnamed meeting invitations).
Calendar bytes are preserved as received; this does not create calendar events.
Remote images or cloud-document links in email HTML are not attached files and
are not downloaded. A message permanently deleted from Gmail before capture
cannot be recovered by this importer.

## Storage and parent email

- Email rows remain in `<prefix>/year=YYYY/month=MM/emails.parquet`.
- `attachment_count` is the number of captured MIME attachments.
- `attachments_json` contains an array with original filename, MIME type,
  content disposition, Content-ID, size, SHA-256, S3 bucket/key, Gmail message ID
  and thread ID. Inline images can be matched through their Content-ID.
- Original bytes live at
  `<prefix>/attachments/<gmail-id>/<MIME-path>/<SHA-256>`.
- A per-message `manifest.json` in that message's attachment directory repeats
  the links and metadata. Filenames are metadata, never executable paths.
- Old email partitions may lack the new columns. DuckDB readers must use
  `read_parquet(..., union_by_name=true)`; null means not yet evaluated, while
  zero means the new importer examined the message and found no attachments.

Files are private S3 objects; do not copy email contents, manifests or object keys
to the public GitHub Pages dashboard. S3 receives a SHA-256 checksum with each
upload. The manifest is committed only after every file succeeds. Retries use
deterministic keys and reuse completed manifests.

## Historical backfill

In **Actions → Gmail Sync → Run workflow**, select `attachment-backfill`.
Locally, with the usual Gmail/S3 environment:

```sh
python3 sync/gmail_sync.py --attachment-backfill
```

It scans all messages, including Spam and Trash, because `has:attachment` alone
can miss unnamed calendar parts and inline media. It updates only email rows
with attachments. Messages are fetched in batches of up to 50; downloads retry
transient Gmail failures. A message deleted during the scan is skipped on 404;
other errors fail the run rather than silently losing attachments.

The separate `<prefix>/attachment_backfill_state.json` saves scanned messages,
attachment-bearing email count, file count, completion and the next page token.
A page is checkpointed only after files, manifests and email rows are written.
The incremental `sync_state.json` is never advanced by backfill. If interrupted,
run the same mode again. A pass stops at a page boundary after four hours by
default; re-run until `complete=True`. `--max-seconds` sets the local pass budget.
An already complete backfill exits without scanning again; subsequent messages
are handled by regular incremental sync. If a saved Gmail page token expires,
the backfill fails visibly; restart from the beginning by explicitly resetting
only the backfill state after reviewing its progress. Deterministic writes make
reprocessing safe.

All Gmail Sync modes share one GitHub Actions concurrency group to prevent
simultaneous partition rewrites. External/manual writers need the same exclusion.
S3 access or corrupt-Parquet errors abort rather than replace an unreadable
partition. No additional Gmail write permission is requested.

## Retrieve a file

Read the parent row's `attachments_json` (or its manifest), then use the recorded
`s3_bucket` and `s3_key` with your authenticated S3 client. For example:

```sh
aws s3 cp 's3://YOUR_BUCKET/RECORDED_S3_KEY' './invite.ics'
```

Use a trusted local output filename rather than blindly using an incoming
attachment filename. Verify the downloaded bytes against `sha256` if needed.

## Retention

The existing incremental importer removes email rows after permanent Gmail
message deletion. This change does **not** add automatic deletion of stored
attachment objects or manifests: they remain until an explicit retention or S3
lifecycle policy removes them. Moving a Gmail message to Trash is not itself
permanent deletion. Existing lake retention and bucket encryption policies apply.
