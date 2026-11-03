import base64
import hashlib
import io
import json
from unittest.mock import MagicMock

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from pyarrow.lib import ArrowInvalid

from sync import gmail_sync as sync
from sync.attachments import attachment_parts, store_attachments


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.puts = []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body.encode() if isinstance(Body, str) else Body
        self.puts.append(Key)


def encoded(data):
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def message():
    return {
        "id": "mail1",
        "threadId": "thread1",
        "internalDate": "1735689600000",
        "historyId": "10",
        "payload": {
            "mimeType": "multipart/mixed",
            "parts": [
                {"mimeType": "text/plain", "body": {"data": encoded(b"Hello")}},
                {
                    "mimeType": "application/pdf",
                    "filename": "../resume.pdf",
                    "body": {"attachmentId": "external", "size": 4},
                },
                {
                    "mimeType": "multipart/related",
                    "parts": [
                        {
                            "mimeType": "image/png",
                            "headers": [{"name": "Content-ID", "value": "<photo>"}],
                            "body": {"data": encoded(b"photo"), "size": 5},
                        },
                        {
                            "mimeType": "text/calendar",
                            "body": {"data": encoded(b"invite"), "size": 6},
                        },
                    ],
                },
            ],
        },
    }


def gmail():
    service = MagicMock()
    service.users().messages().attachments().get().execute.return_value = {
        "data": encoded(b"%PDF")
    }
    return service


def test_recursive_inline_calendar_and_external_pdf_bytes_and_parent_links():
    s3 = MemoryS3()
    service = gmail()
    attachments = store_attachments(service, s3, "private", "gmail", message())
    assert len(attachments) == 3
    assert [s3.objects[a["s3_key"]] for a in attachments] == [
        b"%PDF",
        b"photo",
        b"invite",
    ]
    for a in attachments:
        assert a["gmail_id"] == "mail1" and a["thread_id"] == "thread1"
        assert a["sha256"] == hashlib.sha256(s3.objects[a["s3_key"]]).hexdigest()
        assert ".." not in a["s3_key"]
    assert attachments[0]["filename"] == "../resume.pdf"
    assert attachments[1]["content_id"] == "<photo>"
    assert (
        json.loads(s3.objects["gmail/attachments/mail1/manifest.json"])["attachments"]
        == attachments
    )
    before = len(s3.puts)
    assert store_attachments(service, s3, "private", "gmail", message()) == attachments
    assert len(s3.puts) == before


def test_named_text_and_unnamed_calendar_but_not_html_body():
    payload = {
        "mimeType": "multipart/alternative",
        "parts": [
            {"mimeType": "text/html", "body": {"data": ""}},
            {"mimeType": "text/plain", "filename": "notes.txt", "body": {"data": ""}},
            {"mimeType": "text/calendar", "body": {"data": ""}},
        ],
    }
    assert len(list(attachment_parts(payload))) == 2


def test_download_failure_never_commits_manifest_or_history():
    s3 = MemoryS3()
    service = gmail()
    service.users().messages().attachments().get().execute.side_effect = RuntimeError(
        "unavailable"
    )
    with pytest.raises(RuntimeError):
        sync.prepare_message(service, s3, message())
    assert s3.objects == {}


def test_size_mismatch_never_commits_manifest():
    s3 = MemoryS3()
    msg = message()
    msg["payload"]["parts"][1]["body"]["size"] = 999
    with pytest.raises(ValueError, match="size mismatch"):
        store_attachments(gmail(), s3, "private", "gmail", msg)
    assert not s3.objects


def test_partition_merge_preserves_existing_and_roundtrips_attachment_metadata(
    monkeypatch,
):
    monkeypatch.setattr(sync, "S3_BUCKET", "private")
    s3 = MemoryS3()
    old = sync.parse_message(message())
    old["gmail_id"] = "older-email"
    sync.write_parquet([old], s3)
    new = sync.prepare_message(gmail(), s3, message())
    sync.write_parquet([new], s3)
    key = "gmail/year=2025/month=01/emails.parquet"
    result = pd.read_parquet(io.BytesIO(s3.objects[key]))
    assert set(result.gmail_id) == {"older-email", "mail1"}
    row = result[result.gmail_id == "mail1"].iloc[0]
    assert row.attachment_count == 3
    assert len(json.loads(row.attachments_json)) == 3
    sync.write_parquet([new], s3)
    assert len(pd.read_parquet(io.BytesIO(s3.objects[key]))) == 2


@pytest.mark.parametrize("error", ["AccessDenied", "SlowDown"])
def test_s3_read_error_never_overwrites_partition(error):
    s3 = MemoryS3()
    s3.get_object = MagicMock(
        side_effect=ClientError({"Error": {"Code": error}}, "GetObject")
    )
    with pytest.raises(ClientError):
        sync.write_parquet([sync.parse_message(message())], s3)
    assert s3.puts == []


def test_corrupt_parquet_is_not_overwritten():
    s3 = MemoryS3()
    s3.objects["gmail/year=2025/month=01/emails.parquet"] = b"corrupt"
    with pytest.raises(ArrowInvalid):
        sync.write_parquet([sync.parse_message(message())], s3)
    assert s3.puts == []


def test_backfill_resumes_and_leaves_incremental_checkpoint_unchanged(monkeypatch):
    s3 = MemoryS3()
    s3.objects[sync.STATE_KEY] = b'{"last_history_id": "previous"}'
    service = gmail()
    service.users().messages().list().execute.side_effect = [
        {"messages": [{"id": "mail1"}], "nextPageToken": "next"},
        {"messages": []},
    ]
    monkeypatch.setattr(sync, "_fetch_message", lambda *args: message())
    monkeypatch.setattr(
        sync, "_fetch_messages", lambda service, refs: [message() for _ in refs]
    )
    first = sync.backfill_attachments(service, s3, max_seconds=0)
    assert first["scanned"] == 1 and not first["complete"]
    second = sync.backfill_attachments(service, s3)
    assert second["complete"] and second["attachments"] == 3
    assert service.users().messages().list.call_args.kwargs["pageToken"] == "next"
    assert service.users().messages().list.call_args.kwargs["includeSpamTrash"] is True
    assert s3.objects[sync.STATE_KEY] == b'{"last_history_id": "previous"}'


def test_failed_parquet_write_does_not_advance_backfill(monkeypatch):
    s3 = MemoryS3()
    service = gmail()
    service.users().messages().list().execute.return_value = {
        "messages": [{"id": "mail1"}]
    }
    monkeypatch.setattr(sync, "_fetch_message", lambda *args: message())
    monkeypatch.setattr(
        sync, "_fetch_messages", lambda service, refs: [message() for _ in refs]
    )
    monkeypatch.setattr(
        sync, "write_parquet", MagicMock(side_effect=RuntimeError("upload failed"))
    )
    with pytest.raises(RuntimeError):
        sync.backfill_attachments(service, s3)
    assert sync.BACKFILL_KEY not in s3.objects
    assert sync.STATE_KEY not in s3.objects


def test_incremental_attachment_failure_leaves_history_checkpoint(monkeypatch):
    s3 = MemoryS3()
    s3.objects[sync.STATE_KEY] = b'{"last_history_id": "old"}'
    service = gmail()
    service.users().history().list().execute.return_value = {
        "historyId": "new",
        "history": [{"messagesAdded": [{"message": {"id": "mail1"}}]}],
    }
    monkeypatch.setattr(sync, "_fetch_message", lambda *args: message())
    monkeypatch.setattr(
        sync, "_fetch_messages", lambda service, refs: [message() for _ in refs]
    )
    service.users().messages().attachments().get().execute.side_effect = RuntimeError(
        "download failed"
    )
    with pytest.raises(RuntimeError):
        sync.sync_incremental(service, s3)
    assert s3.objects[sync.STATE_KEY] == b'{"last_history_id": "old"}'


def test_batch_retries_failed_member_and_preserves_success(monkeypatch):
    service = MagicMock()
    request = MagicMock()
    service.new_batch_http_request.return_value = request

    def execute():
        callback = service.new_batch_http_request.call_args.kwargs["callback"]
        callback("ok", {"id": "ok"}, None)
        callback("retry", None, RuntimeError("temporary"))

    request.execute.side_effect = execute
    retry = MagicMock(return_value={"id": "retry"})
    monkeypatch.setattr(sync, "_fetch_message", retry)
    monkeypatch.setattr(sync.time, "sleep", lambda _: None)
    assert sync._fetch_messages(service, [{"id": "ok"}, {"id": "retry"}]) == [
        {"id": "ok"},
        {"id": "retry"},
    ]
    retry.assert_called_once_with(service, "retry")


def test_no_attachment_body_does_not_write_objects():
    s3 = MemoryS3()
    msg = message()
    msg["payload"]["parts"] = msg["payload"]["parts"][:1]
    record = sync.prepare_message(gmail(), s3, msg)
    assert record["attachment_count"] == 0
    assert record["attachments_json"] == "[]"
    assert s3.puts == []


def test_reader_schema_can_mix_old_and_new_partitions(tmp_path):
    import duckdb

    pd.DataFrame({"gmail_id": ["old"]}).to_parquet(tmp_path / "old.parquet")
    pd.DataFrame(
        {"gmail_id": ["new"], "attachment_count": [3], "attachments_json": ["[]"]}
    ).to_parquet(tmp_path / "new.parquet")
    rows = duckdb.sql(
        "SELECT gmail_id, attachment_count FROM read_parquet(?, union_by_name=true) ORDER BY gmail_id",
        params=[str(tmp_path / "*.parquet")],
    ).fetchall()
    assert rows == [("new", 3), ("old", None)]
