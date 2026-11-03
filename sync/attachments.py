"""Preserve Gmail MIME attachments in private S3, linked by immutable message ID."""

import base64
import hashlib
import json
from datetime import UTC, datetime

from botocore.exceptions import ClientError

try:
    from .telemetry import telemetry
except ImportError:
    from telemetry import telemetry

SCHEMA_VERSION = 1


def decode_data(data):
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


def walk_parts(part, path="0"):
    yield path, part
    for index, child in enumerate(part.get("parts", [])):
        yield from walk_parts(child, f"{path}.{index}")


def attachment_parts(payload):
    for path, part in walk_parts(payload):
        headers = {h["name"].lower(): h["value"] for h in part.get("headers", [])}
        disposition = headers.get("content-disposition", "")
        mime = part.get("mimeType", "application/octet-stream").lower()
        body = part.get("body", {})
        has_content = "data" in body or "attachmentId" in body
        if has_content and (
            part.get("filename")
            or disposition.lower().startswith("attachment")
            or mime == "text/calendar"
            or (
                not mime.startswith("multipart/")
                and mime not in {"text/plain", "text/html"}
            )
        ):
            yield path, part, headers


def read_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return None
        raise


def store_attachments(service, s3, bucket, prefix, message):
    """Commit manifest only after every attachment succeeds; retries reuse it."""
    parts = list(attachment_parts(message.get("payload", {})))
    if not parts:
        return []
    message_id = message["id"]
    manifest_key = f"{prefix}/attachments/{message_id}/manifest.json"
    prior = read_json(s3, bucket, manifest_key)
    if prior and prior.get("schema_version") == SCHEMA_VERSION:
        return prior["attachments"]
    attachments = []
    for path, part, headers in parts:
        body = part["body"]
        if "attachmentId" in body:
            encoded = (
                service.users()
                .messages()
                .attachments()
                .get(userId="me", messageId=message_id, id=body["attachmentId"])
                .execute(num_retries=5)["data"]
            )
        else:
            encoded = body["data"]
        data = decode_data(encoded)
        if "size" in body and len(data) != body["size"]:
            raise ValueError("Attachment size mismatch; checkpoint has not advanced")
        digest = hashlib.sha256(data).hexdigest()
        key = f"{prefix}/attachments/{message_id}/{path}/{digest}"
        mime = part.get("mimeType", "application/octet-stream")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=mime,
            ChecksumSHA256=base64.b64encode(hashlib.sha256(data).digest()).decode(
                "ascii"
            ),
        )
        attachments.append(
            {
                "gmail_id": message_id,
                "thread_id": message.get("threadId", ""),
                "part_id": part.get("partId", path),
                "part_path": path,
                "filename": part.get("filename", ""),
                "mime_type": mime,
                "disposition": headers.get("content-disposition", ""),
                "content_id": headers.get("content-id", ""),
                "size_bytes": len(data),
                "sha256": digest,
                "s3_bucket": bucket,
                "s3_key": key,
            }
        )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "gmail_id": message_id,
        "thread_id": message.get("threadId", ""),
        "stored_at": datetime.now(UTC).isoformat(),
        "attachments": attachments,
    }
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest).encode(),
        ContentType="application/json",
    )
    telemetry.count("lake_attachments_stored", len(attachments))
    telemetry.count(
        "lake_attachment_bytes_stored", sum(a["size_bytes"] for a in attachments)
    )
    return attachments
