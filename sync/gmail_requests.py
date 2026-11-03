"""Bounded retries for transient Gmail transport and credential-refresh failures."""

import random
import time

import httplib2
from google.auth.exceptions import TransportError


def retry_transport(operation, attempts=5):
    """Retry connection failures, never permanent OAuth denial or arbitrary errors.

    Covers broken pipes during automatic access-token refresh as well as Gmail
    reads. These operations are read-only or token refreshes and safe to retry.
    """
    for attempt in range(attempts):
        try:
            return operation()
        except (OSError, TransportError, httplib2.ServerNotFoundError):
            if attempt == attempts - 1:
                raise
            time.sleep(min(2**attempt, 16) + random.uniform(0, 0.5))


def execute_request(request):
    # google-api-python-client handles HTTP 429/5xx; this outer layer handles
    # connection loss during Gmail calls and authentication refresh.
    return retry_transport(lambda: request.execute(num_retries=5))


def execute_batch(batch):
    return retry_transport(batch.execute)
