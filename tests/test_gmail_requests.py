from unittest.mock import MagicMock

import pytest
from google.auth.exceptions import RefreshError, TransportError

from sync.gmail_requests import execute_batch, execute_request, retry_transport


@pytest.fixture(autouse=True)
def no_retry_sleep(monkeypatch):
    monkeypatch.setattr("sync.gmail_requests.time.sleep", lambda _: None)


def test_request_retries_broken_pipe_during_token_refresh():
    request = MagicMock()
    request.execute.side_effect = [
        BrokenPipeError("connection closed"),
        {"messages": []},
    ]
    assert execute_request(request) == {"messages": []}
    assert request.execute.call_count == 2


def test_batch_retries_transient_auth_transport_error():
    batch = MagicMock()
    batch.execute.side_effect = [TransportError("connection failed"), None]
    assert execute_batch(batch) is None
    assert batch.execute.call_count == 2


def test_repeated_transport_failure_is_bounded():
    operation = MagicMock(side_effect=BrokenPipeError("connection closed"))
    with pytest.raises(BrokenPipeError):
        retry_transport(operation)
    assert operation.call_count == 5


def test_permanent_oauth_denial_is_not_retried():
    operation = MagicMock(side_effect=RefreshError("invalid_grant"))
    with pytest.raises(RefreshError):
        retry_transport(operation)
    operation.assert_called_once()
