import importlib
import json
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest

from azki_pipeline.connect_client import ConnectClient, ConnectError

MODULE = importlib.import_module("azki_pipeline.connect_client")


def connector():
    return {
        "name": "synthetic-source",
        "config": {"connector.class": "example.Source", "tasks.max": "1"},
    }


def running_status():
    return {"connector": {"state": "RUNNING"}, "tasks": [{"id": 0, "state": "RUNNING"}]}


@pytest.mark.unit
def test_U29_register_validates_before_putting_config():
    request = Mock(side_effect=[{"error_count": 0}, {"name": "synthetic-source"}])
    client = ConnectClient("http://localhost:8083", request=request)
    document = connector()

    result = client.register(document)

    assert result == {"name": document["name"]}
    assert request.call_args_list[0].args == (
        "PUT",
        "/connector-plugins/example.Source/config/validate",
        document["config"] | {"name": document["name"]},
    )
    assert request.call_args_list[1].args == (
        "PUT",
        "/connectors/synthetic-source/config",
        document["config"] | {"name": document["name"]},
    )


@pytest.mark.unit
def test_U29_validation_errors_prevent_registration():
    request = Mock(
        return_value={
            "error_count": 1,
            "configs": [{"value": {"name": "input.path", "errors": ["not writable"]}}],
        }
    )
    client = ConnectClient("http://localhost:8083", request=request)

    with pytest.raises(ConnectError, match="input.path.*not writable"):
        client.register(connector())

    assert request.call_count == 1


@pytest.mark.unit
def test_U29_waits_for_connector_and_nonempty_running_tasks(monkeypatch):
    request = Mock(
        side_effect=[
            {"connector": {"state": "RUNNING"}, "tasks": []},
            {"connector": {"state": "RUNNING"}, "tasks": [{"state": "UNASSIGNED"}]},
            running_status(),
        ]
    )
    waits = []
    monkeypatch.setattr(MODULE, "monotonic", lambda: 0)
    monkeypatch.setattr(MODULE, "_wait", waits.append)

    status = ConnectClient("http://localhost:8083", request=request).wait_running(
        "synthetic-source", timeout=1, interval=0.1
    )

    assert status == running_status()
    assert waits == [0.1, 0.1]


@pytest.mark.unit
def test_U29_failed_task_surfaces_trace():
    request = Mock(
        return_value={
            "connector": {"state": "RUNNING"},
            "tasks": [{"state": "FAILED", "trace": "synthetic failure"}],
        }
    )

    with pytest.raises(ConnectError, match="synthetic failure"):
        ConnectClient("http://localhost:8083", request=request).wait_running("source")


@pytest.mark.unit
def test_U29_timeout_is_bounded(monkeypatch):
    clock = iter([0.0, 1.0])
    monkeypatch.setattr(MODULE, "monotonic", lambda: next(clock))
    request = Mock(return_value={"connector": {"state": "UNASSIGNED"}, "tasks": []})

    with pytest.raises(TimeoutError, match="source"):
        ConnectClient("http://localhost:8083", request=request).wait_running("source", timeout=1)


@pytest.mark.unit
def test_U29_http_request_encodes_json_and_connector_names(monkeypatch):
    response = Mock()
    response.read.return_value = json.dumps(running_status()).encode()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    urlopen = Mock(return_value=response)
    monkeypatch.setattr(MODULE, "urlopen", urlopen)
    client = ConnectClient("http://localhost:8083/", timeout=2)

    assert client.status("source name") == running_status()

    request = urlopen.call_args.args[0]
    assert request.full_url == "http://localhost:8083/connectors/source%20name/status"
    assert request.method == "GET"
    assert urlopen.call_args.kwargs["timeout"] == 2


@pytest.mark.unit
def test_U29_validation_sends_config_as_json(monkeypatch):
    response = Mock()
    response.read.return_value = b'{"error_count": 0}'
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    urlopen = Mock(return_value=response)
    monkeypatch.setattr(MODULE, "urlopen", urlopen)

    ConnectClient("http://localhost:8083").validate(connector())

    request = urlopen.call_args.args[0]
    assert request.method == "PUT"
    assert json.loads(request.data) == connector()["config"] | {"name": connector()["name"]}
    assert request.get_header("Content-type") == "application/json"


@pytest.mark.unit
def test_U29_transient_status_http_error_is_retried(monkeypatch):
    request = Mock(
        side_effect=[
            HTTPError("http://localhost", 503, "temporarily unavailable", {}, None),
            running_status(),
        ]
    )
    monkeypatch.setattr(MODULE, "_wait", lambda interval: None)

    result = ConnectClient("http://localhost:8083", request=request).wait_running("source")

    assert result == running_status()


@pytest.mark.unit
def test_U29_status_authentication_error_is_not_retried():
    request = Mock(side_effect=HTTPError("http://localhost", 401, "unauthorized", {}, None))

    with pytest.raises(HTTPError) as failure:
        ConnectClient("http://localhost:8083", request=request).wait_running("source")

    assert failure.value.code == 401
    assert request.call_count == 1
