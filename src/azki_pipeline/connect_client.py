import json
from collections.abc import Callable
from math import isfinite
from threading import Event
from time import monotonic
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

_wait = Event().wait


class ConnectError(RuntimeError):
    pass


class ConnectClient:
    def __init__(self, base_url: str, *, timeout: float = 10, request: Callable | None = None):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._request = request or self._http

    def _http(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        with urlopen(request, timeout=self.timeout) as response:
            payload = response.read()
        return json.loads(payload) if payload else None

    def validate(self, document: dict) -> dict:
        config = document["config"] | {"name": document["name"]}
        connector_class = quote(config["connector.class"], safe="")
        result = self._request(
            "PUT", f"/connector-plugins/{connector_class}/config/validate", config
        )
        if result.get("error_count") != 0:
            errors = []
            for entry in result.get("configs", []):
                value = entry.get("value", {})
                for error in value.get("errors", []):
                    errors.append(f"{value.get('name')}: {error}")
            raise ConnectError("Connector validation failed: " + "; ".join(errors))
        return result

    def register(self, document: dict) -> dict:
        self.validate(document)
        name = quote(document["name"], safe="")
        return self._request(
            "PUT", f"/connectors/{name}/config", document["config"] | {"name": document["name"]}
        )

    def status(self, name: str) -> dict:
        return self._request("GET", f"/connectors/{quote(name, safe='')}/status")

    def delete(self, name: str) -> None:
        self._request("DELETE", f"/connectors/{quote(name, safe='')}")

    def wait_running(self, name: str, *, timeout: float = 60, interval: float = 0.5) -> dict:
        if not isfinite(timeout) or timeout < 0 or not isfinite(interval) or interval <= 0:
            raise ValueError("Invalid polling timeout or interval")
        deadline = monotonic() + timeout
        last_status = None
        while True:
            try:
                last_status = self.status(name)
                connector = last_status.get("connector", {})
                tasks = last_status.get("tasks", [])
                failed = [state for state in [connector, *tasks] if state.get("state") == "FAILED"]
                if failed:
                    raise ConnectError(f"Connector {name} failed: {failed}")
                if (
                    connector.get("state") == "RUNNING"
                    and tasks
                    and all(task.get("state") == "RUNNING" for task in tasks)
                ):
                    return last_status
            except HTTPError as error:
                if error.code not in {404, 409, 500, 502, 503, 504}:
                    raise
                last_status = str(error)
            except URLError as error:
                last_status = str(error)
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Connector {name} did not reach RUNNING: {last_status}")
            _wait(min(interval, remaining))
