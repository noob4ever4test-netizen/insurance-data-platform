import importlib

import pytest

from tests.helpers.eventually import eventually
from tests.helpers.factories import make_event, make_user

polling = importlib.import_module("tests.helpers.eventually")


@pytest.mark.unit
def test_U21_event_factory_supports_overrides():
    event = make_event(user_id=9, premium_amount=0)

    assert event["user_id"] == 9
    assert event["premium_amount"] == 0
    assert set(event) == {
        "event_time",
        "user_id",
        "session_id",
        "event_type",
        "channel",
        "premium_amount",
    }


@pytest.mark.unit
def test_U22_user_factory_supports_overrides():
    user = make_user(user_id=9, city="Shiraz")

    assert user["user_id"] == 9
    assert user["city"] == "Shiraz"
    assert set(user) == {"user_id", "signup_date", "city", "device_type"}


@pytest.mark.unit
@pytest.mark.parametrize("factory", [make_event, make_user])
def test_U23_factory_results_are_independent(factory):
    first = factory()
    expected = factory()

    first["user_id"] = 999

    assert factory() == expected
    assert factory() is not expected


@pytest.mark.unit
def test_U24_eventually_returns_immediately_without_waiting(monkeypatch):
    def unexpected_wait(duration):
        pytest.fail("An immediately successful check must not wait")

    monkeypatch.setattr(polling, "_wait", unexpected_wait)

    assert eventually(lambda: "ready", timeout=0) == "ready"
    assert eventually(lambda: None, timeout=0) is None


@pytest.mark.unit
@pytest.mark.parametrize("use_assertion", [True, False])
def test_U25_eventually_retries_until_ready(monkeypatch, use_assertion):
    attempts = []
    waits = []
    monkeypatch.setattr(polling, "monotonic", lambda: 0.0)
    monkeypatch.setattr(polling, "_wait", waits.append)

    def check():
        attempts.append(1)
        if use_assertion:
            assert len(attempts) == 3, "not ready"
            return "ready"
        return len(attempts) == 3

    result = eventually(check, timeout=1, interval=0.1)

    assert result == ("ready" if use_assertion else True)
    assert len(attempts) == 3
    assert waits == [0.1, 0.1]


@pytest.mark.unit
def test_U26_eventually_timeout_retains_last_assertion(monkeypatch):
    clock = iter([0.0, 0.75, 1.0])
    waits = []
    monkeypatch.setattr(polling, "monotonic", lambda: next(clock))
    monkeypatch.setattr(polling, "_wait", waits.append)

    def check():
        raise AssertionError("still missing")

    with pytest.raises(TimeoutError, match="still missing") as failure:
        eventually(check, timeout=1, interval=0.5)

    assert isinstance(failure.value.__cause__, AssertionError)
    assert waits == [0.25]


@pytest.mark.unit
def test_U27_eventually_propagates_unexpected_errors():
    def check():
        raise RuntimeError("broken query")

    with pytest.raises(RuntimeError, match="broken query"):
        eventually(check)


@pytest.mark.unit
@pytest.mark.parametrize(("timeout", "interval"), [(-1, 0.1), (1, 0), (1, -0.1), (float("inf"), 1)])
def test_U28_eventually_rejects_invalid_timing(timeout, interval):
    with pytest.raises(ValueError):
        eventually(lambda: True, timeout=timeout, interval=interval)
