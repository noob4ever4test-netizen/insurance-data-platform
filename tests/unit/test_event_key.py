from datetime import UTC, datetime, timedelta, timezone

import pytest

from azki_pipeline.contracts import event_key


@pytest.mark.unit
@pytest.mark.parametrize(
    "event_time",
    [
        "2025-10-16 17:17:33",
        datetime(2025, 10, 16, 17, 17, 33),
        datetime(2025, 10, 16, 17, 17, 33, tzinfo=UTC),
        datetime(2025, 10, 16, 20, 47, 33, tzinfo=timezone(timedelta(hours=3.5))),
    ],
)
def test_U20_event_key_matches_clickhouse_typed_vector(event_time):
    actual = event_key(3, "s_1", event_time, "policy_click")

    assert actual == 14560372016242613758
    assert event_key(3, "s_1", event_time, "policy_click") == actual


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [
        {"user_id": 4},
        {"session_id": "s_2"},
        {"event_time": "2025-10-16 17:17:34"},
        {"event_type": "purchase"},
    ],
)
def test_U20_each_key_component_affects_hash(overrides):
    arguments = {
        "user_id": 3,
        "session_id": "s_1",
        "event_time": "2025-10-16 17:17:33",
        "event_type": "policy_click",
    }

    actual = event_key(**(arguments | overrides))

    assert actual != event_key(**arguments)


@pytest.mark.unit
@pytest.mark.parametrize("user_id", [-(2**63) - 1, 2**63])
def test_U20_user_id_must_fit_int64(user_id):
    with pytest.raises(ValueError, match="Int64"):
        event_key(user_id, "s_1", "2025-10-16 17:17:33", "purchase")


@pytest.mark.unit
@pytest.mark.parametrize(
    "event_time",
    ["2025-02-30 00:00:00", "bad", "2025-1-1 00:00:00", "1969-12-31 23:59:59"],
)
def test_U20_rejects_invalid_clickhouse_datetime(event_time):
    with pytest.raises(ValueError):
        event_key(3, "s_1", event_time, "purchase")
