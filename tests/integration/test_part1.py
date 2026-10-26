import pytest

from azki_pipeline.contracts import event_key
from azki_pipeline.sql_runner import apply_sql
from tests.helpers.factories import make_event, make_user
from tests.helpers.pipeline import (
    aggregate_rows,
    expected_aggregates,
    fixture_rows,
    insert_events,
    insert_users,
)

pytestmark = pytest.mark.integration


def test_I01_deployment_is_idempotent(ch_db):
    client, databases = ch_db

    count = apply_sql(client, database_overrides=databases)

    assert count >= 15
    assert client.query(f"EXISTS TABLE {databases['core']}.user_events").first_item == {"result": 1}


def test_I02_fixture_aggregates_and_hash_match_python(ch_db):
    client, databases = ch_db
    users, events = fixture_rows("users.csv"), fixture_rows("user_events.csv")
    insert_users(client, databases["core"], users)

    insert_events(client, databases["core"], events)

    assert aggregate_rows(client, databases["mart"]) == expected_aggregates(events, users)
    actual = client.query(
        f"SELECT user_id, session_id, event_time, event_type, event_key "
        f"FROM {databases['core']}.user_events FINAL"
    ).result_rows
    assert all(
        key == event_key(user, session, timestamp, kind)
        for user, session, timestamp, kind, key in actual
    )


def test_I03_late_event_uses_event_date(ch_db):
    client, databases = ch_db
    events = [
        make_event(event_time="2025-01-01 01:00:00"),
        make_event(event_time="2024-12-01 01:00:00"),
    ]
    insert_events(client, databases["core"], events[:1])

    insert_events(client, databases["core"], events[1:])

    assert aggregate_rows(client, databases["mart"]) == expected_aggregates(events, [])


def test_I04_unknown_user_and_orphan_ratio(ch_db):
    client, databases = ch_db
    users = [make_user()]
    events = [make_event(), make_event(user_id=999, session_id="orphan")]
    insert_users(client, databases["core"], users)

    insert_events(client, databases["core"], events)

    assert aggregate_rows(client, databases["mart"]) == expected_aggregates(events, users)
    assert client.query(
        f"SELECT event_count, orphan_count, orphan_ratio FROM {databases['dq']}.user_event_orphans"
    ).result_rows == [(2, 1, 0.5)]


def test_I05_user_updates_deletes_and_dictionary_latest_state(ch_db):
    client, databases = ch_db
    core = databases["core"]
    insert_users(client, core, [make_user()], version=10)
    insert_users(client, core, [make_user(city="Tehran")], version=20)
    insert_users(client, core, [make_user(city="stale")], version=5)

    assert client.query(
        f"SELECT city FROM {core}.users FINAL WHERE _is_deleted = 0"
    ).result_rows == [("Tehran",)]
    assert client.query(
        f"SELECT dictGetOrDefault('{core}.users_dict', 'city', toInt64(3), 'unknown')"
    ).result_rows == [("Tehran",)]
    insert_users(client, core, [make_user()], version=30, deleted=1)
    assert client.query(
        f"SELECT count() FROM {core}.users FINAL WHERE _is_deleted = 0"
    ).result_rows == [(0,)]
    assert client.query(
        f"SELECT dictGetOrDefault('{core}.users_dict', 'city', toInt64(3), 'unknown')"
    ).result_rows == [("unknown",)]


def test_I13_rebuild_reenriches_late_users_and_preserves_other_partitions(ch_db):
    from azki_pipeline.reconcile import rebuild_aggregate_partition

    client, databases = ch_db
    events = [make_event(), make_event(event_time="2025-09-01 01:00:00")]
    insert_events(client, databases["core"], events)
    insert_users(client, databases["core"], [make_user()])

    assert aggregate_rows(client, databases["mart"]) == expected_aggregates(events, [])
    rebuild_aggregate_partition(client, 202510, database_overrides=databases)

    expected = sorted(
        expected_aggregates(events[:1], [make_user()]) + expected_aggregates(events[1:], [])
    )
    assert aggregate_rows(client, databases["mart"]) == expected
    client.command(f"ALTER TABLE {databases['core']}.user_events DROP PARTITION 202510")
    rebuild_aggregate_partition(client, 202510, database_overrides=databases)
    assert aggregate_rows(client, databases["mart"]) == expected_aggregates(events[1:], [])
