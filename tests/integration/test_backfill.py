import csv
from datetime import UTC, datetime

import pytest

from tests.helpers.factories import make_event, make_financial, make_order, make_user
from tests.helpers.pipeline import aggregate_rows, expected_aggregates, insert_events, insert_users
from tests.helpers.purchases import active_purchases, insert_financial, insert_order, reload_orders

pytestmark = pytest.mark.integration
START = datetime(2025, 10, 1, tzinfo=UTC)
END = datetime(2025, 11, 1, tzinfo=UTC)


def write_events(path, events):
    fields = list(make_event())
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(events)
    return path


def test_I15_fill_gaps_dry_run_and_second_run_noop(ch_db, spark, tmp_path):
    from azki_pipeline.backfill.job import run_backfill

    client, databases = ch_db
    events = [make_event(), make_event(session_id="gap", event_time="2025-10-16 18:00:00")]
    insert_users(client, databases["core"], [make_user()])
    insert_events(client, databases["core"], events)
    client.command(
        f"ALTER TABLE {databases['core']}.user_events DELETE WHERE session_id = 'gap' "
        "SETTINGS mutations_sync = 2"
    )
    path = write_events(tmp_path / "events.csv", [*events, events[1]])
    options = dict(
        source="csv",
        path=path,
        start=START,
        end=END,
        database_overrides=databases,
        writers_paused=True,
    )

    dry = run_backfill(spark, client, dry_run=True, **options)

    assert (dry["rows_read"], dry["invalid"], dry["missing"], dry["written"]) == (3, 0, 1, 0)
    assert client.query(f"SELECT count() FROM {databases['core']}.user_events FINAL").first_row == (
        1,
    )
    first = run_backfill(spark, client, **options)
    second = run_backfill(spark, client, **options)
    assert first["written"] == 1
    assert second["written"] == 0
    assert aggregate_rows(client, databases["mart"]) == expected_aggregates(events, [make_user()])
    assert client.query(
        f"SELECT status, written FROM {databases['dq']}.backfill_runs ORDER BY started_at"
    ).result_rows == [("dry-run", 0), ("success", 1), ("success", 0)]


def test_I16_replace_partition_removes_stale_rows_and_rebuilds_marts(ch_db, spark, tmp_path):
    from azki_pipeline.backfill.job import run_backfill

    client, databases = ch_db
    outside = make_event(event_time="2025-09-01 00:00:00", session_id="outside")
    stale = make_event(event_type="purchase", session_id="stale")
    replacement = make_event(session_id="replacement", premium_amount=7654)
    insert_users(client, databases["core"], [make_user()])
    insert_events(client, databases["core"], [stale, outside])
    path = write_events(tmp_path / "replacement.csv", [replacement, replacement])

    result = run_backfill(
        spark,
        client,
        source="csv",
        path=path,
        start=START,
        end=END,
        mode="replace-partition",
        database_overrides=databases,
        writers_paused=True,
    )

    assert result["written"] == 1
    assert client.query(
        f"SELECT session_id FROM {databases['core']}.user_events FINAL ORDER BY session_id"
    ).result_rows == [("outside",), ("replacement",)]
    assert aggregate_rows(client, databases["mart"]) == expected_aggregates(
        [outside, replacement], [make_user()]
    )
    assert client.query(
        f"SELECT count() FROM {databases['mart']}.purchase_events_enriched FINAL"
    ).first_row == (0,)
    empty = write_events(tmp_path / "empty.csv", [])
    run_backfill(
        spark,
        client,
        source="csv",
        path=empty,
        start=START,
        end=END,
        mode="replace-partition",
        database_overrides=databases,
        writers_paused=True,
    )
    assert aggregate_rows(client, databases["mart"]) == expected_aggregates(
        [outside], [make_user()]
    )


def test_I16_invalid_replacement_never_changes_live_partition(ch_db, spark, tmp_path):
    from azki_pipeline.backfill.job import run_backfill

    client, databases = ch_db
    event = make_event()
    insert_events(client, databases["core"], [event])
    path = write_events(tmp_path / "invalid.csv", [make_event(premium_amount=-1)])

    with pytest.raises(ValueError, match="invalid"):
        run_backfill(
            spark,
            client,
            source="csv",
            path=path,
            start=START,
            end=END,
            mode="replace-partition",
            database_overrides=databases,
            writers_paused=True,
        )

    assert client.query(f"SELECT count() FROM {databases['core']}.user_events FINAL").first_row == (
        1,
    )
    assert client.query(f"SELECT status FROM {databases['dq']}.backfill_runs").first_row == (
        "failed",
    )


def test_I16_replacement_rebuilds_purchase_payload_and_is_repeatable(ch_db, spark, tmp_path):
    from azki_pipeline.backfill.job import run_backfill
    from azki_pipeline.contracts import event_key

    client, databases = ch_db
    event = make_event(event_type="purchase")
    insert_order(client, databases["core"], make_order())
    insert_financial(client, databases["core"], make_financial())
    reload_orders(client, databases["core"])
    path = write_events(tmp_path / "purchase.csv", [event])
    options = dict(
        source="csv",
        path=path,
        start=START,
        end=END,
        mode="replace-partition",
        database_overrides=databases,
        writers_paused=True,
    )

    run_backfill(spark, client, **options)
    run_backfill(spark, client, **options)

    key = event_key(event["user_id"], event["session_id"], event["event_time"], event["event_type"])
    assert active_purchases(client, databases["mart"]) == [
        (key, 1, "third", make_financial()["paid_amount"], make_order()["policy_no"])
    ]
    assert aggregate_rows(client, databases["mart"]) == expected_aggregates([event], [])


def test_I15_partial_write_is_audited_and_retry_fills_only_remaining_gap(ch_db, spark, tmp_path):
    from azki_pipeline.backfill.job import run_backfill

    client, databases = ch_db
    events = [make_event(session_id=f"batch_{index}") for index in range(1001)]
    path = write_events(tmp_path / "batches.csv", events)
    options = dict(
        source="csv",
        path=path,
        start=START,
        end=END,
        database_overrides=databases,
        writers_paused=True,
    )

    class InterruptedClient:
        def __init__(self):
            self.batches = 0

        def __getattr__(self, name):
            return getattr(client, name)

        def insert(self, table, rows, **kwargs):
            if table == f"{databases['core']}.user_events":
                self.batches += 1
                if self.batches == 2:
                    raise RuntimeError("synthetic interrupted insert")
            return client.insert(table, rows, **kwargs)

    with pytest.raises(RuntimeError, match="synthetic interrupted"):
        run_backfill(spark, InterruptedClient(), **options)

    assert client.query(
        f"SELECT status, written, error FROM {databases['dq']}.backfill_runs"
    ).first_row == ("failed", 1000, "RuntimeError")
    recovered = run_backfill(spark, client, **options)
    assert recovered["written"] == 1
    assert aggregate_rows(client, databases["mart"]) == expected_aggregates(events, [])
