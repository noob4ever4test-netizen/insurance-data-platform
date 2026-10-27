import subprocess
from datetime import UTC, datetime, timedelta

import pytest
from confluent_kafka import Producer

from azki_pipeline.backfill.job import run_backfill
from azki_pipeline.connect_client import ConnectClient
from azki_pipeline.dq.runner import exit_code, run_checks
from tests.e2e.test_part1 import part1_stack as part1_stack
from tests.helpers.eventually import eventually
from tests.helpers.factories import make_event
from tests.integration.test_backfill import write_events
from tests.integration.test_kafka_part1 import publish

pytestmark = pytest.mark.e2e


def test_E04_connect_restart_recovers_files_staged_during_outage(part1_stack, tmp_path):
    client, databases, spool, command = part1_stack
    events = [make_event(session_id=f"outage_{index}") for index in range(3)]
    path = write_events(tmp_path / "outage.csv", events)
    target = f"connect:{spool['config']['input.path']}/outage.csv"

    subprocess.run([*command, "stop", "connect"], check=True, timeout=60)
    try:
        subprocess.run([*command, "cp", str(path), target], check=True, timeout=30)
    finally:
        subprocess.run(
            [*command, "up", "-d", "--wait", "--wait-timeout", "120", "connect"],
            check=True,
            timeout=180,
        )
    ConnectClient("http://127.0.0.1:8083").wait_running(spool["name"], timeout=120)

    def recovered():
        rows = client.query(
            f"SELECT session_id FROM {databases['core']}.user_events FINAL ORDER BY session_id"
        ).result_rows
        assert rows == [(event["session_id"],) for event in events]

    eventually(recovered, timeout=120, interval=0.5)
    assert client.query(
        f"SELECT count(), uniqExact(event_key) FROM {databases['core']}.user_events FINAL"
    ).first_row == (len(events), len(events))


def test_E06_healthy_deleted_hour_backfill_healthy(part1_stack, spark, tmp_path):
    client, databases, spool, _command = part1_stack
    now = datetime.now(UTC).replace(microsecond=0)
    target = now.replace(minute=0, second=0) - timedelta(hours=1)
    timestamps = [target - timedelta(days=day) for day in range(8)] + [now]
    events = [
        make_event(
            event_time=stamp.strftime("%Y-%m-%dT%H:%M:%SZ"), session_id=f"dq_recovery_{index}"
        )
        for index, stamp in enumerate(timestamps)
    ]
    topic = spool["config"]["topic"]
    publish(Producer({"bootstrap.servers": "127.0.0.1:9092"}), topic, events)
    manifest = {target.strftime("%Y-%m-%dT%H:00:00Z"): 1}
    options = dict(
        database_overrides=databases,
        source_counts=manifest,
        kafka_bootstrap="127.0.0.1:9092",
        event_topic=topic,
        consumer_group=f"azki-events-{spool['name'].rsplit('-', 1)[1]}",
    )

    def healthy():
        results = run_checks(client, **options)
        assert {result.name: result.status for result in results} == {
            result.name: "PASS" for result in results
        }, [result.model_dump(mode="json") for result in results if result.status != "PASS"]
        assert exit_code(results) == 0

    eventually(healthy, timeout=120, interval=1)
    client.command(
        f"ALTER TABLE {databases['core']}.user_events DELETE "
        "WHERE event_time >= {start:DateTime('UTC')} AND event_time < {end:DateTime('UTC')} "
        "SETTINGS mutations_sync = 2",
        parameters={"start": target, "end": target + timedelta(hours=1)},
    )

    broken = run_checks(client, **options)

    assert (
        next(result for result in broken if result.name == "source_reconciliation").status == "FAIL"
    )
    assert exit_code(broken) == 1
    event = events[0] | {"event_time": target.strftime("%Y-%m-%d %H:%M:%S")}
    path = write_events(tmp_path / "recovery.csv", [event])
    result = run_backfill(
        spark,
        client,
        source="csv",
        path=path,
        start=target,
        end=target + timedelta(hours=1),
        writers_paused=True,
        database_overrides=databases,
    )
    assert result["written"] == 1
    eventually(healthy, timeout=60, interval=1)
    assert client.query(f"SELECT count() FROM {databases['core']}.user_events FINAL").first_row == (
        len(events),
    )
