from datetime import UTC, datetime

import pytest
from confluent_kafka import Producer

from azki_pipeline.dq.checks import evaluate_schema_drift
from azki_pipeline.dq.runner import collect_metrics
from tests.e2e.test_part1 import part1_stack as part1_stack
from tests.helpers.eventually import eventually
from tests.helpers.factories import make_event
from tests.integration.test_kafka_part1 import publish

pytestmark = pytest.mark.e2e


def test_E05_added_field_warns_and_event_ingestion_continues(part1_stack):
    client, databases, spool, _command = part1_stack
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    events = [
        make_event(event_time=timestamp, session_id="dq_evolved", utm_campaign="synthetic"),
        make_event(event_time=timestamp, session_id="dq_after_change"),
    ]
    producer = Producer({"bootstrap.servers": "127.0.0.1:9092"})

    publish(producer, spool["config"]["topic"], events)

    def check():
        assert client.query(
            f"SELECT session_id FROM {databases['core']}.user_events FINAL ORDER BY session_id"
        ).result_rows == [("dq_after_change",), ("dq_evolved",)]
        now = datetime.now(UTC)
        values = collect_metrics(
            client, now=now, database_overrides=databases, event_topic=spool["config"]["topic"]
        )
        result = evaluate_schema_drift({**values["schema_drift"], "checked_at": now}, {})
        assert result.status == "WARN"
        assert result.details["added"] == ["utm_campaign"]
        assert (
            client.query(f"SELECT count() FROM {databases['dq']}.kafka_dead_letter").first_row[0]
            == 0
        )

    eventually(check, timeout=60, interval=0.5)
