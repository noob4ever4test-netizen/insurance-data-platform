import json

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from tests.helpers.eventually import eventually
from tests.helpers.factories import make_event
from tests.helpers.pipeline import aggregate_rows, expected_aggregates

pytestmark = pytest.mark.integration


@pytest.fixture
def kafka_pipeline(kafka, ch_db):
    client, databases = ch_db
    suffix = databases["core"].removeprefix("test_core_")
    topic = f"azki.user_events_{suffix}"
    bootstrap = kafka.get_bootstrap_server().replace("localhost:", "127.0.0.1:")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for future in admin.create_topics([NewTopic(topic, 1, 1)]).values():
        future.result(timeout=30)
    producer = Producer({"bootstrap.servers": bootstrap})
    yield client, databases, topic, producer
    producer.flush(10)
    for future in admin.delete_topics([topic]).values():
        future.result(timeout=30)


def publish(producer, topic, messages):
    errors = []
    for message in messages:
        value = message if isinstance(message, str) else json.dumps(message)
        producer.produce(topic, value=value, on_delivery=lambda error, _: errors.append(error))
    assert producer.flush(15) == 0
    assert errors and not any(errors)


def test_I11_kafka_events_have_offsets_and_forward_compatible_columns(kafka_pipeline):
    client, databases, topic, producer = kafka_pipeline
    event = make_event(event_time="2025-10-16T17:17:33Z", utm_campaign="synthetic")

    publish(producer, topic, [event])

    def check():
        rows = client.query(
            f"SELECT user_id, _topic, _partition, _offset "
            f"FROM {databases['core']}.user_events FINAL"
        ).result_rows
        assert rows == [(event["user_id"], topic, 0, 0)]

    eventually(check, timeout=60, interval=0.5)


@pytest.mark.parametrize("message", ["not-json", '{"event_time":"invalid"}'])
def test_I12_malformed_message_is_quarantined_and_stream_continues(kafka_pipeline, message):
    client, databases, topic, producer = kafka_pipeline

    publish(producer, topic, [message, make_event()])

    def check():
        assert client.query(
            f"SELECT count() FROM {databases['core']}.user_events FINAL"
        ).result_rows == [(1,)]
        rows = client.query(
            f"SELECT raw_message, error, topic, offset FROM {databases['dq']}.kafka_dead_letter"
        ).result_rows
        assert len(rows) == 1
        assert rows[0][0] == message
        assert rows[0][1]
        assert rows[0][2:] == (topic, 0)

    eventually(check, timeout=60, interval=0.5)


def test_I13_duplicate_delivery_is_deduped_and_partition_rebuilt(kafka_pipeline):
    from azki_pipeline.reconcile import rebuild_aggregate_partition

    client, databases, topic, producer = kafka_pipeline
    event = make_event()
    publish(producer, topic, [event, event])

    def check():
        assert aggregate_rows(client, databases["mart"]) == expected_aggregates([event, event], [])

    eventually(check, timeout=60, interval=0.5)
    assert client.query(
        f"SELECT count() FROM {databases['core']}.user_events FINAL"
    ).result_rows == [(1,)]
    rebuild_aggregate_partition(client, 202510, database_overrides=databases)
    assert aggregate_rows(client, databases["mart"]) == expected_aggregates([event], [])
    rebuild_aggregate_partition(client, 202510, database_overrides=databases)
    assert aggregate_rows(client, databases["mart"]) == expected_aggregates([event], [])
