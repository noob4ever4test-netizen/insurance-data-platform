import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from confluent_kafka import ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from azki_pipeline.dq.collectors import collect_kafka_lag
from azki_pipeline.dq.runner import collect_metrics
from tests.helpers.eventually import eventually
from tests.helpers.factories import make_event
from tests.integration.test_kafka_part1 import kafka_pipeline as kafka_pipeline
from tests.integration.test_kafka_part1 import publish

pytestmark = pytest.mark.integration


def test_I14_lag_collector_reads_broker_high_watermarks_and_group_offsets(kafka):
    bootstrap = kafka.get_bootstrap_server().replace("localhost:", "127.0.0.1:")
    topic = f"dq-offsets-{uuid4().hex}"
    group = f"dq-probe-{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(topic, 2, 1)])[topic].result(timeout=30)
    try:
        producer = Producer({"bootstrap.servers": bootstrap})
        errors = []
        for partition, count in ((0, 2), (1, 3)):
            for index in range(count):
                producer.produce(
                    topic,
                    partition=partition,
                    value=json.dumps(make_event(session_id=f"partition_{partition}_{index}")),
                    on_delivery=lambda error, message: errors.append(error),
                )
        assert producer.flush(15) == 0
        assert len(errors) == 5 and not any(errors)
        admin.alter_consumer_group_offsets(
            [
                ConsumerGroupTopicPartitions(
                    group,
                    [TopicPartition(topic, 0, 1), TopicPartition(topic, 1, 0)],
                )
            ]
        )[group].result(timeout=30)

        values = collect_kafka_lag(bootstrap, topic, group)

        assert sorted(values, key=lambda row: row["partition"]) == [
            {"topic": topic, "partition": 0, "committed": 1, "low": 0, "high": 2},
            {"topic": topic, "partition": 1, "committed": 0, "low": 0, "high": 3},
        ]
    finally:
        admin.delete_topics([topic])[topic].result(timeout=30)


def test_I14_heartbeat_kafka_mv_preserves_source_timestamp(kafka_pipeline):
    client, databases, _topic, producer = kafka_pipeline
    suffix = databases["core"].removeprefix("test_core_")
    now = datetime.now(UTC)
    source_ms = int(now.timestamp() * 1000)

    publish(producer, f"__debezium-heartbeat.mysql_{suffix}", [{"ts_ms": source_ms}])

    def check():
        values = collect_metrics(client, now=datetime.now(UTC), database_overrides=databases)
        assert values["cdc_lag"]["last_heartbeat_at"] == datetime.fromtimestamp(
            source_ms / 1000, UTC
        )

    eventually(check, timeout=60, interval=0.5)
