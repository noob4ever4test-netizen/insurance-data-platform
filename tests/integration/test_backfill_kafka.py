import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pyspark
import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from tests.helpers.factories import make_event

pytestmark = pytest.mark.integration


def test_I15_bounded_kafka_backfill_preserves_offsets_across_partitions(ch_db, kafka):
    client, databases = ch_db
    bootstrap = kafka.get_bootstrap_server().replace("localhost:", "127.0.0.1:")
    topic = f"backfill.source.{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for future in admin.create_topics([NewTopic(topic, 2, 1)]).values():
        future.result(timeout=30)
    start = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=1)
    end = start + timedelta(seconds=30)
    producer = Producer({"bootstrap.servers": bootstrap})
    errors = []
    try:
        for partition in range(2):
            for seconds in (-1, 1, 30):
                timestamp = start + timedelta(seconds=seconds)
                event = make_event(
                    event_time=timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    session_id=f"partition_{partition}_second_{seconds}",
                )
                producer.produce(
                    topic,
                    partition=partition,
                    timestamp=int(timestamp.timestamp() * 1000),
                    value=json.dumps(event),
                    on_delivery=lambda error, _: errors.append(error),
                )
        assert producer.flush(15) == 0
        assert errors and not any(errors)
        options = dict(
            clickhouse_url=client.url,
            database_overrides=databases,
            source="kafka",
            topic=topic,
            kafka_bootstrap=bootstrap,
            start=start.isoformat(),
            end=end.isoformat(),
            writers_paused=True,
        )
        environment = os.environ | {
            "SPARK_HOME": str(Path(pyspark.__file__).parent),
            "PYSPARK_PYTHON": sys.executable,
            "PYSPARK_SUBMIT_ARGS": "--master local[1] pyspark-shell",
        }

        process = subprocess.run(
            [sys.executable, "-m", "tests.helpers.backfill_process"],
            input=json.dumps(options),
            capture_output=True,
            text=True,
            env=environment,
            timeout=300,
        )

        assert process.returncode == 0, process.stdout + process.stderr
        first, second = json.loads(process.stdout.strip().splitlines()[-1])
        assert (first["rows_read"], first["invalid"], first["written"], second["written"]) == (
            2,
            0,
            2,
            0,
        )
        assert client.query(
            f"SELECT _topic, _partition, _offset FROM {databases['core']}.user_events FINAL "
            "ORDER BY _partition"
        ).result_rows == [(topic, 0, 1), (topic, 1, 1)]
    finally:
        producer.flush(10)
        for future in admin.delete_topics([topic]).values():
            future.result(timeout=30)
