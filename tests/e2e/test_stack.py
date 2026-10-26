import csv
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from azki_pipeline.connect_client import ConnectClient
from tests.helpers.eventually import eventually

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.e2e
def test_E01_stack_healthy_and_connectors_run(compose_stack):
    suffix = uuid4().hex[:12]
    client = ConnectClient("http://localhost:8083")
    documents = [
        json.loads((ROOT / "connect" / filename).read_text())
        for filename in ("debezium-mysql-source.json", "spooldir-user-events.json")
    ]
    mysql, spool = documents
    for document in documents:
        document["name"] += f"-{suffix}"
    mysql["config"]["topic.prefix"] = f"mysql_{suffix}"
    mysql["config"]["database.server.id"] = str(10000 + int(suffix[:6], 16))
    mysql["config"]["schema.history.internal.kafka.topic"] += f".{suffix}"
    spool["config"]["topic"] = f"test.events.{suffix}"
    users_topic = f"{mysql['config']['topic.prefix']}.azki.users"
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
    for future in admin.create_topics(
        [
            NewTopic(topic, num_partitions=3, replication_factor=1)
            for topic in (users_topic, spool["config"]["topic"])
        ],
        request_timeout=15,
    ).values():
        future.result(timeout=20)
    for key, directory in (
        ("input.path", "input"),
        ("finished.path", "finished"),
        ("error.path", "error"),
    ):
        spool["config"][key] = f"/spool/{suffix}/{directory}"
    subprocess.run(
        [
            *compose_stack,
            "exec",
            "-T",
            "connect",
            "mkdir",
            "-p",
            *[spool["config"][key] for key in ("input.path", "finished.path", "error.path")],
        ],
        check=True,
        timeout=30,
    )
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": f"e01-{suffix}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    registered = []
    try:
        for document in documents:
            client.register(document)
            registered.append(document["name"])
            status = client.wait_running(document["name"], timeout=120)
            assert status["connector"]["state"] == "RUNNING"
            assert status["tasks"]
            assert all(task["state"] == "RUNNING" for task in status["tasks"])
        subprocess.run(
            [
                *compose_stack,
                "cp",
                str(ROOT / "tests" / "fixtures" / "user_events.csv"),
                f"connect:{spool['config']['input.path']}/synthetic.csv.tmp",
            ],
            check=True,
            timeout=30,
        )
        subprocess.run(
            [
                *compose_stack,
                "exec",
                "-T",
                "connect",
                "mv",
                f"{spool['config']['input.path']}/synthetic.csv.tmp",
                f"{spool['config']['input.path']}/synthetic.csv",
            ],
            check=True,
            timeout=30,
        )
        consumer.subscribe([spool["config"]["topic"], users_topic])
        with (ROOT / "tests" / "fixtures" / "users.csv").open(newline="") as stream:
            expected_users = {int(row["user_id"]) for row in csv.DictReader(stream)}
        with (ROOT / "tests" / "fixtures" / "user_events.csv").open(newline="") as stream:
            expected_events = list(csv.DictReader(stream))
        seen_users = set()
        seen_events = []

        def assert_message():
            message = consumer.poll(0.5)
            if message is not None:
                assert message.error() is None, str(message.error())
                record = json.loads(message.value())
                if message.topic() == users_topic:
                    seen_users.add(record["user_id"])
                else:
                    seen_events.append((message.key(), record))
            assert seen_users == expected_users, f"Users snapshot incomplete: {seen_users}"
            assert len(seen_events) == len(expected_events), (
                f"Only {len(seen_events)} events received"
            )

        eventually(assert_message, timeout=60, interval=0.1)
        for key, record in seen_events:
            assert key == str(record["user_id"]).encode()
            assert isinstance(record["premium_amount"], int)
        for document in documents:
            client.wait_running(document["name"], timeout=10)
    finally:
        consumer.close()
        for name in registered:
            client.delete(name)
