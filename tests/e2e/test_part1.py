import json
import subprocess
from pathlib import Path
from uuid import uuid4

import clickhouse_connect
import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from azki_pipeline.connect_client import ConnectClient
from azki_pipeline.contracts import event_key
from azki_pipeline.seed import wait_clickhouse_orders, wait_clickhouse_users
from azki_pipeline.sql_runner import apply_sql, load_sql
from tests.helpers.eventually import eventually
from tests.helpers.factories import PRODUCT_ATTRIBUTES
from tests.helpers.pipeline import aggregate_rows, expected_aggregates, fixture_rows

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.e2e


@pytest.fixture
def part1_stack(compose_stack):
    suffix = uuid4().hex[:12]
    databases = {name: f"test_{name}_{suffix}" for name in ("raw", "core", "mart", "dq", "gov")}
    client = clickhouse_connect.get_client(host="127.0.0.1", port=8123)
    connect = ConnectClient("http://127.0.0.1:8083")
    mysql, spool = [
        json.loads((ROOT / "connect" / name).read_text())
        for name in ("debezium-mysql-source.json", "spooldir-user-events.json")
    ]
    mysql["config"].update(
        {
            "topic.prefix": f"mysql_{suffix}",
            "database.server.id": str(10000 + int(suffix[:6], 16)),
            "schema.history.internal.kafka.topic": f"schema-history.phase3.{suffix}",
        }
    )
    spool["config"]["topic"] = f"test.events.{suffix}"
    for document in (mysql, spool):
        document["name"] += f"-{suffix}"
    for key, directory in (
        ("input.path", "input"),
        ("finished.path", "finished"),
        ("error.path", "error"),
    ):
        spool["config"][key] = f"/spool/{suffix}/{directory}"
    topics = [
        spool["config"]["topic"],
        *[f"mysql_{suffix}.{table}" for table in mysql["config"]["table.include.list"].split(",")],
        f"__debezium-heartbeat.mysql_{suffix}",
    ]
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
    for future in admin.create_topics([NewTopic(topic, 1, 1) for topic in topics]).values():
        future.result(timeout=30)

    class ConfiguredClient:
        def command(self, query):
            query = query.replace("'mysql.azki.", f"'mysql_{suffix}.azki.")
            for name in (
                "third-orders",
                "body-orders",
                "medical-orders",
                "fire-orders",
                "financial-order",
            ):
                query = query.replace(f"'azki-{name}'", f"'azki-{name}-{suffix}'")
            for original, replacement in (
                ("azki.user_events", topics[0]),
                ("mysql.azki.users", topics[1]),
                ("azki-events", f"azki-events-{suffix}"),
                ("azki-users", f"azki-users-{suffix}"),
                ("azki-dq-samples", f"azki-dq-samples-{suffix}"),
                ("azki-dq-heartbeats", f"azki-dq-heartbeats-{suffix}"),
                ("__debezium-heartbeat.mysql", f"__debezium-heartbeat.mysql_{suffix}"),
            ):
                query = query.replace(f"'{original}'", f"'{replacement}'")
            return client.command(query)

    registered = []
    try:
        apply_sql(ConfiguredClient(), database_overrides=databases)
        connect.register(mysql)
        registered.append(mysql["name"])
        connect.wait_running(mysql["name"], timeout=120)

        def users_ready():
            actual = client.query(
                f"SELECT user_id FROM {databases['core']}.users FINAL WHERE _is_deleted = 0"
            ).result_rows
            assert {row[0] for row in actual} == {
                row["user_id"] for row in fixture_rows("users.csv")
            }

        eventually(users_ready, timeout=120, interval=0.5)
        wait_clickhouse_users(client=client, database_overrides=databases)
        wait_clickhouse_orders(client=client, database_overrides=databases)
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
        connect.register(spool)
        registered.append(spool["name"])
        connect.wait_running(spool["name"], timeout=120)
        yield client, databases, spool, compose_stack
    finally:
        for name in reversed(registered):
            connect.delete(name)
        for name in ("raw", "mart", "dq", "core", "gov"):
            client.command(f"DROP DATABASE IF EXISTS {databases[name]} SYNC")
        for query in load_sql(
            ROOT / "sql/test_fixtures/governance/072_governance_roles_cleanup.sql",
            database_overrides=databases,
        ):
            client.command(query)
        for future in admin.delete_topics(topics).values():
            future.result(timeout=30)
        client.close()


def test_E02_seed_users_before_events_and_reconcile_fixture(part1_stack):
    from azki_pipeline.reconcile import rebuild_aggregate_partition

    client, databases, spool, command = part1_stack
    target = f"connect:{spool['config']['input.path']}/events.csv"
    subprocess.run(
        [*command, "cp", str(ROOT / "tests" / "fixtures" / "user_events.csv"), target + ".tmp"],
        check=True,
        timeout=30,
    )
    subprocess.run(
        [
            *command,
            "exec",
            "-T",
            "connect",
            "mv",
            target.removeprefix("connect:") + ".tmp",
            target.removeprefix("connect:"),
        ],
        check=True,
        timeout=30,
    )
    events, users = fixture_rows("user_events.csv"), fixture_rows("users.csv")

    def received():
        assert aggregate_rows(client, databases["mart"]) == expected_aggregates(events, users)

    eventually(received, timeout=120, interval=0.5)
    unique = list(
        {
            (event["user_id"], event["session_id"], event["event_time"], event["event_type"]): event
            for event in events
        }.values()
    )
    assert client.query(
        f"SELECT count() FROM {databases['core']}.user_events FINAL"
    ).result_rows == [(len(unique),)]
    known = {user["user_id"] for user in users}
    missing = sum(event["user_id"] not in known for event in unique)
    assert client.query(
        f"SELECT orphan_ratio FROM {databases['dq']}.user_event_orphans"
    ).result_rows == [(missing / len(unique),)]
    purchases = [event for event in unique if event["event_type"] == "purchase"]
    expected = [
        (
            event_key(event["user_id"], event["session_id"], event["event_time"], "purchase"),
            index + 1,
            list(PRODUCT_ATTRIBUTES)[index % len(PRODUCT_ATTRIBUTES)],
            event["premium_amount"],
            1,
        )
        for index, event in enumerate(purchases)
    ]
    enriched = client.query(
        f"SELECT event_key, order_id, product_type, paid_amount, event_time = order_created_at "
        f"FROM {databases['mart']}.purchase_events_enriched FINAL "
        "WHERE _is_deleted = 0 ORDER BY order_id"
    ).result_rows
    assert enriched == expected
    for partition in sorted({int(event["event_time"][:7].replace("-", "")) for event in events}):
        rebuild_aggregate_partition(client, partition, database_overrides=databases)
    assert aggregate_rows(client, databases["mart"]) == expected_aggregates(unique, users)


def test_E03_mysql_update_reaches_clickhouse_and_dictionary(part1_stack):
    client, databases, _, command = part1_stack
    user_id = 10000 + int(uuid4().hex[:6], 16)

    def mysql(sql):
        subprocess.run(
            [
                *command,
                "exec",
                "-T",
                "mysql",
                "sh",
                "-c",
                'MYSQL_PWD="$(cat /run/secrets/mysql_root_password)" mysql -uroot azki',
            ],
            input=sql,
            text=True,
            capture_output=True,
            check=True,
            timeout=30,
        )

    def assert_city(city):
        rows = client.query(
            f"SELECT city, _version FROM {databases['core']}.users FINAL "
            "WHERE user_id = {user_id:Int64} AND _is_deleted = 0",
            parameters={"user_id": user_id},
        ).result_rows
        assert len(rows) == 1 and rows[0][0] == city and rows[0][1] > 0

    try:
        mysql(f"INSERT INTO users VALUES ({user_id}, '2025-01-01', 'Shiraz', 'Mobile');")
        eventually(lambda: assert_city("Shiraz"), timeout=60, interval=0.5)

        mysql(f"UPDATE users SET city = 'Tehran' WHERE user_id = {user_id};")

        eventually(lambda: assert_city("Tehran"), timeout=60, interval=0.5)
        client.command(f"SYSTEM RELOAD DICTIONARY {databases['core']}.users_dict")
        assert client.query(
            f"SELECT dictGetOrDefault('{databases['core']}.users_dict', 'city', "
            "{user_id:Int64}, 'unknown')",
            parameters={"user_id": user_id},
        ).result_rows == [("Tehran",)]
        mysql(f"DELETE FROM users WHERE user_id = {user_id};")

        def deleted():
            assert client.query(
                f"SELECT count() FROM {databases['core']}.users FINAL "
                "WHERE user_id = {user_id:Int64} AND _is_deleted = 0",
                parameters={"user_id": user_id},
            ).result_rows == [(0,)]

        eventually(deleted, timeout=60, interval=0.5)
    finally:
        mysql(f"DELETE FROM users WHERE user_id = {user_id};")
