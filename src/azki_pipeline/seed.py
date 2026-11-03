import argparse
import csv
import json
import os
import secrets
import subprocess
from pathlib import Path
from threading import Event
from time import monotonic
from uuid import uuid4

import clickhouse_connect
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from azki_pipeline.connect_client import ConnectClient
from azki_pipeline.contracts import event_key
from azki_pipeline.sql_runner import load_sql

ROOT = Path(__file__).resolve().parents[2]
_wait = Event().wait
PRODUCTS = {
    "third": "plate_no",
    "body": "car_value",
    "medical": "coverage_limit",
    "fire": "property_value",
}
ORDER_FIELDS = [
    "order_id",
    "user_id",
    "session_id",
    "created_at",
    "partner_id",
    "national_id",
    "phone",
    "policy_no",
    "premium_amount",
]
FINANCIAL_FIELDS = [
    "order_id",
    "user_id",
    "session_id",
    "created_at",
    "product_type",
    "payment_status",
    "paid_amount",
]


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def prepare_environment(root: Path = ROOT) -> None:
    """Create stable local credentials and order CSVs derived only from synthetic fixtures."""
    secret_dir = root / "docker" / "secrets"
    secret_dir.mkdir(parents=True, exist_ok=True)
    for name in ("mysql_root_password.txt", "mysql_cdc_password.txt"):
        path = secret_dir / name
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            path.chmod(0o600)
        else:
            with os.fdopen(descriptor, "w") as stream:
                stream.write(secrets.token_hex(24) + "\n")
    properties = secret_dir / "mysql.properties"
    descriptor = os.open(properties, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write("password=" + (secret_dir / "mysql_cdc_password.txt").read_text())
    properties.chmod(0o600)

    generated = root / "docker" / "generated"
    generated.mkdir(parents=True, exist_ok=True)
    orders = {product: [] for product in PRODUCTS}
    financial = []
    seen = set()
    with (root / "tests" / "fixtures" / "user_events.csv").open(newline="") as stream:
        for event in csv.DictReader(stream):
            if event["event_type"] != "purchase":
                continue
            key = event_key(
                int(event["user_id"]), event["session_id"], event["event_time"], "purchase"
            )
            if key in seen:
                continue
            seen.add(key)
            index = len(financial)
            product = list(PRODUCTS)[index % len(PRODUCTS)]
            order_id = index + 1
            order = {
                "order_id": order_id,
                "user_id": event["user_id"],
                "session_id": event["session_id"],
                "created_at": event["event_time"],
                "partner_id": f"partner_{index % 2 + 1}",
                "national_id": f"{order_id:010d}",
                "phone": f"090{order_id:08d}",
                "policy_no": f"SYNTHETIC-{order_id}",
                "premium_amount": event["premium_amount"],
                PRODUCTS[product]: f"TEST-{order_id}" if product == "third" else 100000000,
            }
            orders[product].append(order)
            financial.append(
                {
                    "order_id": order_id,
                    "user_id": event["user_id"],
                    "session_id": event["session_id"],
                    "created_at": event["event_time"],
                    "product_type": product,
                    "payment_status": "paid",
                    "paid_amount": event["premium_amount"],
                }
            )
    for product, rows in orders.items():
        _write_csv(generated / f"{product}_orders.csv", [*ORDER_FIELDS, PRODUCTS[product]], rows)
    _write_csv(generated / "financial_order.csv", FINANCIAL_FIELDS, financial)


def wait_users_snapshot(bootstrap_servers: str, *, timeout: float = 120) -> None:
    with (ROOT / "tests" / "fixtures" / "users.csv").open(newline="") as stream:
        expected = {int(row["user_id"]) for row in csv.DictReader(stream)}
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"azki-seed-{uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "topic.metadata.refresh.interval.ms": 1000,
        }
    )
    try:
        deadline = monotonic() + timeout
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Users snapshot incomplete; missing {sorted(expected)}")
            topic = consumer.list_topics("mysql.azki.users", timeout=min(10, remaining)).topics.get(
                "mysql.azki.users"
            )
            if topic is not None and not topic.error and topic.partitions:
                consumer.assign(
                    [
                        TopicPartition("mysql.azki.users", number, OFFSET_BEGINNING)
                        for number in sorted(topic.partitions)
                    ]
                )
                break
            _wait(min(0.5, remaining))
        while expected:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Users snapshot incomplete; missing {sorted(expected)}")
            message = consumer.poll(min(1.0, remaining))
            if message is None:
                continue
            if message.error():
                continue
            if message.value() is not None:
                expected.discard(int(json.loads(message.value())["user_id"]))
    finally:
        consumer.close()


def wait_clickhouse_users(
    *, client=None, host="127.0.0.1", port=8123, timeout=120, database_overrides=None
) -> None:
    """Wait for synthetic user IDs and dictionary values matching their latest CDC rows."""
    with (ROOT / "tests" / "fixtures" / "users.csv").open(newline="") as stream:
        expected = {int(row["user_id"]) for row in csv.DictReader(stream)}
    query = load_sql(
        ROOT / "sql" / "seed" / "001_users_ready.sql", database_overrides=database_overrides
    )[0]
    owned = client is None
    if owned:
        client = clickhouse_connect.get_client(host=host, port=port)
    try:
        deadline = monotonic() + timeout
        while True:
            rows = client.query(query).result_rows
            ready = {
                user_id
                for user_id, city, device, dictionary_city, dictionary_device in rows
                if city == dictionary_city and device == dictionary_device
            }
            missing = expected - ready
            if not missing:
                return
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"ClickHouse users/dictionary not ready; missing {sorted(missing)}"
                )
            _wait(min(0.5, remaining))
    finally:
        if owned:
            client.close()


def wait_clickhouse_orders(
    *, client=None, host="127.0.0.1", port=8123, timeout=120, database_overrides=None
) -> None:
    """Wait for every fixture purchase's fully joined order and matching dictionary entry."""
    with (ROOT / "tests" / "fixtures" / "user_events.csv").open(newline="") as stream:
        expected = {
            (int(row["user_id"]), row["session_id"])
            for row in csv.DictReader(stream)
            if row["event_type"] == "purchase"
        }
    query = load_sql(
        ROOT / "sql" / "seed" / "002_orders_ready.sql", database_overrides=database_overrides
    )[0]
    owned = client is None
    if owned:
        client = clickhouse_connect.get_client(host=host, port=port)
    try:
        deadline = monotonic() + timeout
        while True:
            rows = client.query(query).result_rows
            ready = {(user_id, session_id) for user_id, session_id, matches in rows if matches}
            missing = expected - ready
            if not missing:
                return
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"ClickHouse orders/dictionary not ready; missing {sorted(missing)}"
                )
            _wait(min(0.5, remaining))
    finally:
        if owned:
            client.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare and seed the synthetic Compose stack")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--connect-url", default="http://localhost:8083")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--clickhouse-host", default="127.0.0.1")
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    args = parser.parse_args(argv)
    prepare_environment()
    if args.prepare_only:
        return 0
    client = ConnectClient(args.connect_url)
    for filename in ("debezium-mysql-source.json", "spooldir-user-events.json"):
        document = json.loads((ROOT / "connect" / filename).read_text())
        client.register(document)
        client.wait_running(document["name"], timeout=120)
        if filename == "debezium-mysql-source.json":
            wait_users_snapshot(args.bootstrap_servers)
            wait_clickhouse_users(host=args.clickhouse_host, port=args.clickhouse_port)
            wait_clickhouse_orders(host=args.clickhouse_host, port=args.clickhouse_port)
    command = ["docker", "compose", "-f", str(ROOT / "docker" / "compose.yml")]
    target = f"/spool/input/seed_{uuid4().hex}.csv"
    subprocess.run(
        [
            *command,
            "cp",
            str(ROOT / "tests" / "fixtures" / "user_events.csv"),
            f"connect:{target}.tmp",
        ],
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
            f"{target}.tmp",
            target,
        ],
        check=True,
        timeout=30,
    )
    print("Users snapshot observed; synthetic events staged for spooldir.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
