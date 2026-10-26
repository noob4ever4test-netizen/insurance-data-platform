from datetime import UTC, datetime

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from tests.helpers.eventually import eventually
from tests.helpers.factories import PRODUCT_ATTRIBUTES, make_event, make_financial, make_order
from tests.helpers.pipeline import insert_events
from tests.helpers.purchases import active_purchases, reload_orders
from tests.integration.test_kafka_part1 import publish

pytestmark = pytest.mark.integration


@pytest.fixture
def purchase_kafka(kafka, ch_db):
    client, databases = ch_db
    suffix = databases["core"].removeprefix("test_core_")
    topics = {
        table: f"mysql.azki.{table}_{suffix}"
        for table in [*[f"{product}_orders" for product in PRODUCT_ATTRIBUTES], "financial_order"]
    }
    bootstrap = kafka.get_bootstrap_server().replace("localhost:", "127.0.0.1:")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for future in admin.create_topics(
        [NewTopic(topic, 1, 1) for topic in topics.values()]
    ).values():
        future.result(timeout=30)
    producer = Producer({"bootstrap.servers": bootstrap})
    yield client, databases, topics, producer
    producer.flush(10)
    for future in admin.delete_topics(list(topics.values())).values():
        future.result(timeout=30)


def debezium_row(row, *, order=False):
    result = {key: value for key, value in row.items() if not key.startswith("_")}
    if order:
        result.pop("product_type")
    timestamp = datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    result["created_at"] = int(timestamp.timestamp()) * 1000
    result["__ts_ms"] = row["_version"]
    result["__deleted"] = "true" if row["_is_deleted"] else "false"
    return result


@pytest.mark.parametrize("product", PRODUCT_ATTRIBUTES)
def test_I06_kafka_cdc_preserves_mysql_datetime_and_union_updates(purchase_kafka, product):
    client, databases, topics, producer = purchase_kafka
    core = databases["core"]
    order, financial = make_order(product), make_financial(product)

    publish(producer, topics[f"{product}_orders"], [debezium_row(order, order=True)])
    publish(producer, topics["financial_order"], [debezium_row(financial)])

    def received():
        rows = client.query(
            f"SELECT toString(created_at), _version, _topic, _offset "
            f"FROM {core}.{product}_orders FINAL"
        ).result_rows
        assert rows == [(order["created_at"], order["_version"], topics[f"{product}_orders"], 0)]
        assert client.query(
            f"SELECT toString(created_at), paid_amount FROM {core}.financial_order FINAL"
        ).result_rows == [(financial["created_at"], financial["paid_amount"])]

    eventually(received, timeout=60, interval=0.5)
    reload_orders(client, core)
    insert_events(client, core, [make_event(event_type="purchase")])
    assert len(active_purchases(client, databases["mart"])) == 1
    publish(
        producer,
        topics[f"{product}_orders"],
        [debezium_row(make_order(product, policy_no="SYNTHETIC-UPDATED", _version=20), order=True)],
    )

    def updated():
        assert client.query(
            f"SELECT policy_no FROM {core}.orders_unified FINAL WHERE _is_deleted = 0"
        ).result_rows == [("SYNTHETIC-UPDATED",)]

    eventually(updated, timeout=60, interval=0.5)
    publish(
        producer,
        topics[f"{product}_orders"],
        [debezium_row(make_order(product, _version=30, _is_deleted=1), order=True)],
    )

    def deleted():
        assert client.query(
            f"SELECT count() FROM {core}.orders_unified FINAL WHERE _is_deleted = 0"
        ).result_rows == [(0,)]

    eventually(deleted, timeout=60, interval=0.5)
