from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from azki_pipeline.contracts import event_key
from azki_pipeline.sql_runner import load_sql
from tests.helpers.factories import (
    PRODUCT_ATTRIBUTES,
    make_event,
    make_financial,
    make_order,
    make_user,
)
from tests.helpers.pipeline import insert_events, insert_users
from tests.helpers.purchases import (
    active_purchases,
    insert_financial,
    insert_order,
    refresh_purchases,
    reload_orders,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def purchase_db(ch_db):
    client, databases = ch_db
    exists = client.query(f"EXISTS TABLE {databases['mart']}.purchase_events_repair_mv").result_rows
    if exists == [(1,)]:
        client.command(f"SYSTEM WAIT VIEW {databases['mart']}.purchase_events_repair_mv")
        client.command(f"SYSTEM STOP VIEW {databases['mart']}.purchase_events_repair_mv")
    return client, databases


def test_I06_four_product_views_preserve_common_and_specific_fields(purchase_db):
    client, databases = purchase_db
    orders = [make_order(product) for product in PRODUCT_ATTRIBUTES]

    for order in orders:
        insert_order(client, databases["core"], order)

    rows = client.query(
        f"SELECT product_type, order_id, user_id, session_id, partner_id, "
        "national_id, phone, policy_no, premium_amount, product_attributes "
        f"FROM {databases['core']}.orders_unified FINAL "
        "WHERE _is_deleted = 0 ORDER BY product_type"
    ).result_rows
    expected = []
    for order in sorted(orders, key=lambda row: row["product_type"]):
        attribute, value = PRODUCT_ATTRIBUTES[order["product_type"]]
        expected.append(
            (
                order["product_type"],
                order["order_id"],
                order["user_id"],
                order["session_id"],
                order["partner_id"],
                order["national_id"],
                order["phone"],
                order["policy_no"],
                order["premium_amount"],
                {attribute: str(value)},
            )
        )
    assert rows == expected


@pytest.mark.parametrize("product", PRODUCT_ATTRIBUTES)
def test_I06_order_updates_and_tombstones_propagate_to_union(purchase_db, product):
    client, databases = purchase_db
    core = databases["core"]
    insert_order(client, core, make_order(product))

    insert_order(client, core, make_order(product, policy_no="SYNTHETIC-UPDATED", _version=20))
    insert_order(client, core, make_order(product, policy_no="stale", _version=2))

    assert client.query(
        f"SELECT policy_no FROM {core}.orders_unified FINAL WHERE _is_deleted = 0"
    ).result_rows == [("SYNTHETIC-UPDATED",)]
    insert_order(client, core, make_order(product, _version=30, _is_deleted=1))
    assert client.query(
        f"SELECT count() FROM {core}.orders_unified FINAL WHERE _is_deleted = 0"
    ).result_rows == [(0,)]


@pytest.mark.parametrize("product", PRODUCT_ATTRIBUTES)
def test_I07_purchase_enriches_event_order_financial_and_user_fields(purchase_db, product):
    client, databases = purchase_db
    core, mart = databases["core"], databases["mart"]
    event, order, financial = (
        make_event(event_type="purchase"),
        make_order(product),
        make_financial(product),
    )
    insert_users(client, core, [make_user()])
    insert_order(client, core, order)
    insert_financial(client, core, financial)
    reload_orders(client, core)

    insert_events(client, core, [event, event])

    rows = client.query(
        f"SELECT user_id, session_id, event_type, channel, premium_amount, "
        "city, device_type, order_id, product_type, partner_id, national_id, "
        "phone, policy_no, order_premium_amount, product_attributes, "
        "payment_status, paid_amount, event_time = order_created_at, "
        "order_created_at = financial_created_at "
        f"FROM {mart}.purchase_events_enriched FINAL WHERE _is_deleted = 0"
    ).result_rows
    attribute, value = PRODUCT_ATTRIBUTES[product]
    assert rows == [
        (
            event["user_id"],
            event["session_id"],
            event["event_type"],
            event["channel"],
            event["premium_amount"],
            "Shiraz",
            "Desktop",
            order["order_id"],
            product,
            order["partner_id"],
            order["national_id"],
            order["phone"],
            order["policy_no"],
            order["premium_amount"],
            {attribute: str(value)},
            financial["payment_status"],
            financial["paid_amount"],
            1,
            1,
        )
    ]


@pytest.mark.parametrize(
    "offset,matched", [(-3601, False), (-3600, True), (0, True), (3600, True), (3601, False)]
)
def test_I07_order_timestamp_must_be_within_one_hour(purchase_db, offset, matched):
    client, databases = purchase_db
    event = make_event(event_type="purchase")
    timestamp = datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S") + timedelta(
        seconds=offset
    )
    insert_order(
        client, databases["core"], make_order(created_at=timestamp.strftime("%Y-%m-%d %H:%M:%S"))
    )
    insert_financial(client, databases["core"], make_financial())
    reload_orders(client, databases["core"])

    insert_events(client, databases["core"], [event])

    assert len(active_purchases(client, databases["mart"])) == int(matched)


@pytest.mark.parametrize(
    "override",
    [{"order_id": 2}, {"user_id": 999}, {"session_id": "other"}, {"product_type": "fire"}],
)
def test_I07_financial_match_requires_order_product_user_and_session(purchase_db, override):
    client, databases = purchase_db
    insert_order(client, databases["core"], make_order())
    insert_financial(client, databases["core"], make_financial(**override))
    reload_orders(client, databases["core"])

    insert_events(client, databases["core"], [make_event(event_type="purchase")])

    assert active_purchases(client, databases["mart"]) == []


@pytest.mark.parametrize("event_type", ["policy_click", "quote_view"])
def test_I08_non_purchase_events_never_enter_enrichment(purchase_db, event_type):
    client, databases = purchase_db
    insert_order(client, databases["core"], make_order())
    insert_financial(client, databases["core"], make_financial())
    reload_orders(client, databases["core"])

    insert_events(client, databases["core"], [make_event(event_type=event_type)])

    assert client.query(
        f"SELECT count() FROM {databases['mart']}.purchase_events_enriched"
    ).result_rows == [(0,)]


@pytest.mark.parametrize("late_source", ["order", "financial"])
def test_I09_late_dimension_arrival_requires_refresh_and_then_enriches(purchase_db, late_source):
    client, databases = purchase_db
    core, mart = databases["core"], databases["mart"]
    timestamp = datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    event = make_event(event_type="purchase", event_time=timestamp)
    order, financial = make_order(created_at=timestamp), make_financial(created_at=timestamp)
    if late_source == "order":
        insert_financial(client, core, financial)
    else:
        insert_order(client, core, order)
    reload_orders(client, core)
    insert_events(client, core, [event])
    assert active_purchases(client, mart) == []

    if late_source == "order":
        insert_order(client, core, order)
    else:
        insert_financial(client, core, financial)
    reload_orders(client, core)

    assert active_purchases(client, mart) == []
    refresh_purchases(client, databases)
    expected = [
        (
            event_key(event["user_id"], event["session_id"], timestamp, "purchase"),
            order["order_id"],
            order["product_type"],
            financial["paid_amount"],
            order["policy_no"],
        )
    ]
    assert active_purchases(client, mart) == expected
    refresh_purchases(client, databases)
    assert active_purchases(client, mart) == expected


@pytest.mark.parametrize("source", ["order", "financial"])
def test_I09_refresh_updates_and_retracts_deleted_dimensions(purchase_db, source):
    client, databases = purchase_db
    core, mart = databases["core"], databases["mart"]
    timestamp = datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    insert_order(client, core, make_order(created_at=timestamp))
    insert_financial(client, core, make_financial(created_at=timestamp))
    reload_orders(client, core)
    insert_events(client, core, [make_event(event_type="purchase", event_time=timestamp)])
    assert len(active_purchases(client, mart)) == 1

    if source == "order":
        insert_order(
            client,
            core,
            make_order(created_at=timestamp, policy_no="SYNTHETIC-UPDATED", _version=20),
        )
    else:
        insert_financial(
            client, core, make_financial(created_at=timestamp, paid_amount=99, _version=20)
        )
    refresh_purchases(client, databases)

    rows = active_purchases(client, mart)
    assert rows[0][4] == ("SYNTHETIC-UPDATED" if source == "order" else "SYNTHETIC-1")
    assert rows[0][3] == (99 if source == "financial" else 1200)
    if source == "order":
        insert_order(client, core, make_order(created_at=timestamp, _version=30, _is_deleted=1))
    else:
        insert_financial(
            client, core, make_financial(created_at=timestamp, _version=30, _is_deleted=1)
        )
    refresh_purchases(client, databases)
    assert active_purchases(client, mart) == []


def test_I07_ambiguous_session_is_not_arbitrarily_attributed(purchase_db):
    client, databases = purchase_db
    for product in ("third", "fire"):
        insert_order(client, databases["core"], make_order(product))
        insert_financial(client, databases["core"], make_financial(product))
    reload_orders(client, databases["core"])

    insert_events(client, databases["core"], [make_event(event_type="purchase")])

    assert active_purchases(client, databases["mart"]) == []


def test_I09_recent_repair_preserves_history_and_explicit_month_repairs_old_data(purchase_db):
    client, databases = purchase_db
    core, mart = databases["core"], databases["mart"]
    now = datetime.now(UTC).replace(microsecond=0)
    old = now - timedelta(days=365)
    events = []
    for order_id, timestamp in enumerate((old, now), start=1):
        text = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        session = f"repair_{order_id}"
        insert_order(
            client, core, make_order(order_id=order_id, session_id=session, created_at=text)
        )
        insert_financial(
            client, core, make_financial(order_id=order_id, session_id=session, created_at=text)
        )
        events.append(make_event(event_type="purchase", session_id=session, event_time=text))
    reload_orders(client, core)
    insert_events(client, core, events)
    assert len(active_purchases(client, mart)) == 2

    plan = client.query(
        f"EXPLAIN indexes = 1 SELECT event_key FROM {core}.user_events FINAL "
        "WHERE event_type = 'purchase' "
        "AND event_time >= toStartOfMonth(now('UTC') - INTERVAL 2 MONTH)"
    ).result_rows
    explanation = "\n".join(row[0] for row in plan)
    assert "Parts: 1/2" in explanation
    print(explanation.replace(core, "core"))

    for order_id, timestamp in enumerate((old, now), start=1):
        insert_order(
            client,
            core,
            make_order(
                order_id=order_id,
                session_id=f"repair_{order_id}",
                created_at=timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                _version=10,
                _is_deleted=1,
            ),
        )
    refresh_purchases(client, databases)
    remaining = active_purchases(client, mart)
    assert len(remaining) == 1 and remaining[0][1] == 1
    job = Path(__file__).resolve().parents[2] / "sql" / "jobs" / "010_repair_purchase_partition.sql"
    assert job.is_file(), "Historical purchase repair SQL is missing"

    for query in load_sql(job, database_overrides=databases):
        client.command(query, parameters={"partition": int(old.strftime("%Y%m"))})

    assert active_purchases(client, mart) == []
