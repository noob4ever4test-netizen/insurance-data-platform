from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import clickhouse_connect
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError

from azki_pipeline.sql_runner import load_sql
from tests.helpers.factories import make_event, make_financial, make_order
from tests.helpers.pipeline import insert_events
from tests.helpers.purchases import insert_financial, insert_order, refresh_purchases, reload_orders

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def test_I10_roles_mask_pii_isolate_partners_enforce_limits_and_audit(ch_db):
    client, databases = ch_db
    assert (
        client.query(
            "SELECT count() FROM system.roles WHERE name = {name:String}",
            parameters={"name": f"role_analyst_{databases['gov']}"},
        ).first_row[0]
        == 1
    ), "Phase 5 analyst role is missing"
    for query in load_sql(ROOT / "sql/070_governance.sql", database_overrides=databases):
        client.command(query)
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    for index in (1, 2):
        product = "third" if index == 1 else "body"
        session = f"governance_{index}"
        insert_order(
            client,
            databases["core"],
            make_order(
                product,
                order_id=index,
                session_id=session,
                partner_id=f"partner_{index}",
                created_at=timestamp,
            ),
        )
        insert_financial(
            client,
            databases["core"],
            make_financial(
                product,
                order_id=index,
                session_id=session,
                created_at=timestamp,
            ),
        )
    reload_orders(client, databases["core"])
    insert_events(
        client,
        databases["core"],
        [
            make_event(
                event_type="purchase", session_id=f"governance_{index}", event_time=timestamp
            )
            for index in (1, 2)
        ],
    )
    connections = {}
    endpoint = urlparse(client.url)
    try:
        for query in load_sql(
            ROOT / "sql/test_fixtures/governance/070_governance_users.sql",
            database_overrides=databases,
        ):
            client.command(query)
        for role in ("admin", "engineer", "analyst", "partner_1", "partner_2"):
            connections[role] = clickhouse_connect.get_client(
                host=endpoint.hostname,
                port=endpoint.port,
                username=f"azki_governance_test_{role}_{databases['gov']}",
            )
        analyst = connections["analyst"]
        mart = databases["mart"]
        masked = analyst.query(
            f"SELECT national_id, phone, policy_no, product_attributes['plate_no'] "
            f"FROM {databases['gov']}.purchase_events_masked ORDER BY order_id"
        ).result_rows
        assert masked == [("***", "***", "***", "***")] * 2
        assert analyst.query(
            "SELECT product_attributes['car_value'] "
            f"FROM {databases['gov']}.purchase_events_masked "
            "WHERE product_type = 'body'"
        ).result_rows == [("100000000",)]
        for role in ("admin", "engineer"):
            assert (
                connections[role]
                .query(f"SELECT national_id FROM {mart}.purchase_events_enriched FINAL")
                .result_rows
                == [("0000000001",)] * 2
            )
        for index in (1, 2):
            partner = connections[f"partner_{index}"]
            assert partner.query(
                f"SELECT partner_id FROM {mart}.purchase_events_enriched FINAL"
            ).result_rows == [(f"partner_{index}",)]
            assert (
                partner.query(
                    f"SELECT count() FROM {mart}.purchase_events_enriched FINAL "
                    f"WHERE partner_id != 'partner_{index}'"
                ).first_row[0]
                == 0
            )
        insert_order(
            client,
            databases["core"],
            make_order(
                session_id="governance_1",
                partner_id="partner_2",
                created_at=timestamp,
                _version=2,
            ),
        )
        refresh_purchases(client, databases)
        assert (
            connections["partner_1"]
            .query(f"SELECT partner_id FROM {mart}.purchase_events_enriched FINAL")
            .result_rows
            == []
        )
        assert (
            connections["partner_2"]
            .query(f"SELECT partner_id FROM {mart}.purchase_events_enriched FINAL")
            .result_rows
            == [("partner_2",)] * 2
        )
        client.command("SYSTEM FLUSH LOGS")
        for role in ("analyst", "partner_1", "partner_2"):
            restricted = connections[role]
            for query in (
                "SELECT national_id, phone, policy_no, product_attributes "
                f"FROM {mart}.purchase_events_enriched",
                f"SELECT national_id FROM {databases['core']}.orders_unified",
                f"SELECT dictGet('{databases['core']}.purchase_orders_dict', 'national_id', "
                "tuple(toInt64(3), 'governance_1'))",
                f"SELECT national_id FROM {databases['core']}.purchase_order_lookup",
                "SELECT query FROM system.query_log",
            ):
                with pytest.raises(
                    DatabaseError, match="(?i)(not enough privileges|ACCESS_DENIED)"
                ):
                    restricted.query(query)
        settings = dict(
            analyst.query(
                "SELECT name, value FROM system.settings "
                "WHERE name IN ('max_execution_time', 'max_result_rows', 'log_queries')"
            ).result_rows
        )
        assert settings == {
            "max_execution_time": "10",
            "max_result_rows": "10000",
            "log_queries": "1",
        }
        with pytest.raises(DatabaseError, match="(?i)(constraint|readonly|read.only)"):
            analyst.query("SELECT 1 SETTINGS max_execution_time = 100")
        for setting in ("max_execution_time", "max_result_rows", "max_memory_usage", "log_queries"):
            with pytest.raises(DatabaseError, match="(?i)(constraint|readonly|read.only)"):
                analyst.query(f"SELECT 1 SETTINGS {setting} = 0")
        for assignment in (
            "log_queries_min_query_duration_ms = 1000000",
            "log_queries_min_type = 'EXCEPTION_BEFORE_START'",
        ):
            with pytest.raises(DatabaseError, match="(?i)(constraint|readonly|read.only)"):
                analyst.query(f"SELECT 1 SETTINGS {assignment}")
        with pytest.raises(DatabaseError, match="(?i)(limit|TOO_MANY_ROWS)"):
            analyst.query("SELECT number FROM numbers(10001)")
        assert (
            client.query(
                "SELECT count() FROM system.quotas WHERE name = {name:String}",
                parameters={"name": f"azki_governance_analyst_quota_{databases['gov']}"},
            ).first_row[0]
            == 1
        )
        audit_id = f"governance_audit_{databases['gov']}"
        analyst.query("SELECT 42", settings={"log_comment": audit_id})
        client.command("SYSTEM FLUSH LOGS")
        assert (
            client.query(
                "SELECT count() FROM system.query_log WHERE log_comment = {tag:String} "
                "AND type = 'QueryFinish' AND initial_user = {user:String}",
                parameters={
                    "tag": audit_id,
                    "user": f"azki_governance_test_analyst_{databases['gov']}",
                },
            ).first_row[0]
            == 1
        )
        with pytest.raises(DatabaseError, match="QUOTA_EXCEEDED"):
            for _attempt in range(1001):
                analyst.query(f"SELECT order_id FROM {databases['gov']}.purchase_events_masked")
        assert connections["admin"].query("SELECT 1").first_row[0] == 1
    finally:
        for connection in connections.values():
            connection.close()
        for query in load_sql(
            ROOT / "sql/test_fixtures/governance/071_governance_cleanup.sql",
            database_overrides=databases,
        ):
            client.command(query)
