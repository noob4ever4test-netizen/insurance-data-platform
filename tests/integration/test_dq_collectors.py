import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import clickhouse_connect
import pytest

from azki_pipeline.dq.runner import collect_metrics, exit_code, run_checks
from azki_pipeline.sql_runner import load_sql
from tests.helpers.factories import make_event, make_user
from tests.helpers.pipeline import insert_events, insert_users

pytestmark = pytest.mark.integration


def test_I14_collectors_return_real_metrics_and_runner_persists_results(ch_db):
    client, databases = ch_db
    assert client.query(f"EXISTS TABLE {databases['dq']}.check_results").first_row[0] == 1, (
        "Monitoring SQL is missing"
    )
    now = datetime.now(UTC)
    event_time = now - timedelta(minutes=5)
    text = event_time.strftime("%Y-%m-%d %H:%M:%S")
    event = make_event(event_time=text, event_type="purchase")
    other = make_event(event_time=text, session_id="other")
    orphan = make_event(event_time=text, user_id=999, session_id="orphan")
    insert_users(client, databases["core"], [make_user()])
    client.command(f"SYSTEM STOP MERGES {databases['core']}.user_events")
    insert_events(client, databases["core"], [event, other, orphan])
    insert_events(client, databases["core"], [event])
    topic = "synthetic.events"
    captured = now - timedelta(seconds=1)
    client.insert(
        f"{databases['dq']}.event_samples",
        [
            [topic, 0, offset, json.dumps(record), captured]
            for offset, record in zip(
                (10, 11, 13), (event, other, {**orphan, "utm_campaign": "test"}), strict=True
            )
        ],
        column_names=["topic", "partition", "offset", "raw_message", "captured_at"],
    )
    client.insert(
        f"{databases['dq']}.cdc_heartbeats",
        [["mysql", int(now.timestamp() * 1000) - 5000, captured]],
        column_names=["source", "ts_ms", "captured_at"],
    )
    client.insert(
        f"{databases['dq']}.kafka_dead_letter",
        [[topic, 0, 14, "bad-json", "synthetic error", captured]],
        column_names=["topic", "partition", "offset", "raw_message", "error", "ingested_at"],
    )
    client.command("SYSTEM FLUSH LOGS")
    expected = {event_time.strftime("%Y-%m-%dT%H:00:00Z"): 3}

    values = collect_metrics(
        client, now=now, database_overrides=databases, event_topic=topic, source_counts=expected
    )

    assert set(values) == {
        "freshness",
        "lag",
        "cdc_lag",
        "offset_gaps",
        "source_reconciliation",
        "volume",
        "orphans",
        "schema_drift",
        "dead_letters",
        "insert_health",
        "duplicates",
        "purchase_enrichment",
    }
    errors = {
        name: metrics["collection_error"]
        for name, metrics in values.items()
        if "collection_error" in metrics
    }
    assert not errors
    assert values["freshness"]["last_event_at"] == event_time.replace(microsecond=0)
    assert abs((now - values["cdc_lag"]["last_heartbeat_at"]).total_seconds() - 5) < 0.01
    assert values["offset_gaps"]["partitions"] == [
        {"topic": topic, "partition": 0, "min_offset": 10, "max_offset": 13, "offset_count": 3},
    ]
    assert values["source_reconciliation"]["sink_counts"] == expected
    assert values["orphans"] == {"events": 3, "orphans": 1, "users_loaded": 1}
    assert values["duplicates"] == {"rows": 4, "unique_keys": 3}
    assert len(values["schema_drift"]["samples"]) == 3
    assert values["dead_letters"] == {"errors": 1, "received": 3}
    assert values["purchase_enrichment"] == {"purchases": 1, "unmatched": 1}
    assert values["insert_health"]["max_parts"] >= 1
    assert values["insert_health"]["part_log_available"] is True
    assert values["insert_health"]["insert_errors"] == 0
    assert isinstance(values["lag"]["consumers"], list)
    assert isinstance(values["volume"]["history"], list)

    results = run_checks(
        client, now=now, database_overrides=databases, event_topic=topic, source_counts=expected
    )

    assert len(results) == 12
    assert exit_code(results) == 1
    statuses = {result.name: result.status for result in results}
    assert statuses["schema_drift"] == "WARN"
    assert statuses["source_reconciliation"] == "PASS"
    assert statuses["offset_gaps"] == "FAIL"
    assert client.query(f"SELECT count() FROM {databases['dq']}.check_results").first_row[0] == 12


def test_I14_source_reconciliation_uses_the_manifest_hours(ch_db):
    client, databases = ch_db
    now = datetime.now(UTC)
    historical = now - timedelta(days=3)
    expected = {historical.strftime("%Y-%m-%dT%H:00:00Z"): 1}
    insert_events(
        client,
        databases["core"],
        [
            make_event(
                event_time=historical.strftime("%Y-%m-%d %H:%M:%S"), session_id="historical"
            ),
            make_event(event_time=now.strftime("%Y-%m-%d %H:%M:%S"), session_id="recent"),
        ],
    )

    values = collect_metrics(client, now=now, database_overrides=databases, source_counts=expected)

    assert values["source_reconciliation"] == {"source_counts": expected, "sink_counts": expected}


def test_I14_engineer_role_can_collect_and_persist_without_access_management(ch_db):
    client, databases = ch_db
    fixtures = Path(__file__).resolve().parents[2] / "sql/test_fixtures/governance"
    insert_events(client, databases["core"], [make_event()])
    client.command("SYSTEM FLUSH LOGS")
    engineer = None
    try:
        for query in load_sql(fixtures / "070_governance_users.sql", database_overrides=databases):
            client.command(query)
        endpoint = urlparse(client.url)
        engineer = clickhouse_connect.get_client(
            host=endpoint.hostname,
            port=endpoint.port,
            username=f"azki_governance_test_engineer_{databases['gov']}",
        )

        results = run_checks(engineer, database_overrides=databases)

        errors = {
            result.name: result.details
            for result in results
            if "collection_error" in result.details
        }
        assert not errors
        assert len(results) == 12
    finally:
        if engineer is not None:
            engineer.close()
        for query in load_sql(
            fixtures / "071_governance_cleanup.sql", database_overrides=databases
        ):
            client.command(query)
