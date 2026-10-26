import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONFIGS = ["debezium-mysql-source.json", "spooldir-user-events.json"]


@pytest.mark.contract
@pytest.mark.parametrize("filename", CONFIGS)
def test_C02_connector_envelope_and_json_converters(filename):
    connector = json.loads((ROOT / "connect" / filename).read_text())

    assert {"name", "config"} <= connector.keys()
    config = connector["config"]
    assert {"connector.class", "tasks.max", "key.converter", "value.converter"} <= config.keys()
    for kind in ("key", "value"):
        assert config[f"{kind}.converter"] == "org.apache.kafka.connect.json.JsonConverter"
        assert config[f"{kind}.converter.schemas.enable"] == "false"


@pytest.mark.contract
def test_C02_debezium_snapshot_cdc_and_flat_delete_metadata():
    connector = json.loads((ROOT / "connect" / CONFIGS[0]).read_text())

    assert "config" in connector
    config = connector["config"]
    assert config["connector.class"] == "io.debezium.connector.mysql.MySqlConnector"
    assert config["topic.prefix"] == "mysql"
    assert config["snapshot.mode"] == "initial"
    assert int(config["heartbeat.interval.ms"]) > 0
    assert set(config["table.include.list"].split(",")) == {
        f"azki.{table}"
        for table in (
            "users",
            "third_orders",
            "body_orders",
            "medical_orders",
            "fire_orders",
            "financial_order",
        )
    }
    assert config["transforms.unwrap.delete.handling.mode"] == "rewrite"
    assert "ts_ms" in config["transforms.unwrap.add.fields"].split(",")


@pytest.mark.contract
def test_C02_spooldir_explicit_types_key_and_error_routing():
    connector = json.loads((ROOT / "connect" / CONFIGS[1]).read_text())

    assert "config" in connector
    config = connector["config"]
    assert config["topic"] == "azki.user_events"
    assert config["schema.generation.enabled"] == "false"
    value_schema = json.loads(config["value.schema"])
    assert {name: field["type"] for name, field in value_schema["fieldSchemas"].items()} == {
        "event_time": "STRING",
        "user_id": "INT64",
        "session_id": "STRING",
        "event_type": "STRING",
        "channel": "STRING",
        "premium_amount": "INT64",
    }
    assert set(json.loads(config["key.schema"])["fieldSchemas"]) == {"user_id"}
    assert config["transforms.extractKey.field"] == "user_id"
    assert config["errors.tolerance"] == "all"
    assert config["errors.deadletterqueue.topic.name"].startswith("dlq.")
    assert config["halt.on.error"] == "false"
    assert len({config[key] for key in ("input.path", "finished.path", "error.path")}) == 3


@pytest.mark.contract
def test_C03_secrets_are_file_provider_references():
    connectors = [json.loads((ROOT / "connect" / filename).read_text()) for filename in CONFIGS]

    assert all("config" in connector for connector in connectors)
    passwords = []
    for connector in connectors:
        for key, value in connector["config"].items():
            if any(term in key.lower() for term in ("password", "secret", "token")):
                passwords.append(value)
                assert re.fullmatch(r"\$\{file:/opt/secrets/[^:}]+:[^}]+\}", value)
    assert passwords


@pytest.mark.contract
def test_C04_connect_topics_match_clickhouse_kafka_tables():
    sql = "\n".join(path.read_text() for path in (ROOT / "sql").glob("*.sql"))
    topics = set()
    for value in re.findall(r"kafka_topic_list\s*=\s*'([^']+)'", sql):
        topics.update(topic.strip() for topic in value.split(","))

    mysql = json.loads((ROOT / "connect" / CONFIGS[0]).read_text())["config"]
    spool = json.loads((ROOT / "connect" / CONFIGS[1]).read_text())["config"]
    assert {spool["topic"], f"{mysql['topic.prefix']}.azki.users"} <= topics
