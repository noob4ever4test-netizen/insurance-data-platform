from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.contract
def test_C09_services_have_pinned_images_and_healthchecks():
    compose = yaml.safe_load((ROOT / "docker" / "compose.yml").read_text())

    assert isinstance(compose, dict)
    services = compose["services"]
    assert {
        "kafka",
        "schema-registry",
        "connect",
        "kafka-ui",
        "mysql",
        "clickhouse",
    } <= services.keys()
    for name, service in services.items():
        image = service["image"]
        assert ":" in image.rsplit("/", 1)[-1], name
        assert not image.endswith(":latest"), name
        assert service["healthcheck"]["test"], name
        assert not service["healthcheck"].get("disable", False), name
    assert services["kafka-2"]["profiles"] == ["three-brokers"]
    assert services["kafka-3"]["profiles"] == ["three-brokers"]
    for name, port in (("kafka", 9092), ("kafka-2", 9094), ("kafka-3", 9096)):
        assert (
            f"EXTERNAL://127.0.0.1:{port}"
            in services[name]["environment"]["KAFKA_ADVERTISED_LISTENERS"]
        )


@pytest.mark.contract
def test_C09_mysql_cdc_settings_and_synthetic_mounts():
    compose = yaml.safe_load((ROOT / "docker" / "compose.yml").read_text())

    assert isinstance(compose, dict)
    mysql = compose["services"]["mysql"]
    settings = " ".join(mysql["command"])
    for setting in ("--binlog-format=ROW", "--binlog-row-image=FULL", "--gtid-mode=ON"):
        assert setting in settings
    assert "--enforce-gtid-consistency=ON" in settings
    assert any("tests/fixtures/users.csv" in mount for mount in mysql["volumes"])
    assert not any("../data" in mount for mount in mysql["volumes"])
    assert mysql["environment"]["MYSQL_PASSWORD_FILE"].startswith("/run/secrets/")
    assert any(
        "001_schema.sql:/docker-entrypoint-initdb.d/001_schema.sql:ro" in mount
        for mount in mysql["volumes"]
    )
    assert any(
        "010_load.sql:/docker-entrypoint-initdb.d/010_load.sql:ro" in mount
        for mount in mysql["volumes"]
    )
