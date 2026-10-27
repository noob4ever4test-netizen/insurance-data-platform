import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def docker_available():
    import docker

    try:
        client = docker.from_env()
        client.ping()
        client.close()
    except docker.errors.DockerException as error:
        pytest.skip(f"Docker unavailable: {error}")


@pytest.fixture(scope="session")
def container_network(docker_available):
    from testcontainers.core.network import Network

    with Network() as network:
        yield network


@pytest.fixture(scope="session")
def clickhouse(container_network):
    import clickhouse_connect
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import HttpWaitStrategy

    container = (
        DockerContainer("clickhouse/clickhouse-server:25.3.6.56")
        .with_network(container_network)
        .with_env("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_exposed_ports(8123)
        .waiting_for(HttpWaitStrategy(8123, "/ping").for_status_code(200))
    )
    with container:
        client = clickhouse_connect.get_client(
            host="127.0.0.1",
            port=container.get_exposed_port(8123),
            connect_timeout=5,
            send_receive_timeout=30,
        )
        yield client
        client.close()


@pytest.fixture(scope="session")
def kafka(container_network):
    from testcontainers.community.kafka import KafkaContainer

    container = (
        KafkaContainer("confluentinc/cp-kafka:7.9.2")
        .with_kraft()
        .with_network(container_network)
        .with_network_aliases("kafka")
    )
    with container:
        yield container


@pytest.fixture
def ch_db(clickhouse):
    from azki_pipeline.sql_runner import apply_sql, load_sql

    suffix = uuid4().hex
    databases = {name: f"test_{name}_{suffix}" for name in ("raw", "core", "mart", "dq", "gov")}

    class IsolatedClient:
        def command(self, query):
            originals = [
                "azki.user_events",
                "azki-events",
                "azki-users",
                "azki-dq-samples",
                "azki-dq-heartbeats",
                "__debezium-heartbeat.mysql",
            ]
            originals.extend(
                f"mysql.azki.{table}"
                for table in (
                    "users",
                    "third_orders",
                    "body_orders",
                    "medical_orders",
                    "fire_orders",
                    "financial_order",
                )
            )
            originals.extend(
                f"azki-{name}"
                for name in (
                    "third-orders",
                    "body-orders",
                    "medical-orders",
                    "fire-orders",
                    "financial-order",
                )
            )
            for original in originals:
                query = query.replace(f"'{original}'", f"'{original}_{suffix}'")
            return clickhouse.command(query.replace("'kafka:29092'", "'kafka:9092'"))

    try:
        apply_sql(IsolatedClient(), database_overrides=databases)
        yield clickhouse, databases
    finally:
        for name in ("raw", "mart", "dq", "core", "gov"):
            clickhouse.command(f"DROP DATABASE IF EXISTS {databases[name]} SYNC")
        for query in load_sql(
            ROOT / "sql/test_fixtures/governance/072_governance_roles_cleanup.sql",
            database_overrides=databases,
        ):
            clickhouse.command(query)


@pytest.fixture(scope="session")
def compose_stack():
    from azki_pipeline.seed import prepare_environment

    prepare_environment(ROOT)
    command = [
        "docker",
        "compose",
        "--progress",
        "quiet",
        "-f",
        str(ROOT / "docker" / "compose.yml"),
    ]
    subprocess.run(
        [*command, "up", "-d", "--build", "--wait", "--wait-timeout", "240"],
        check=True,
        timeout=900,
        cwd=ROOT,
    )
    return command


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.failed and item.get_closest_marker("e2e"):
        logs = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(ROOT / "docker" / "compose.yml"),
                "logs",
                "--tail",
                "100",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        report.sections.append(("Compose logs", logs.stdout + logs.stderr))
