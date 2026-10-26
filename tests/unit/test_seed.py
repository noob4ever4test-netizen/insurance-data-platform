import csv
import importlib
import json
import stat
from pathlib import Path
from unittest.mock import Mock

import pytest

from azki_pipeline.seed import main, prepare_environment, wait_users_snapshot
from tests.helpers.factories import make_user

MODULE = importlib.import_module("azki_pipeline.seed")
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


def prepare_root(tmp_path):
    fixtures = tmp_path / "tests" / "fixtures"
    fixtures.mkdir(parents=True)
    for name in ("users.csv", "user_events.csv"):
        (fixtures / name).write_bytes((FIXTURES / name).read_bytes())
    return tmp_path


@pytest.mark.unit
def test_U30_generated_secrets_are_private_and_stable(tmp_path):
    root = prepare_root(tmp_path)

    prepare_environment(root)
    secret_dir = root / "docker" / "secrets"
    initial = {path.name: path.read_bytes() for path in secret_dir.iterdir()}
    prepare_environment(root)

    assert initial == {path.name: path.read_bytes() for path in secret_dir.iterdir()}
    assert initial["mysql_root_password.txt"] != initial["mysql_cdc_password.txt"]
    assert initial["mysql.properties"] == b"password=" + initial["mysql_cdc_password.txt"]
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in secret_dir.iterdir())


@pytest.mark.unit
def test_U30_synthetic_purchase_orders_match_events_and_cover_products(tmp_path):
    root = prepare_root(tmp_path)
    with (FIXTURES / "user_events.csv").open(newline="") as stream:
        purchases = [row for row in csv.DictReader(stream) if row["event_type"] == "purchase"]

    prepare_environment(root)

    generated = root / "docker" / "generated"
    actual = []
    for product in ("third", "body", "medical", "fire"):
        with (generated / f"{product}_orders.csv").open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        assert rows
        assert all(row["policy_no"].startswith("SYNTHETIC-") for row in rows)
        assert all(len(row["national_id"]) == 10 for row in rows)
        actual.extend(rows)
    assert {(row["user_id"], row["session_id"], row["created_at"]) for row in actual} == {
        (row["user_id"], row["session_id"], row["event_time"]) for row in purchases
    }
    with (generated / "financial_order.csv").open(newline="") as stream:
        financial = list(csv.DictReader(stream))
    assert len(financial) == len(actual)
    assert {row["order_id"] for row in financial} == {row["order_id"] for row in actual}


@pytest.mark.unit
def test_U30_snapshot_waits_for_all_synthetic_users(tmp_path, monkeypatch):
    root = prepare_root(tmp_path)
    monkeypatch.setattr(MODULE, "ROOT", root)
    messages = []
    for user_id in range(1, 10):
        message = Mock()
        message.error.return_value = None
        message.value.return_value = json.dumps(make_user(user_id=user_id)).encode()
        messages.append(message)
    consumer = Mock()
    consumer.poll.side_effect = [None, *messages]
    monkeypatch.setattr(MODULE, "Consumer", Mock(return_value=consumer))

    wait_users_snapshot("localhost:9092")

    assert consumer.poll.call_count == 10
    assert MODULE.Consumer.call_args.args[0]["topic.metadata.refresh.interval.ms"] == 1000
    consumer.subscribe.assert_called_once_with(["mysql.azki.users"])
    consumer.close.assert_called_once()


@pytest.mark.unit
def test_U30_snapshot_timeout_closes_consumer(tmp_path, monkeypatch):
    monkeypatch.setattr(MODULE, "ROOT", prepare_root(tmp_path))
    consumer = Mock()
    monkeypatch.setattr(MODULE, "Consumer", Mock(return_value=consumer))

    with pytest.raises(TimeoutError, match="Users snapshot incomplete"):
        wait_users_snapshot("localhost:9092", timeout=0)

    consumer.close.assert_called_once()


@pytest.mark.unit
def test_U30_seed_observes_snapshot_before_staging_events(monkeypatch, capsys):
    events = []
    client = Mock()
    client.register.side_effect = lambda document: events.append(document["name"])
    monkeypatch.setattr(MODULE, "prepare_environment", lambda: None)
    monkeypatch.setattr(MODULE, "ConnectClient", lambda url: client)
    monkeypatch.setattr(MODULE, "wait_users_snapshot", lambda servers: events.append("snapshot"))
    monkeypatch.setattr(MODULE.subprocess, "run", lambda command, **kwargs: events.append(command))

    status = main([])

    assert status == 0
    assert events[:3] == ["azki-mysql-source", "snapshot", "azki-user-events-source"]
    assert events[3][4] == "cp"
    assert events[3][-1].endswith(".csv.tmp")
    assert "mv" in events[4]
    assert events[4][-1].endswith(".csv")
    assert "Users snapshot observed" in capsys.readouterr().out
