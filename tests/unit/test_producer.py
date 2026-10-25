import csv
import json
from pathlib import Path

import pytest

from azki_pipeline import producer as producer_module
from azki_pipeline.contracts import validate_event
from azki_pipeline.producer import main, produce_csv
from tests.helpers.factories import make_event

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


class RecordingProducer:
    def __init__(self, delivery_error=None, remaining=0):
        self.messages = []
        self.delivery_error = delivery_error
        self.remaining = remaining
        self.polls = []

    def produce(self, topic, *, key, value, on_delivery):
        self.messages.append((topic, key, value, on_delivery))

    def poll(self, timeout):
        self.polls.append(timeout)

    def flush(self, timeout):
        for _, _, _, callback in self.messages:
            callback(self.delivery_error, None)
        return self.remaining


def write_events(tmp_path, rows):
    path = tmp_path / "events.csv"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(make_event()))
        writer.writeheader()
        writer.writerows(rows)
    return path


@pytest.mark.unit
def test_U03_kafka_key_is_user_id_and_json_matches_wire_contract(tmp_path):
    event = make_event()
    source = write_events(tmp_path, [event])
    kafka = RecordingProducer()

    result = produce_csv(source, kafka, timezone_name="UTC")

    assert result.published == 1
    assert result.errors == []
    topic, key, payload, _ = kafka.messages[0]
    assert topic == "azki.user_events"
    assert key == str(event["user_id"]).encode("utf-8")
    assert isinstance(payload, bytes)
    decoded = json.loads(payload)
    assert decoded == event | {"event_time": "2025-10-16T17:17:33Z"}
    assert validate_event(decoded, wire=True) is None
    assert kafka.polls == [0]


@pytest.mark.unit
def test_U03_synthetic_fixture_publishes_every_row_in_source_order():
    kafka = RecordingProducer()
    with (FIXTURES / "user_events.csv").open(newline="") as stream:
        expected = list(csv.DictReader(stream))

    result = produce_csv(FIXTURES / "user_events.csv", kafka, timezone_name="UTC")

    assert result.published == len(expected)
    assert result.errors == []
    assert [json.loads(message[2])["user_id"] for message in kafka.messages] == [
        int(row["user_id"]) for row in expected
    ]
    assert kafka.messages[0][2] == kafka.messages[-1][2]


@pytest.mark.unit
def test_U03_cli_configures_idempotent_producer(tmp_path, monkeypatch, capsys):
    source = write_events(tmp_path, [make_event()])
    kafka = RecordingProducer()
    configurations = []

    def create_producer(config):
        configurations.append(config)
        return kafka

    monkeypatch.setattr(producer_module, "Producer", create_producer)

    status = main([str(source), "--bootstrap-servers", "localhost:9092", "--timezone", "UTC"])

    assert status == 0
    assert configurations == [
        {"bootstrap.servers": "localhost:9092", "enable.idempotence": True, "acks": "all"}
    ]
    assert json.loads(capsys.readouterr().out) == {"published": 1, "errors": []}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("timezone_name", "expected"),
    [("UTC", "2025-10-16T17:17:33Z"), ("Asia/Tehran", "2025-10-16T13:47:33Z")],
)
def test_U04_configurable_timezone_emits_utc(tmp_path, timezone_name, expected):
    source = write_events(tmp_path, [make_event()])
    kafka = RecordingProducer()

    produce_csv(source, kafka, timezone_name=timezone_name)

    assert json.loads(kafka.messages[0][2])["event_time"] == expected


@pytest.mark.unit
def test_U04_environment_timezone_is_used_unless_explicitly_overridden(tmp_path, monkeypatch):
    source = write_events(tmp_path, [make_event()])
    monkeypatch.setenv("EVENTS_TZ", "Asia/Tehran")
    from_environment = RecordingProducer()
    explicit = RecordingProducer()

    produce_csv(source, from_environment)
    produce_csv(source, explicit, timezone_name="UTC")

    assert json.loads(from_environment.messages[0][2])["event_time"] == "2025-10-16T13:47:33Z"
    assert json.loads(explicit.messages[0][2])["event_time"] == "2025-10-16T17:17:33Z"


@pytest.mark.unit
def test_U04_default_timezone_is_utc(tmp_path, monkeypatch):
    monkeypatch.delenv("EVENTS_TZ", raising=False)
    kafka = RecordingProducer()

    produce_csv(write_events(tmp_path, [make_event()]), kafka)

    assert json.loads(kafka.messages[0][2])["event_time"] == "2025-10-16T17:17:33Z"


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [
        {"user_id": "bad"},
        {"user_id": ""},
        {"event_time": "bad"},
        {"event_time": "2025-02-30 00:00:00"},
        {"event_type": "unknown"},
        {"premium_amount": -1},
        {"premium_amount": "1.5"},
        {"user_id": None},
    ],
)
def test_U05_bad_row_is_collected_and_next_row_still_publishes(tmp_path, overrides):
    source = write_events(tmp_path, [make_event(**overrides), make_event(user_id=4)])
    kafka = RecordingProducer()

    result = produce_csv(source, kafka, timezone_name="UTC")

    assert result.published == 1
    assert len(result.errors) == 1
    assert result.errors[0].line_number == 2
    assert result.errors[0].reason
    assert json.loads(kafka.messages[0][2])["user_id"] == 4


@pytest.mark.unit
@pytest.mark.parametrize("extra_field", [True, False])
def test_U05_wrong_csv_field_count_is_collected(tmp_path, extra_field):
    source = write_events(tmp_path, [make_event(), make_event(user_id=4)])
    lines = source.read_text().splitlines()
    fields = lines[1].split(",")
    lines[1] = ",".join(fields + ["extra"] if extra_field else fields[:-1])
    source.write_text("\n".join(lines) + "\n")
    kafka = RecordingProducer()

    result = produce_csv(source, kafka, timezone_name="UTC")

    assert result.published == 1
    assert result.errors[0].line_number == 2
    assert len(result.errors) == 1


@pytest.mark.unit
def test_U05_malformed_quoting_reports_physical_line_and_continues(tmp_path):
    source = write_events(tmp_path, [make_event(), make_event(user_id=4)])
    lines = source.read_text().splitlines()
    fields = lines[1].split(",")
    fields[0] = f'"{fields[0]}"invalid'
    lines[1] = ",".join(fields)
    source.write_text("\n".join(lines) + "\n")
    kafka = RecordingProducer()

    result = produce_csv(source, kafka, timezone_name="UTC")

    assert result.published == 1
    assert len(result.errors) == 1
    assert result.errors[0].line_number == 2
    assert "Malformed CSV" in result.errors[0].reason
    assert json.loads(kafka.messages[0][2])["user_id"] == 4


@pytest.mark.unit
def test_U03_bom_and_new_fields_are_preserved_for_drift_detection(tmp_path):
    source = tmp_path / "events.csv"
    event = make_event(utm_campaign="synthetic campaign")
    with source.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(event))
        writer.writeheader()
        writer.writerow(event)
    kafka = RecordingProducer()

    result = produce_csv(source, kafka, topic="custom.events", timezone_name="UTC")

    assert result.published == 1
    assert result.errors == []
    assert kafka.messages[0][0] == "custom.events"
    assert json.loads(kafka.messages[0][2])["utm_campaign"] == event["utm_campaign"]


@pytest.mark.unit
@pytest.mark.parametrize("contents", ["", "user_id,session_id\n3,s_1\n"])
def test_U05_invalid_header_is_reported(tmp_path, contents):
    source = tmp_path / "invalid.csv"
    source.write_text(contents)

    result = produce_csv(source, RecordingProducer(), timezone_name="UTC")

    assert result.published == 0
    assert len(result.errors) == 1
    assert result.errors[0].line_number == 1
    assert "header" in result.errors[0].reason.lower()


@pytest.mark.unit
def test_U05_delivery_errors_are_not_misreported_as_bad_rows(tmp_path):
    source = write_events(tmp_path, [make_event()])

    with pytest.raises(RuntimeError, match="delivery"):
        produce_csv(source, RecordingProducer(delivery_error="broker unavailable"))


@pytest.mark.unit
def test_U05_undelivered_messages_raise_timeout(tmp_path):
    source = write_events(tmp_path, [make_event()])

    with pytest.raises(TimeoutError, match="undelivered"):
        produce_csv(source, RecordingProducer(remaining=1))


@pytest.mark.unit
def test_U05_cli_returns_nonzero_for_rejected_rows(tmp_path, monkeypatch, capsys):
    source = write_events(tmp_path, [make_event(premium_amount=-1)])
    monkeypatch.setattr(producer_module, "Producer", lambda config: RecordingProducer())

    status = main([str(source), "--bootstrap-servers", "localhost:9092"])

    assert status == 1
    output = json.loads(capsys.readouterr().out)
    assert output["published"] == 0
    assert output["errors"][0]["line_number"] == 2
