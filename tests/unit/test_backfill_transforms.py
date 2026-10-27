import csv
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from azki_pipeline.contracts import event_key
from tests.helpers.factories import make_event

pytestmark = pytest.mark.unit
FIELDS = ["event_time", "user_id", "session_id", "event_type", "channel", "premium_amount"]


def csv_frame(spark, events):
    return spark.createDataFrame(
        [
            tuple(None if row.get(name) is None else str(row[name]) for name in FIELDS)
            for row in events
        ],
        "event_time string, user_id string, session_id string, event_type string, "
        "channel string, premium_amount string",
    )


def test_U15_parse_validate_counts_invalid_rows(spark):
    from azki_pipeline.backfill.transforms import parse_validate

    changes = [
        {"event_time": "2025-02-30 10:00:00"},
        {"event_time": "bad"},
        {"user_id": None},
        {"user_id": "1.5"},
        {"user_id": str(2**63)},
        {"premium_amount": -1},
        {"premium_amount": str(2**63)},
        {"event_type": "unknown"},
        {"channel": None},
        {"session_id": None},
    ]
    frame = csv_frame(spark, [make_event(), *[make_event(**change) for change in changes]])

    valid, invalid = parse_validate(frame)

    assert valid.count() == 1
    assert invalid.count() == len(changes)
    assert all(row._error for row in invalid.collect())
    assert valid.first().event_time == datetime(2025, 10, 16, 17, 17, 33)


def test_U15_csv_timezone_is_normalized_before_hashing(spark):
    from azki_pipeline.backfill.transforms import parse_validate

    valid, invalid = parse_validate(csv_frame(spark, [make_event()]), timezone_name="Asia/Tehran")

    row = valid.first()
    assert invalid.count() == 0
    assert row.event_time == datetime(2025, 10, 16, 13, 47, 33)
    assert int(row.event_key) == event_key(3, "s_1", "2025-10-16 13:47:33", "policy_click")


def test_U15_kafka_validation_rejects_wrong_json_types_and_malformed_payloads(spark):
    from azki_pipeline.backfill.transforms import parse_validate

    event = make_event(event_time="2025-10-16T17:17:33Z")
    payloads = [
        json.dumps(event),
        json.dumps(event | {"user_id": "3"}),
        json.dumps(event | {"premium_amount": True}),
        "{",
        "[]",
    ]
    frame = spark.createDataFrame([(value,) for value in payloads], "value string")

    valid, invalid = parse_validate(frame, source="kafka")

    assert valid.count() == 1
    assert invalid.count() == 4


def test_U15_csv_reader_preserves_empty_strings_and_detects_short_records(spark, tmp_path):
    from azki_pipeline.backfill.job import read_source
    from azki_pipeline.backfill.transforms import parse_validate

    path = tmp_path / "empty_strings.csv"
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows([make_event(), make_event(session_id="", channel="")])
        stream.write("2025-10-16 17:17:33,3\n")
    frame = read_source(spark, source="csv", path=path, start=None, end=None).cache()
    try:
        valid, invalid = parse_validate(frame)

        assert valid.count() == 2
        assert invalid.count() == 1
        assert any(row.session_id == "" and row.channel == "" for row in valid.collect())
    finally:
        frame.unpersist()


def test_U16_hash_matches_python_and_dedupe_preserves_first_payload(spark):
    from azki_pipeline.backfill.transforms import deduplicate, parse_validate

    events = [
        make_event(user_id=-5, session_id="نشست"),
        make_event(user_id=-5, session_id="نشست", premium_amount=9999),
        make_event(session_id="another"),
    ]
    valid, _ = parse_validate(csv_frame(spark, events))

    rows = deduplicate(valid).collect()

    expected = {
        event_key(row["user_id"], row["session_id"], row["event_time"], row["event_type"])
        for row in events
    }
    assert {int(row.event_key) for row in rows} == expected
    assert len(rows) == 2
    assert next(row for row in rows if row.user_id == -5).premium_amount == 1000


def test_U17_missing_keys_uses_left_anti_join(spark):
    from azki_pipeline.backfill.transforms import missing_events, parse_validate

    valid, _ = parse_validate(csv_frame(spark, [make_event(), make_event(session_id="missing")]))
    key = event_key(3, "s_1", "2025-10-16 17:17:33", "policy_click")
    existing = spark.createDataFrame([(Decimal(key),), (Decimal(key),)], "event_key decimal(20,0)")

    rows = missing_events(valid, existing).collect()

    assert [row.session_id for row in rows] == ["missing"]


def test_U18_date_range_is_utc_half_open(spark):
    from azki_pipeline.backfill.transforms import filter_range, parse_validate

    events = [make_event(event_time=f"2025-10-16 {hour}:00:00") for hour in ("09", "10", "11")]
    valid, _ = parse_validate(csv_frame(spark, events))
    start = datetime(2025, 10, 16, 10, tzinfo=UTC)
    end = datetime(2025, 10, 16, 11, tzinfo=UTC)

    assert filter_range(valid, start, end).count() == 1
    for bounds in [(end, start), (start, start), (start.replace(tzinfo=None), end)]:
        with pytest.raises(ValueError):
            filter_range(valid, *bounds)
