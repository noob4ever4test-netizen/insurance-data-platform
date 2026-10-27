from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

import pytest
from confluent_kafka import TopicPartition
from pydantic import ValidationError

from azki_pipeline.contracts import load_schema
from azki_pipeline.dq import checks, runner
from azki_pipeline.dq.collectors import collect_kafka_lag
from azki_pipeline.dq.models import CheckResult
from tests.helpers.factories import make_event

pytestmark = pytest.mark.unit
NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
LIMITS = {"warn": 10, "fail": 20}


def metrics(**values):
    return {"checked_at": NOW, **values}


@pytest.mark.parametrize(
    ("age", "status"), [(0, "PASS"), (10, "PASS"), (11, "WARN"), (20, "WARN"), (21, "FAIL")]
)
@pytest.mark.parametrize("stale_field", ["last_event_at", "last_ingested_at"])
def test_U07_freshness_boundaries(age, status, stale_field):
    values = metrics(last_event_at=NOW, last_ingested_at=NOW)
    values[stale_field] = NOW - timedelta(seconds=age)

    result = checks.evaluate_freshness(values, LIMITS)

    assert result.status == status
    assert result.observed["age_seconds"] == age
    assert result.observed[stale_field] == values[stale_field]
    assert result.checked_at == NOW


@pytest.mark.parametrize("last", [None, NOW + timedelta(minutes=2)])
def test_U07_empty_or_future_data_cannot_report_healthy(last):
    result = checks.evaluate_freshness(metrics(last_event_at=last, last_ingested_at=last), LIMITS)

    assert result.status == ("WARN" if last is None else "FAIL")


@pytest.mark.parametrize(("age", "status"), [(10, "PASS"), (11, "WARN"), (21, "FAIL")])
def test_U07_cdc_uses_heartbeat_freshness(age, status):
    result = checks.evaluate_cdc_lag(
        metrics(last_heartbeat_at=NOW - timedelta(seconds=age)), LIMITS
    )

    assert result.status == status
    assert checks.evaluate_cdc_lag(metrics(last_heartbeat_at=None), LIMITS).status == "WARN"


@pytest.mark.parametrize(
    ("lag", "status"), [(10, "PASS"), (11, "WARN"), (20, "WARN"), (21, "FAIL")]
)
def test_U08_lag_uses_worst_partition_and_reports_total(lag, status):
    result = checks.evaluate_lag(
        metrics(
            partitions=[
                {"topic": "events", "partition": 0, "committed": 5, "low": 0, "high": 15},
                {"topic": "events", "partition": 1, "committed": 5, "low": 0, "high": 5 + lag},
            ]
        ),
        LIMITS,
    )

    assert result.status == status
    assert result.observed["total_lag"] == 10 + lag
    assert result.observed["max_lag"] == lag


def test_U08_uncommitted_and_retained_offsets_are_not_negative():
    result = checks.evaluate_lag(
        metrics(
            partitions=[
                {"topic": "events", "partition": 0, "committed": -1, "low": 100, "high": 115},
            ]
        ),
        LIMITS,
    )

    assert result.observed["total_lag"] == 15
    assert result.status == "WARN"
    assert checks.evaluate_lag(metrics(partitions=[]), LIMITS).status == "WARN"


@pytest.mark.parametrize(("count", "missing"), [(4, 0), (3, 1), (1, 3)])
def test_U09_offset_gaps_use_distinct_offsets_per_topic_partition(count, missing):
    result = checks.evaluate_offset_gaps(
        metrics(
            partitions=[
                {
                    "topic": "events",
                    "partition": 0,
                    "min_offset": 5,
                    "max_offset": 8,
                    "offset_count": count,
                },
                {
                    "topic": "other",
                    "partition": 0,
                    "min_offset": 0,
                    "max_offset": 1,
                    "offset_count": 2,
                },
            ]
        ),
        {"warn": 0, "fail": 0},
    )

    assert result.observed["missing_offsets"] == missing
    assert result.status == ("FAIL" if missing else "PASS")


@pytest.mark.parametrize(
    ("current", "status"), [(10, "PASS"), (13, "PASS"), (14, "WARN"), (16, "WARN"), (17, "FAIL")]
)
def test_U10_flat_history_uses_mad_floor(current, status):
    result = checks.evaluate_volume(
        metrics(current=current, history=[10] * 7),
        {"warn": 3, "fail": 6, "mad_floor": 1, "min_history": 7},
    )

    assert result.status == status
    assert result.observed["median"] == 10
    assert result.observed["mad"] == 0


def test_U10_trailing_history_is_robust_and_insufficient_history_warns():
    limits = {"warn": 3, "fail": 6, "mad_floor": 1, "min_history": 7}

    result = checks.evaluate_volume(
        metrics(current=10, history=[9, 10, 11, 10, 10, 9, 1000]), limits
    )

    assert result.status == "PASS"
    assert checks.evaluate_volume(metrics(current=10, history=[10] * 6), limits).status == "WARN"


@pytest.mark.parametrize(
    ("orphans", "status"), [(0, "PASS"), (1, "PASS"), (2, "WARN"), (10, "WARN"), (11, "FAIL")]
)
def test_U11_orphan_ratio_boundaries(orphans, status):
    result = checks.evaluate_orphans(
        metrics(events=100, orphans=orphans, users_loaded=1), {"warn": 0.01, "fail": 0.1}
    )

    assert result.status == status
    assert result.observed["ratio"] == orphans / 100


def test_U11_orphans_wait_for_users_and_handle_empty_events():
    limits = {"warn": 0.01, "fail": 0.1}

    assert (
        checks.evaluate_orphans(metrics(events=10, orphans=10, users_loaded=0), limits).status
        == "WARN"
    )
    assert (
        checks.evaluate_orphans(metrics(events=0, orphans=0, users_loaded=1), limits).status
        == "PASS"
    )


@pytest.mark.parametrize(
    ("change", "status"),
    [
        ({"utm_campaign": "synthetic"}, "WARN"),
        ({"user_id": "3"}, "FAIL"),
        ({"user_id": True}, "FAIL"),
        ({"user_id": None}, "FAIL"),
        ({}, "PASS"),
    ],
)
def test_U12_schema_drift_detects_additions_and_type_changes(change, status):
    event = make_event(**change)
    original = deepcopy(event)

    result = checks.evaluate_schema_drift(
        metrics(samples=[event], schema=load_schema("user_event")), {}
    )

    assert result.status == status
    assert event == original
    assert "synthetic" not in result.model_dump_json()


def test_U12_missing_required_fields_and_invalid_samples_fail():
    event = make_event()
    del event["session_id"]

    for samples in ([event], ["not-json"], [[]]):
        assert (
            checks.evaluate_schema_drift(
                metrics(samples=samples, schema=load_schema("user_event")), {}
            ).status
            == "FAIL"
        )
    assert (
        checks.evaluate_schema_drift(
            metrics(samples=[], schema=load_schema("user_event")), {}
        ).status
        == "WARN"
    )


@pytest.mark.parametrize(
    ("unique", "status"), [(100, "PASS"), (99, "PASS"), (98, "WARN"), (95, "WARN"), (94, "FAIL")]
)
def test_U13_duplicate_ratio_boundaries(unique, status):
    result = checks.evaluate_duplicates(
        metrics(rows=100, unique_keys=unique), {"warn": 0.01, "fail": 0.05}
    )

    assert result.status == status
    assert result.observed["duplicates"] == 100 - unique
    assert checks.evaluate_duplicates(metrics(rows=0, unique_keys=0), LIMITS).status == "PASS"


@pytest.mark.parametrize(
    ("name", "values", "status"),
    [
        ("dead_letters", {"errors": 0, "received": 100}, "PASS"),
        ("dead_letters", {"errors": 1, "received": 100}, "WARN"),
        ("dead_letters", {"errors": 6, "received": 100}, "FAIL"),
        ("dead_letters", {"errors": 0, "received": 0}, "WARN"),
        ("purchase_enrichment", {"purchases": 100, "unmatched": 6}, "FAIL"),
        ("purchase_enrichment", {"purchases": 0, "unmatched": 0}, "PASS"),
        ("insert_health", {"max_parts": 1, "insert_errors": 1, "part_log_available": True}, "FAIL"),
        (
            "insert_health",
            {"max_parts": 1, "insert_errors": 0, "part_log_available": False},
            "WARN",
        ),
        ("insert_health", {"max_parts": 6, "insert_errors": 0, "part_log_available": True}, "FAIL"),
        (
            "source_reconciliation",
            {"source_counts": {"hour": 5}, "sink_counts": {"hour": 4}},
            "FAIL",
        ),
        (
            "source_reconciliation",
            {"source_counts": {"hour": 5}, "sink_counts": {"hour": 5}},
            "PASS",
        ),
        ("source_reconciliation", {"source_counts": None, "sink_counts": {}}, "WARN"),
        ("source_reconciliation", {"source_counts": {}, "sink_counts": {}}, "WARN"),
    ],
)
def test_U14_additional_checks(name, values, status):
    limits = (
        {"warn": 0, "fail": 0.05}
        if name in {"dead_letters", "purchase_enrichment"}
        else {"warn": 2, "fail": 5}
    )
    if name == "source_reconciliation":
        limits = {"warn": 0, "fail": 0}

    result = getattr(checks, f"evaluate_{name}")(metrics(**values), limits)

    assert result.status == status


@pytest.mark.parametrize(("status", "expected"), [("PASS", 0), ("WARN", 0), ("FAIL", 1)])
def test_U14_runner_serializes_results_and_sets_exit_code(status, expected):
    result = CheckResult(
        name="freshness",
        status=status,
        observed={"age": 1},
        threshold=LIMITS,
        details={},
        checked_at=NOW,
    )
    client = Mock()

    with (
        patch.object(runner, "collect_metrics", return_value={}),
        patch.object(runner, "evaluate_all", return_value=[result]),
    ):
        results = runner.run_checks(client, now=NOW)

    assert runner.exit_code(results) == expected
    table, rows = client.insert.call_args.args
    assert table == "dq.check_results"
    assert rows[0][0:2] == ["freshness", status]
    assert rows[0][-1] == NOW
    assert CheckResult.model_validate_json(result.model_dump_json()) == result


def test_U14_missing_collectors_fail_instead_of_disappearing():
    thresholds = runner.load_thresholds()

    results = runner.evaluate_all({}, thresholds, checked_at=NOW)

    assert len(results) == 12
    assert all(result.status == "FAIL" for result in results)


@pytest.mark.parametrize("content", ["freshness: {warn: 20, fail: 10}", "unexpected: {}"])
def test_U14_invalid_thresholds_are_rejected(tmp_path, content):
    path = tmp_path / "thresholds.yaml"
    path.write_text(content)

    with pytest.raises((ValidationError, ValueError)):
        runner.load_thresholds(path)


def test_U14_cli_prints_json_and_closes_client(capsys):
    result = CheckResult(name="freshness", status="FAIL", observed={}, threshold={}, checked_at=NOW)
    with (
        patch.object(runner.clickhouse_connect, "get_client") as connect,
        patch.object(runner, "run_checks", return_value=[result]),
    ):
        assert runner.main([]) == 1

    connect.return_value.close.assert_called_once_with()
    assert CheckResult.model_validate_json(capsys.readouterr().out.strip()) == result


@pytest.mark.parametrize(
    "source", [[], {"invalid": 1}, {"2026-10-27T11:00:00Z": -1}, {"2026-10-27T11:00:00Z": True}]
)
def test_U14_source_manifest_rejects_invalid_hours_and_counts(source):
    client = Mock()

    with pytest.raises(ValueError):
        runner.collect_metrics(client, now=NOW, source_counts=source)

    client.query.assert_not_called()


def test_U08_partition_offset_errors_are_not_treated_as_empty_backlog():
    admin = Mock()
    admin.list_topics.return_value.topics = {"events": Mock(error=None, partitions={0: Mock()})}
    response = Mock()
    response.result.return_value.topic_partitions = [
        Mock(topic="events", partition=0, offset=-1, error=RuntimeError("offset access denied")),
    ]
    admin.list_consumer_group_offsets.return_value = {"group": response}
    offset = Mock()
    offset.result.return_value.offset = 0
    admin.list_offsets.return_value = {TopicPartition("events", 0): offset}

    with (
        patch("azki_pipeline.dq.collectors.AdminClient", return_value=admin),
        pytest.raises(RuntimeError),
    ):
        collect_kafka_lag("broker:9092", "events", "group")


def test_U14_dead_letters_fail_when_receipt_evidence_is_missing():
    result = checks.evaluate_dead_letters(metrics(errors=1, received=0), {"warn": 0, "fail": 0.05})

    assert result.status == "FAIL"
