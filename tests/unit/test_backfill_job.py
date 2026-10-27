import json
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

pytestmark = pytest.mark.unit
START = datetime(2025, 10, 1, tzinfo=UTC)
END = datetime(2025, 11, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    "options,reason",
    [
        ({"writers_paused": False}, "writers"),
        ({"mode": "unknown"}, "mode"),
        ({"source": "unknown"}, "source"),
        ({"start": END}, "start"),
        ({"start": START.replace(tzinfo=None)}, "aware"),
        ({"start": START.replace(day=2), "mode": "replace-partition"}, "month"),
        ({"end": END.replace(hour=1), "mode": "replace-partition"}, "month"),
        ({"database_overrides": {"core": "bad;DROP core"}}, "database"),
    ],
)
def test_U32_invalid_job_options_never_touch_clickhouse(options, reason):
    from azki_pipeline.backfill.job import run_backfill

    client = Mock()
    arguments = dict(source="csv", path="unused.csv", start=START, end=END, writers_paused=True)

    with pytest.raises(ValueError, match=reason):
        run_backfill(Mock(), client, **(arguments | options))

    assert client.mock_calls == []


def test_U32_kafka_source_is_bounded_batch_with_provenance():
    from azki_pipeline.backfill.job import read_source

    spark = Mock()
    reader = spark.read.format.return_value
    reader.options.return_value = reader
    reader.load.return_value.selectExpr.return_value.orderBy.return_value = "bounded"

    result = read_source(
        spark,
        source="kafka",
        start=START,
        end=END,
        kafka_bootstrap="localhost:9092",
        topic="test.events",
    )

    assert result == "bounded"
    spark.read.format.assert_called_once_with("kafka")
    options = reader.options.call_args.kwargs
    assert options["startingTimestamp"] == str(int(START.timestamp() * 1000))
    assert options["endingTimestamp"] == str(int(END.timestamp() * 1000))
    assert options["startingOffsetsByTimestampStrategy"] == "error"
    assert options["failOnDataLoss"] == "true"
    assert options["kafka.bootstrap.servers"] == "localhost:9092"


def test_U32_cli_prints_summary_and_closes_resources(monkeypatch, capsys):
    from azki_pipeline.backfill import job

    client, spark = Mock(), Mock()
    monkeypatch.setattr(job.clickhouse_connect, "get_client", Mock(return_value=client))
    monkeypatch.setattr(job, "create_spark", Mock(return_value=spark))
    run = Mock(return_value={"written": 0, "status": "dry-run"})
    monkeypatch.setattr(job, "run_backfill", run)

    result = job.main(
        [
            "--source",
            "csv",
            "--path",
            "events.csv",
            "--start",
            START.isoformat(),
            "--end",
            END.isoformat(),
            "--dry-run",
        ]
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out) == {"written": 0, "status": "dry-run"}
    assert run.call_args.kwargs["dry_run"] is True
    spark.stop.assert_called_once()
    client.close.assert_called_once()
