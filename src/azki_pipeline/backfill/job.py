import argparse
import csv
import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from itertools import islice
from pathlib import Path
from uuid import uuid4

import clickhouse_connect
from pyspark.sql import SparkSession

from azki_pipeline.backfill.transforms import (
    EVENT_COLUMNS,
    deduplicate,
    filter_range,
    missing_events,
    parse_validate,
    utc_bounds,
)
from azki_pipeline.reconcile import rebuild_aggregate_partition
from azki_pipeline.sql_runner import load_sql

SQL = Path(__file__).resolve().parents[3] / "sql" / "backfill"


def create_spark(*, kafka=False):
    builder = SparkSession.builder.appName("azki-backfill").config(
        "spark.sql.session.timeZone", "UTC"
    )
    if kafka:
        import pyspark

        builder = builder.config(
            "spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}",
        )
    return builder.getOrCreate()


def read_source(spark, *, source, start, end, path=None, kafka_bootstrap=None, topic=None):
    if source == "csv":
        with Path(path).open(newline="", encoding="utf-8-sig") as stream:
            fields = next(csv.reader(stream), [])
        if (
            not set(EVENT_COLUMNS).issubset(fields)
            or len(fields) != len(set(fields))
            or "_corrupt_record" in fields
        ):
            raise ValueError("CSV header must contain each required field once")
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [StructField(name, StringType()) for name in fields + ["_corrupt_record"]]
        )
        return (
            spark.read.schema(schema)
            .options(header=True, mode="PERMISSIVE", emptyValue="")
            .csv(str(path))
            .fillna({"session_id": "", "channel": ""})
        )
    return (
        spark.read.format("kafka")
        .options(
            **{
                "kafka.bootstrap.servers": kafka_bootstrap,
                "subscribe": topic,
                "startingTimestamp": str(int(start.timestamp() * 1000)),
                "endingTimestamp": str(int(end.timestamp() * 1000)),
                "startingOffsetsByTimestampStrategy": "error",
                "failOnDataLoss": "true",
            }
        )
        .load()
        .selectExpr(
            "CAST(value AS STRING) AS value",
            "topic AS _topic",
            "partition AS _partition",
            "offset AS _offset",
        )
        .orderBy("_partition", "_offset")
    )


def _partitions(start, end):
    current = start.replace(day=1, hour=0, minute=0, second=0)
    while current < end:
        yield current.year * 100 + current.month
        current = (
            current.replace(year=current.year + 1, month=1)
            if current.month == 12
            else (current.replace(month=current.month + 1))
        )


def _insert_events(client, table, frame, *, on_batch=None):
    columns = EVENT_COLUMNS + [
        name for name in ("_topic", "_partition", "_offset") if name in frame.columns
    ]
    iterator = frame.select(*columns).toLocalIterator()
    written = 0
    while batch := list(islice(iterator, 1000)):
        rows = []
        for row in batch:
            values = list(row)
            values[0] = values[0].replace(tzinfo=UTC)
            rows.append(values)
        client.insert(table, rows, column_names=columns)
        written += len(rows)
        if on_batch is not None:
            on_batch(written)
    return written


def run_backfill(
    spark,
    client,
    *,
    source,
    start,
    end,
    path=None,
    mode="fill-gaps",
    dry_run=False,
    writers_paused=False,
    database_overrides=None,
    timezone_name="UTC",
    kafka_bootstrap=None,
    topic="azki.user_events",
):
    """Recover [start, end); serialize jobs and pause all target writers before applying."""
    start, end = utc_bounds(start, end)
    if source not in {"csv", "kafka"}:
        raise ValueError("source must be csv or kafka")
    if mode not in {"fill-gaps", "replace-partition"}:
        raise ValueError("mode must be fill-gaps or replace-partition")
    if not dry_run and not writers_paused:
        raise ValueError(
            "Pause event writers, refreshes and other repairs; acknowledge writers_paused"
        )
    if mode == "replace-partition" and any(
        value.day != 1 or value.hour or value.minute or value.second for value in (start, end)
    ):
        raise ValueError("replace-partition requires complete UTC month boundaries")
    if source == "csv" and path is None:
        raise ValueError("CSV source requires path")
    if source == "kafka" and (not kafka_bootstrap or not topic):
        raise ValueError("Kafka source requires bootstrap and topic")
    overrides = database_overrides or {}
    if any(
        not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value)
        for pair in overrides.items()
        for value in pair
    ):
        raise ValueError("Invalid database identifier")
    core, dq = (overrides.get(name, name) for name in ("core", "dq"))
    summary = dict(
        run_id=uuid4(),
        source=source,
        mode=mode,
        status="failed",
        range_start=start,
        range_end=end,
        rows_read=0,
        invalid=0,
        missing=0,
        written=0,
        error="",
        started_at=datetime.now(UTC),
    )
    cached = []

    def queries(filename):
        return load_sql(SQL / filename, database_overrides=overrides)

    def execute(filename, parameters=None):
        for query in queries(filename):
            client.command(query, parameters=parameters)

    def record_written(count):
        summary["written"] = count

    try:
        frame = read_source(
            spark,
            source=source,
            start=start,
            end=end,
            path=path,
            kafka_bootstrap=kafka_bootstrap,
            topic=topic,
        ).cache()
        cached.append(frame)
        summary["rows_read"] = frame.count()
        valid, invalid = parse_validate(frame, source=source, timezone_name=timezone_name)
        summary["invalid"] = invalid.count()
        if mode == "replace-partition" and summary["invalid"]:
            raise ValueError("Cannot replace a partition from a source containing invalid rows")
        candidates = deduplicate(filter_range(valid, start, end)).cache()
        cached.append(candidates)
        keys = client.query(
            queries("001_existing_keys.sql")[0], parameters={"start": start, "end": end}
        ).result_rows
        existing = spark.createDataFrame(
            [(Decimal(row[0]),) for row in keys], "event_key decimal(20,0)"
        )
        missing = missing_events(candidates, existing).cache()
        cached.append(missing)
        summary["missing"] = missing.count()
        if not dry_run:
            if mode == "fill-gaps":
                _insert_events(client, f"{core}.user_events", missing, on_batch=record_written)
            else:
                execute("002_clear_staging.sql")
                _insert_events(client, f"{core}.user_events_staging", candidates)
            for partition in _partitions(start, end):
                parameters = {"partition": partition}
                if mode == "replace-partition":
                    count = candidates.where(
                        f"year(event_time) * 100 + month(event_time) = {partition}"
                    ).count()
                    execute(
                        "003_replace_events.sql" if count else "004_drop_events.sql", parameters
                    )
                    summary["written"] += count
                rebuild_aggregate_partition(client, partition, database_overrides=overrides)
                execute("005_stage_purchases.sql", parameters)
                count = client.query(
                    queries("006_count_purchases.sql")[0], parameters=parameters
                ).first_row[0]
                execute(
                    "007_replace_purchases.sql" if count else "008_drop_purchases.sql", parameters
                )
        summary["status"] = "dry-run" if dry_run else "success"
    except Exception as error:
        summary["error"] = type(error).__name__
        raise
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        summary["finished_at"] = datetime.now(UTC)
        client.insert(f"{dq}.backfill_runs", [list(summary.values())], column_names=list(summary))
    return {
        name: str(value) if isinstance(value, (datetime, type(summary["run_id"]))) else value
        for name, value in summary.items()
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Recover UTC event ranges with Spark")
    parser.add_argument("--source", required=True, choices=["csv", "kafka"])
    parser.add_argument("--path", type=Path)
    parser.add_argument("--start", required=True, type=datetime.fromisoformat)
    parser.add_argument("--end", required=True, type=datetime.fromisoformat)
    parser.add_argument("--mode", choices=["fill-gaps", "replace-partition"], default="fill-gaps")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--writers-paused", action="store_true")
    parser.add_argument("--timezone", dest="timezone_name", default="UTC")
    parser.add_argument("--kafka-bootstrap")
    parser.add_argument("--topic", default="azki.user_events")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8123)
    args = vars(parser.parse_args(argv))
    host, port = args.pop("host"), args.pop("port")
    client = clickhouse_connect.get_client(host=host, port=port)
    spark = None
    try:
        spark = create_spark(kafka=args["source"] == "kafka")
        print(json.dumps(run_backfill(spark, client, **args)))
    finally:
        if spark is not None:
            spark.stop()
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
