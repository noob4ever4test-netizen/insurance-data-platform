from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from azki_pipeline.contracts import load_schema
from azki_pipeline.sql_runner import load_sql

SQL = Path(__file__).resolve().parents[3] / "sql/dq"


def collect_kafka_lag(bootstrap, topic, group):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(topic=topic, timeout=10).topics[topic]
    if metadata.error:
        raise RuntimeError("Kafka topic metadata unavailable")
    partitions = [TopicPartition(topic, partition) for partition in metadata.partitions]
    committed = (
        admin.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group, partitions)],
            request_timeout=10,
        )[group]
        .result(timeout=15)
        .topic_partitions
    )
    if any(partition.error for partition in committed):
        raise RuntimeError("Kafka group offset unavailable")
    latest = admin.list_offsets(
        {partition: OffsetSpec.latest() for partition in partitions}, request_timeout=10
    )
    earliest = admin.list_offsets(
        {partition: OffsetSpec.earliest() for partition in partitions}, request_timeout=10
    )
    committed = {
        (partition.topic, partition.partition): partition.offset for partition in committed
    }
    return [
        {
            "topic": partition.topic,
            "partition": partition.partition,
            "committed": committed[(partition.topic, partition.partition)],
            "high": latest[partition].result(timeout=15).offset,
            "low": earliest[partition].result(timeout=15).offset,
        }
        for partition in partitions
    ]


def _utc(value):
    return value.replace(tzinfo=UTC) if value is not None and value.tzinfo is None else value


def _hour(value):
    return _utc(value).strftime("%Y-%m-%dT%H:00:00Z")


def collect(
    client,
    *,
    now,
    database_overrides=None,
    source_counts=None,
    kafka_bootstrap=None,
    event_topic="azki.user_events",
    consumer_group="azki-events",
):
    if now.tzinfo is None:
        raise ValueError("now must include a timezone")
    now = now.astimezone(UTC)
    overrides = database_overrides or {}
    target = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    source_hours = [target]
    if source_counts is not None:
        if not isinstance(source_counts, dict):
            raise ValueError("Source counts must map UTC hours to nonnegative integer counts")
        source_hours = []
        for hour, count in source_counts.items():
            if not isinstance(hour, str) or type(count) is not int or count < 0:
                raise ValueError("Invalid source hour or count")
            parsed = datetime.strptime(hour, "%Y-%m-%dT%H:00:00Z").replace(tzinfo=UTC)
            if _hour(parsed) != hour:
                raise ValueError("Source hours must be canonical UTC hour strings")
            source_hours.append(parsed)
    parameters = {
        "as_of": now,
        "window_start": now - timedelta(hours=1),
        "target_hour": target,
        "event_topic": event_topic,
        "raw_db": overrides.get("raw", "raw"),
        "databases": [overrides.get(name, name) for name in ("core", "mart", "dq")],
        "source_hours": source_hours,
    }

    def query(filename):
        (statement,) = load_sql(SQL / filename, database_overrides=overrides)
        result = client.query(statement, parameters=parameters)
        return [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]

    def single(filename):
        return query(filename)[0]

    def lag():
        consumers = query("002_consumers.sql")
        partitions = (
            collect_kafka_lag(kafka_bootstrap, event_topic, consumer_group)
            if kafka_bootstrap
            else []
        )
        return {"partitions": partitions, "consumers": consumers}

    def freshness():
        return {key: _utc(value) for key, value in single("001_freshness.sql").items()}

    def volume():
        counts = {_utc(row["hour"]): row["events"] for row in query("006_volume.sql")}
        first = freshness()["first_event_at"]
        history = [
            counts.get(target - timedelta(days=day), 0)
            for day in range(1, 8)
            if first is not None and first <= target - timedelta(days=day)
        ]
        return {"current": counts.get(target, 0), "history": history}

    def insert_health():
        values = single("010_parts.sql")
        exists = bool(next(iter(single("011_part_log_exists.sql").values())))
        values.update(
            single("012_part_log.sql") if exists else {"insert_errors": 0, "inserted_parts": 0}
        )
        return {
            **values,
            "part_log_available": exists,
            "inserts_per_minute": values["inserted_parts"] / 60,
        }

    collectors = {
        "freshness": freshness,
        "lag": lag,
        "cdc_lag": lambda: {key: _utc(value) for key, value in single("003_cdc.sql").items()},
        "offset_gaps": lambda: {"partitions": query("004_offsets.sql")},
        "source_reconciliation": lambda: {
            "source_counts": source_counts,
            "sink_counts": {_hour(row["hour"]): row["events"] for row in query("005_hourly.sql")},
        },
        "volume": volume,
        "orphans": lambda: single("007_orphans.sql"),
        "schema_drift": lambda: {
            "samples": [row["raw_message"] for row in query("008_samples.sql")],
            "schema": load_schema("user_event"),
        },
        "dead_letters": lambda: single("009_dead_letters.sql"),
        "insert_health": insert_health,
        "duplicates": lambda: single("013_duplicates.sql"),
        "purchase_enrichment": lambda: single("014_purchases.sql"),
    }
    values = {}
    for name, collector in collectors.items():
        try:
            values[name] = collector()
        except Exception as error:
            values[name] = {"collection_error": type(error).__name__}
    return values
