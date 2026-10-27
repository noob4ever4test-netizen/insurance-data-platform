import json
from datetime import UTC, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from jsonschema import ValidationError
from pyspark.sql import Window
from pyspark.sql import functions as functions
from pyspark.sql import types as types

from azki_pipeline.contracts import event_key, validate_event

EVENT_COLUMNS = ["event_time", "user_id", "session_id", "event_type", "channel", "premium_amount"]
EVENT_SCHEMA = types.StructType(
    [
        types.StructField("event_time", types.TimestampNTZType()),
        types.StructField("user_id", types.LongType()),
        types.StructField("session_id", types.StringType()),
        types.StructField("event_type", types.StringType()),
        types.StructField("channel", types.StringType()),
        types.StructField("premium_amount", types.LongType()),
        types.StructField("event_key", types.DecimalType(20, 0)),
        types.StructField("_error", types.StringType()),
    ]
)


def utc_bounds(start, end):
    if start.tzinfo is None or end.tzinfo is None or start >= end:
        raise ValueError("start and end must be aware timestamps with start < end")
    start, end = start.astimezone(UTC), end.astimezone(UTC)
    if start.microsecond or end.microsecond:
        raise ValueError("range bounds must have whole-second precision")
    return start, end


def parse_validate(frame, *, source="csv", timezone_name="UTC"):
    """Return valid typed events and rejected source rows, retaining input order."""
    if source not in {"csv", "kafka"}:
        raise ValueError("source must be csv or kafka")
    source_zone = ZoneInfo(timezone_name)

    def parse(row):
        try:
            values = row.asDict()
            if source == "kafka":
                event = json.loads(values["value"])
                if not isinstance(event, dict):
                    raise ValueError("expected JSON object")
                validate_event(event, wire=True)
                timestamp = datetime.strptime(event["event_time"], "%Y-%m-%dT%H:%M:%SZ")
                timestamp = timestamp.replace(tzinfo=UTC)
            else:
                if values.get("_corrupt_record") is not None:
                    raise ValueError("malformed CSV record")
                event = {name: values.get(name) for name in EVENT_COLUMNS}
                for name in ("user_id", "premium_amount"):
                    value = event[name]
                    if isinstance(value, bool) or not isinstance(value, (str, int)):
                        raise ValueError("expected integer")
                    event[name] = int(value)
                validate_event(event)
                timestamp = datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S")
                timestamp = timestamp.replace(tzinfo=source_zone).astimezone(UTC)
            key = event_key(event["user_id"], event["session_id"], timestamp, event["event_type"])
            return (
                timestamp.replace(tzinfo=None),
                event["user_id"],
                event["session_id"],
                event["event_type"],
                event["channel"],
                event["premium_amount"],
                Decimal(key),
                None,
            )
        except (ValueError, TypeError, KeyError, OverflowError, ValidationError) as error:
            return (None, None, None, None, None, None, None, type(error).__name__)

    indexed = frame.withColumn("_source_index", functions.monotonically_increasing_id())
    parsed = indexed.withColumn(
        "_parsed", functions.udf(parse, EVENT_SCHEMA)(functions.struct(*frame.columns))
    )
    provenance = [name for name in ("_topic", "_partition", "_offset") if name in frame.columns]
    valid = (
        parsed.where("_parsed._error IS NULL")
        .select("_parsed.*", "_source_index", *provenance)
        .drop("_error")
    )
    invalid = parsed.where("_parsed._error IS NOT NULL").select(
        *frame.columns, functions.col("_parsed._error").alias("_error")
    )
    return valid, invalid


def deduplicate(frame):
    order = Window.partitionBy("event_key").orderBy("_source_index")
    return (
        frame.withColumn("_rank", functions.row_number().over(order))
        .where("_rank = 1")
        .drop("_rank")
    )


def missing_events(frame, existing):
    return frame.join(existing.select("event_key"), on="event_key", how="left_anti")


def filter_range(frame, start, end):
    """Select UTC event time in [start, end)."""
    start, end = utc_bounds(start, end)
    return frame.where(
        (
            functions.col("event_time")
            >= functions.lit(start.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz")
        )
        & (
            functions.col("event_time")
            < functions.lit(end.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz")
        )
    )
