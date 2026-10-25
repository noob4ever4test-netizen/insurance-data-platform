import argparse
import csv
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from confluent_kafka import Producer
from jsonschema import ValidationError

from azki_pipeline.contracts import load_schema, validate_event


class KafkaProducer(Protocol):
    def produce(self, topic: str, *, key: bytes, value: bytes, on_delivery) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True)
class RowError:
    line_number: int
    reason: str


@dataclass
class ProductionResult:
    published: int = 0
    errors: list[RowError] = field(default_factory=list)


def produce_csv(
    source: str | Path,
    producer: KafkaProducer,
    *,
    topic: str = "azki.user_events",
    timezone_name: str | None = None,
    flush_timeout: float = 10.0,
) -> ProductionResult:
    """Publish valid rows in order; collect bad-row reasons and surface delivery failures."""
    timezone_name = (
        timezone_name if timezone_name is not None else os.environ.get("EVENTS_TZ", "UTC")
    )
    source_timezone = ZoneInfo(timezone_name)
    result = ProductionResult()
    delivery_errors = []

    def on_delivery(error, message):
        if error is not None:
            delivery_errors.append(str(error))

    with Path(source).open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream, strict=True)
        required = set(load_schema("user_event")["required"])
        try:
            header = reader.fieldnames or []
        except csv.Error as error:
            result.errors.append(RowError(1, f"Invalid CSV header: {error}"))
            return result
        if not required.issubset(header) or len(set(header)) != len(header):
            result.errors.append(RowError(1, "CSV header must contain each required field once"))
            return result
        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            except csv.Error as error:
                result.errors.append(RowError(reader.reader.line_num, f"Malformed CSV: {error}"))
                continue
            try:
                if None in row or any(value is None for value in row.values()):
                    raise ValueError("CSV row field count does not match its header")
                event = dict(row)
                event["user_id"] = int(event["user_id"])
                event["premium_amount"] = int(event["premium_amount"])
                validate_event(event)
                local_time = datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S")
                event["event_time"] = (
                    local_time.replace(tzinfo=source_timezone)
                    .astimezone(UTC)
                    .strftime("%Y-%m-%dT%H:%M:%SZ")
                )
                validate_event(event, wire=True)
            except (ValueError, ValidationError) as error:
                reason = error.message if isinstance(error, ValidationError) else str(error)
                result.errors.append(RowError(reader.line_num, reason))
                continue
            producer.produce(
                topic,
                key=str(event["user_id"]).encode("utf-8"),
                value=json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
                on_delivery=on_delivery,
            )
            result.published += 1
            producer.poll(0)
    remaining = producer.flush(flush_timeout)
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed: {'; '.join(delivery_errors)}")
    if remaining:
        raise TimeoutError(f"Kafka flush left {remaining} undelivered messages")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish validated CSV events to Kafka")
    parser.add_argument("source", type=Path)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", default="azki.user_events")
    parser.add_argument("--timezone", dest="timezone_name")
    args = parser.parse_args(argv)
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    result = produce_csv(args.source, producer, topic=args.topic, timezone_name=args.timezone_name)
    print(json.dumps(asdict(result)))
    return int(bool(result.errors))


if __name__ == "__main__":
    raise SystemExit(main())
