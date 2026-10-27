import argparse
import json
import re
from datetime import UTC, datetime
from pathlib import Path

import clickhouse_connect
import yaml

from azki_pipeline.dq import checks
from azki_pipeline.dq.models import CheckResult, Thresholds

CHECKS = (
    "freshness",
    "lag",
    "cdc_lag",
    "offset_gaps",
    "source_reconciliation",
    "volume",
    "orphans",
    "schema_drift",
    "dead_letters",
    "insert_health",
    "duplicates",
    "purchase_enrichment",
)


def load_thresholds(path=None):
    path = Path(path) if path else Path(__file__).with_name("thresholds.yaml")
    return Thresholds.model_validate(yaml.safe_load(path.read_text())).model_dump()


def evaluate_all(metrics, thresholds, *, checked_at):
    results = []
    for name in CHECKS:
        values = metrics.get(name, {"collection_error": "MissingCollector"})
        values = {**values, "checked_at": checked_at}
        try:
            if "collection_error" in values:
                raise ValueError(values["collection_error"])
            result = getattr(checks, f"evaluate_{name}")(values, thresholds[name])
        except (KeyError, ValueError, TypeError, OverflowError) as error:
            result = CheckResult(
                name=name,
                status="FAIL",
                observed={},
                threshold=thresholds[name],
                details={"collection_error": values.get("collection_error", type(error).__name__)},
                checked_at=checked_at,
            )
        results.append(result)
    return results


def collect_metrics(
    client,
    *,
    now,
    database_overrides=None,
    source_counts=None,
    kafka_bootstrap=None,
    event_topic="azki.user_events",
    consumer_group="azki-events",
):
    from azki_pipeline.dq.collectors import collect

    return collect(
        client,
        now=now,
        database_overrides=database_overrides,
        source_counts=source_counts,
        kafka_bootstrap=kafka_bootstrap,
        event_topic=event_topic,
        consumer_group=consumer_group,
    )


def run_checks(client, *, now=None, thresholds=None, database_overrides=None, **options):
    now = now or datetime.now(UTC)
    thresholds = thresholds if thresholds is not None else load_thresholds()
    database = (database_overrides or {}).get("dq", "dq")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise ValueError("Invalid DQ database identifier")
    metrics = collect_metrics(client, now=now, database_overrides=database_overrides, **options)
    results = evaluate_all(metrics, thresholds, checked_at=now)
    rows = []
    for result in results:
        values = result.model_dump(mode="json")
        rows.append(
            [
                result.name,
                result.status,
                json.dumps(values["observed"]),
                json.dumps(values["threshold"]),
                json.dumps(values["details"]),
                result.checked_at,
            ]
        )
    client.insert(
        f"{database}.check_results",
        rows,
        column_names=["name", "status", "observed", "threshold", "details", "checked_at"],
    )
    return results


def exit_code(results):
    return int(any(result.status == "FAIL" for result in results))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Collect and persist pipeline data-quality results"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8123)
    parser.add_argument("--thresholds", type=Path)
    parser.add_argument(
        "--source-counts", type=Path, help="JSON mapping of UTC hours to distinct event counts"
    )
    parser.add_argument("--kafka-bootstrap")
    parser.add_argument("--event-topic", default="azki.user_events")
    parser.add_argument("--consumer-group", default="azki-events")
    args = parser.parse_args(argv)
    thresholds = load_thresholds(args.thresholds)
    counts = json.loads(args.source_counts.read_text()) if args.source_counts else None
    client = clickhouse_connect.get_client(host=args.host, port=args.port)
    try:
        results = run_checks(
            client,
            thresholds=thresholds,
            source_counts=counts,
            kafka_bootstrap=args.kafka_bootstrap,
            event_topic=args.event_topic,
            consumer_group=args.consumer_group,
        )
        for result in results:
            print(result.model_dump_json())
        return exit_code(results)
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
