---
name: data-quality
description: Design of DQ checks (freshness/lag, completeness, schema drift, load) as pure-Python logic over metrics queried from ClickHouse.
---
# Data quality

## Design
- `dq/models.py`: `CheckResult(name, status: PASS|WARN|FAIL, observed, threshold, details, checked_at)`.
- Each check = **pure function** `evaluate(metrics, thresholds) -> CheckResult` (unit-tested) + a small **collector** that runs SQL to get metrics (integration-tested). Never mix the two.
- `dq/runner.py` runs all checks, writes results to `dq.check_results` (MergeTree), exits non-zero on FAIL (CI/Airflow friendly).
- Thresholds in `dq/thresholds.yaml`.

## Checks
| Area | Check | Metric source |
|---|---|---|
| Sync/delay | consumer lag | `system.kafka_consumers` / Kafka admin offsets |
| Sync/delay | freshness | `now() - max(event_time)` and `max(ingested_at)` in `core.user_events` |
| Sync/delay | CDC lag | Debezium heartbeat `ts_ms` vs `now()`; users has no `updated_at` |
| Missing events | offset gaps | per partition: `max(_offset) - min(_offset) + 1` vs `count()` |
| Missing events | source reconciliation | rows per hour: CSV/Kafka vs ClickHouse |
| Missing events | volume anomaly | hourly count vs trailing 7-day same-hour median (± k·MAD) |
| Missing events | orphan ratio | share of events with city = `'unknown'` (user not in dictionary) — FAIL only above a threshold once users are loaded |
| Schema drift | contract diff | keys in sampled `_raw_message` vs `contracts/*.json` (added/removed/type change) |
| Schema drift | dead-letter rate | rows in `dq.kafka_dead_letter` per hour |
| Load | insert health | `system.parts` active parts per partition, `system.part_log` insert rate/errors |
| Load | duplicates | `count() - uniqExact(event_key)` |
| Load | purchase enrichment | share of purchase events without order match |
