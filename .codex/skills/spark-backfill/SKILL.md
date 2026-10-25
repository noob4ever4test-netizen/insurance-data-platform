---
name: spark-backfill
description: PySpark backfill job conventions — idempotent, partition-scoped, testable transforms.
---
# Spark backfill

## Shape
- `backfill/transforms.py`: pure DataFrame → DataFrame functions (parse, validate against contract, dedupe by `event_key` (hash of user_id, session_id, event_time, event_type — same function as ClickHouse `cityHash64`), filter to date range, find missing keys via left-anti join with what ClickHouse already has).
- `backfill/job.py`: CLI `--source {csv,kafka} --start --end --mode {fill-gaps,replace-partition} --dry-run`.
  - `kafka` source reads a bounded offset/timestamp range (`startingOffsetsByTimestamp`).
  - `fill-gaps`: insert only rows missing in ClickHouse.
  - `replace-partition`: write to `core.user_events_staging`, then `ALTER TABLE core.user_events REPLACE PARTITION ... FROM core.user_events_staging`; then rebuild affected aggregate partitions.
- Idempotency is a requirement: running twice = same result.
- Emits a summary (rows read / invalid / missing / written) and a `dq.backfill_runs` row.

## Tests
- Unit: `spark` fixture = `SparkSession.builder.master("local[1]").config("spark.sql.shuffle.partitions", 1)`, session-scoped. Test each transform with tiny DataFrames.
- Integration: run job against Testcontainers ClickHouse; assert gap filled, second run is a no-op, aggregates rebuilt.
