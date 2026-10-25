---
name: clickhouse-sql
description: Conventions and gotchas for ClickHouse DDL, materialized views, dictionaries, governance; how to test them.
---
# ClickHouse SQL

## Conventions
- Files in `sql/` are numbered, idempotent (`IF NOT EXISTS`), one concern per file, applied by `sql_runner.py` in order.
- Databases: `raw` (Kafka-engine landing), `core` (cleaned/deduped), `mart` (aggregates, denormalized), `dq` (monitoring), `gov` (masked views).
- Every MergeTree table states `PARTITION BY` (usually `toYYYYMM(event_date)`), an `ORDER BY` that starts with the most common filter columns, and `LowCardinality(String)` for low-cardinality strings (event_type, product_type, city).
- `core.user_events`: `event_key UInt64 MATERIALIZED cityHash64(user_id, session_id, event_time, event_type)`, `ReplacingMergeTree(ingested_at)`, `ORDER BY (event_type, user_id, event_time, session_id)`, `PARTITION BY toYYYYMM(event_time)`; keeps `_topic`, `_partition`, `_offset`, `ingested_at DEFAULT now()` for reconciliation. Settings: `input_format_skip_unknown_fields=1`, `date_time_input_format='best_effort'`.
- Columns are exactly those in `docs/data_contract.md`; there is no event_id or order_id in the source.

## Gotchas the tests must cover
- A Kafka-engine table is a stream; read it only via an MV. Never `SELECT` from it in tests.
- An MV fires only on INSERT into its **source** table. Rows in a joined table that arrive later do NOT update existing MV output → need a repair path (refreshable MV `REFRESH EVERY ...` or scheduled rebuild). Test this explicitly.
- Multiple MVs writing into one target table = the UNION pattern (one MV per product table).
- `ReplacingMergeTree` dedup is eventual. In tests use `SELECT ... FINAL` or `argMax(col, version)`; don't rely on merge timing in assertions (you may call `OPTIMIZE TABLE ... FINAL` explicitly in a test).
- AggregatingMergeTree: write `*State` in the MV, read with `*Merge` + `GROUP BY`. Late events simply add states to the right bucket.
- Duplicate Kafka deliveries inflate aggregate MVs → document and test the reconciliation/rebuild job.
- Kafka engine error handling: `kafka_handle_error_mode = 'stream'` exposes `_error` and `_raw_message`; route rows with non-empty `_error` to `dq.kafka_dead_letter` via a separate MV.
- Dictionaries for user enrichment (`dictGetOrDefault(core.users_dict, 'city', user_id, 'unknown')`): unknown user → `'unknown'`, measured by a DQ check. The sample data has NO overlap between event user_ids and users — seed users first and keep the aggregate rebuild job to re-enrich after users land.
- Purchase ↔ order join key is `(user_id, session_id)`; order tables are invented (see contract) and carry PII used by governance tests.

## Governance
Roles: `role_admin`, `role_engineer`, `role_analyst`, `role_partner_<x>`. Column-level `GRANT SELECT(col, ...)`; PII (phone, national_id, email…) only via `gov.*` masked views for analysts; `CREATE ROW POLICY` for partner-scoped rows; settings profiles + quotas for analysts; audit via `system.query_log`.

## Testing
- Contract layer: parse every file with `sqlglot` (dialect `clickhouse`) and assert structural rules (engine present, ORDER BY present, no `SELECT *` in MVs, no PII column granted to `role_analyst`).
- Integration layer: apply all SQL to a Testcontainers ClickHouse, INSERT directly into `core` tables (bypassing Kafka) to test MV logic fast; separate tests go through Kafka.
