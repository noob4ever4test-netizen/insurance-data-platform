---
name: kafka-connect
description: Kafka (KRaft) Compose stack, Debezium MySQL source, spooldir CSV source; config conventions and tests.
---
# Kafka & Connect

## Topics
- `azki.user_events` — events, key = `user_id` (per-user ordering), JSON values.
- `mysql.azki.users`, `mysql.azki.third_orders`, `...body_orders`, `...medical_orders`, `...fire_orders`, `...financial_order` — Debezium.
- `connect-*` internal topics; `dlq.*` for Connect errors.

## Debezium MySQL
- MySQL must run with `binlog_format=ROW`, `binlog_row_image=FULL`, GTID on; dedicated user with REPLICATION SLAVE/CLIENT, SELECT.
- Use the `ExtractNewRecordState` SMT with rewrite delete handling so ClickHouse gets flat rows + a deleted flag; keep source `ts_ms` as the ReplacingMergeTree version (users has no `updated_at`). Enable Debezium heartbeat topic for lag measurement.
- Secrets via `${file:/opt/secrets/mysql.properties:password}` (FileConfigProvider), never inline.

## Spooldir (user_events.csv)
- `SpoolDirCsvSourceConnector` with explicit value schema `event_time STRING, user_id INT64, session_id STRING, event_type STRING, channel STRING, premium_amount INT64`, key = `user_id`, `errors.tolerance=all` + DLQ topic, `finished.path`/`error.path`. ClickHouse parses `event_time` with `best_effort`.

## Tests
- Contract: every JSON in `connect/` parses; required keys present; converters consistent; topic names match `kafka_topic_list` in `sql/*.sql` (cross-file consistency test); no plaintext passwords.
- Integration/e2e: PUT config to Connect REST `/connector-plugins/<class>/config/validate` → zero errors; then register and poll `/connectors/<n>/status` until RUNNING.
