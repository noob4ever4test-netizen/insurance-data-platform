---
name: integration-testing
description: Testcontainers fixtures, polling helper, and e2e compose conventions.
---
# Integration & E2E testing

## Fixtures (`tests/conftest.py`)
- Session-scoped containers: `clickhouse`, `kafka` (KRaft image), `mysql`. Start lazily — only when a test with that marker runs.
- Function-scoped `ch_db`: creates `test_<uuid>` databases by applying `sql/` with a database-name override, drops after.
- `kafka_topic`: unique topic per test.
- Skip the whole integration layer with a clear message if Docker isn't available.

## Polling
`eventually(fn, timeout=30, interval=0.5)` retries until `fn()` doesn't raise; on timeout re-raises the last AssertionError. Kafka-engine MVs flush on `kafka_flush_interval_ms` — set it low (e.g. 500) in test SQL settings.

## E2E (`tests/e2e/`)
- `make up` → `make seed` (MySQL init + drop synthetic CSV into spooldir) → register connectors → assert.
- Fault-injection scenarios live here: stop Connect then resume (no loss), push a malformed message (dead-letter), duplicate events (reconciliation), late user insert (repair MV), schema change (drift check fires).
- Always dump `docker compose logs` on failure (pytest hook).
