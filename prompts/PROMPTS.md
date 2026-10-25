# Codex prompts — run one phase per session, in order

Each prompt assumes Codex has read `AGENTS.md`. Paste one phase, review the diff, commit, then move on.

---

## Phase 0 — Scaffold + data contract (no business logic)
```
Read AGENTS.md and .codex/skills/tdd-workflow/SKILL.md.
1. Create the repository layout from AGENTS.md with empty modules, pyproject.toml (uv, pytest markers:
   unit, contract, integration, e2e; strict markers), Makefile targets, ruff config, .gitignore (data/ ignored).
2. Read docs/data_contract.md (already written from the real headers). Confirm against data/*.csv headers if present;
   do not copy real rows into the repo.
3. Write contracts/user_event.schema.json and contracts/user.schema.json from the contract (event_type enum, premium_amount >= 0).
4. Synthetic fixtures already exist in tests/fixtures/ (users 1–9 + events over those users, one unknown user,
   one exact duplicate row, one late event). Extend only if a test needs more.
5. Implement `event_key()` in Python so it matches ClickHouse `cityHash64(user_id, session_id, event_time, event_type)`
   (use the `clickhouse-cityhash` package; U20 pins a known vector).
6. Add tests/helpers/eventually.py and factories.py, with their own unit tests.
Only tests that exist so far should run; `make test-unit` and `make test-contract` must be green (C01 included).
```

## Phase 1 — Contracts & producer (U01–U06, U19, C10)
```
Use skills: tdd-workflow, kafka-connect.
Implement TEST_PLAN IDs U01–U06, U19, U20 strictly test-first: write all tests, run them, show red,
then implement src/azki_pipeline/contracts.py, producer.py and sql_runner.py until green.
Producer: CSV → validated JSON → Kafka key user_id; invalid rows collected with reason, never raised.
```

## Phase 2 — Compose stack + Connect configs (C02–C04, C09, E01)
```
Use skills: kafka-connect, integration-testing.
Write contract tests C02, C03, C09 first (C04 will be red until Phase 3 SQL exists — mark xfail(strict=True)
with reason "SQL lands in phase 3").
Then build docker/compose.yml: Kafka 3-broker KRaft (or 1 broker with a COMPOSE_PROFILE for 3), Schema Registry,
Kafka Connect (custom image with Debezium MySQL + spooldir), Kafka UI, MySQL 8 (binlog ROW, GTID, init scripts
loading users from tests/fixtures/users.csv and invented product/financial tables keyed by (user_id, session_id, created_at) with PII columns national_id, phone, plate_no, policy_no), ClickHouse. Healthchecks on every service,
pinned images. Write connect/debezium-mysql-source.json and connect/spooldir-user-events.json and
connect_client.py (validate + register + wait RUNNING). Then E01.
```

## Phase 3 — Part 1 ClickHouse pipeline (C05–C07, I01–I05, I11–I13, E02, E03)
```
Use skills: clickhouse-sql, integration-testing.
Test-first: write C05–C07 and I01–I05 (direct inserts into core tables), show red, then write
sql/001–040. Design:
- raw.kafka_user_events (Kafka engine, JSONEachRow, kafka_handle_error_mode='stream', input_format_skip_unknown_fields=1)
  → MV → core.user_events(event_time DateTime, user_id Int64, session_id String, event_type LowCardinality(String),
  channel LowCardinality(String), premium_amount Int64, event_key UInt64 MATERIALIZED cityHash64(...),
  _partition, _offset, ingested_at) ReplacingMergeTree(ingested_at) ORDER BY (event_type, user_id, event_time, session_id)
  and a second MV → dq.kafka_dead_letter for rows with _error.
- raw.kafka_users (Debezium topic) → MV → core.users (ReplacingMergeTree(_version, _is_deleted)).
- dictionary core.users_dict over the latest non-deleted users.
- mart.user_event_agg (AggregatingMergeTree) by event_date, event_type, channel, city, device_type (via
  dictGetOrDefault, unknown → 'unknown'): countState, uniqState(user_id), uniqState(session_id),
  sumState(premium_amount), avgState(premium_amount).
- a reconciliation statement/job rebuilding an aggregate partition from deduped core.user_events (for I13).
Then I11–I13 through real Kafka, then E02–E03. Remove the xfail on C04.
```

## Phase 4 — Part 2 denormalized purchases (I06–I09)
```
Use skill: clickhouse-sql.
Write I06–I09 first. Then sql/050–060:
- 4 MVs (third, body, medical, fire CDC tables) → core.orders_unified with a product_type column
  and a common schema (map product-specific columns to a common set + a Map/JSON column for extras).
- core.financial_order (ReplacingMergeTree) from CDC.
- mart.purchase_events_enriched: MV on core.user_events WHERE event_type='purchase', joining orders_unified
  and financial_order on (user_id, session_id) — events carry no order_id (see docs/data_contract.md) (prefer dictionaries / pre-joined order table over large JOINs in the MV; justify in report).
- Repair path for orders arriving after the event: refreshable MV (REFRESH EVERY N MINUTE) recomputing
  recent partitions. I09 must prove it.
Add projections / skip indexes where a test or EXPLAIN shows benefit; record EXPLAIN output in docs/report.md.
```

## Phase 5 — Governance (C08, I10)
```
Use skill: clickhouse-sql.
Write C08 and I10 first. I10 must connect as each role and assert: analyst gets masked PII via gov.* views and
is denied on raw PII columns; partner role sees only its rows (row policy); analyst quota/settings profile
limits are active; admin sees everything. Then write sql/070_governance.sql.
```

## Phase 6 — Data quality (U07–U14, I14, E05)
```
Use skill: data-quality.
Write unit tests U07–U14 for pure evaluate() functions first (boundary values via parametrize), then implement
dq/checks.py, models.py, runner.py and thresholds.yaml. Then I14 for the SQL collectors (sql/080_monitoring.sql),
then E05 (inject a new field → drift WARN, pipeline continues).
```

## Phase 7 — Spark backfill (U15–U18, I15–I16, E04, E06)
```
Use skill: spark-backfill.
Write U15–U18 with a local SparkSession fixture first; implement backfill/transforms.py. Then I15–I16 against
Testcontainers ClickHouse (idempotency: second run writes 0 rows), implement backfill/job.py CLI.
Finish with E04 and E06 (healthy → delete an hour → DQ FAIL → backfill → DQ PASS).
```

## Phase 8 — Report & diagram
```
Write docs/architecture.mmd (Mermaid: sources → Kafka/Connect → ClickHouse raw/core/mart/dq/gov → consumers,
plus Spark backfill and DQ runner) and docs/report.md (2–4 pages): architecture choices and trade-offs
(ClickHouse-native vs Spark/Flink streaming, MV limitations and the repair path, duplicate handling,
dictionary vs JOIN), performance optimizations with evidence, governance model, DQ plan table,
backfill strategy, what I'd do in production (replication/sharding, Keeper, schema registry formats,
Airflow scheduling of DQ/backfill), and a test-pyramid summary with counts per layer.
Do not invent benchmark numbers — only report what was measured.
```

---

## Reusable mini-prompts
- **Stuck red test:** `Test <ID> fails. Diagnose root cause before changing anything. If the test is wrong, explain why and propose the change; don't edit it until I agree.`
- **Review pass:** `Review the diff of this phase against AGENTS.md and the relevant skill. List violations (tests weakened, sleeps, DDL built in Python, missing IDs). Fix them test-first.`
- **Coverage gap:** `List behaviours in sql/ and src/ not covered by any TEST_PLAN ID. Propose new IDs; don't implement yet.`
