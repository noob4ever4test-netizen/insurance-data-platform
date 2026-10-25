# AGENTS.md — Azki Data Engineering Task (TDD)

## Mission
Build a ClickHouse-native streaming pipeline, test-first:
- **Part 1**: `user_events.csv` → Kafka → ClickHouse (Kafka engine + MVs), joined with MySQL `users`
  (replicated by Debezium) and aggregated into an `AggregatingMergeTree`.
- **Part 2**: denormalized `purchase_events_enriched` table built with MVs, unioning 4 product tables
  (third, body, medical, fire) and joining `financial_order`. Plus performance + governance (RBAC, row policies, masking).
- **Part 3**: data-quality checks (lag/delay, missing events, schema drift, load) + PySpark backfill job.
- **Bonus**: full Compose stack (Kafka KRaft, Schema Registry, Kafka Connect + Debezium + spooldir, Kafka UI, MySQL, ClickHouse).

## Non-negotiable rules
1. **Tests first.** For every task: write/extend tests → run them and show they FAIL for the right reason → implement → show GREEN → refactor.
2. Never weaken, skip or delete a test to make it pass. If a test is wrong, say so explicitly and explain the change.
3. Work one phase at a time (see `prompts/PROMPTS.md`). Stop at the end of a phase and summarize.
4. Every test case ID in `docs/TEST_PLAN.md` must map to a test function: name tests `test_<ID>_<short_desc>` (e.g. `test_U03_event_key_is_user_id`).
5. No `time.sleep` in tests — use the `eventually()` polling helper in `tests/helpers/`.
6. **Confidential data**: real CSVs live in `data/` (gitignored). Tests use synthetic fixtures in `tests/fixtures/` that mimic the real headers. Never commit real rows.
7. All SQL lives in `sql/` as numbered, idempotent files (`CREATE ... IF NOT EXISTS`). Python never builds DDL by string concat.

## Stack
Python 3.11, pytest, `clickhouse-connect`, `confluent-kafka`, `testcontainers`, `pyspark`, `pydantic`, `jsonschema`, `sqlglot`, `ruff`. Dependency management with `uv`.

## Repository layout
```
.
├── AGENTS.md
├── Makefile                      # up, down, seed, test-unit, test-contract, test-integration, test-e2e, lint
├── pyproject.toml                # pytest markers: unit, contract, integration, e2e
├── docker/
│   ├── compose.yml               # kafka (KRaft), schema-registry, connect, kafka-ui, mysql, clickhouse
│   ├── connect/Dockerfile        # installs debezium-mysql + spooldir plugins
│   ├── mysql/init/               # schema + load users.csv & product/financial tables
│   └── clickhouse/{config,users}.d/
├── connect/
│   ├── debezium-mysql-source.json
│   └── spooldir-user-events.json
├── contracts/
│   ├── user_event.schema.json
│   └── user.schema.json
├── sql/
│   ├── 001_databases.sql
│   ├── 010_raw_events.sql        # Kafka engine + MV → raw events (ReplacingMergeTree) + dead-letter
│   ├── 020_users_cdc.sql         # Kafka engine on Debezium topic → users ReplacingMergeTree
│   ├── 030_dictionaries.sql      # users dictionary (latest version)
│   ├── 040_agg_part1.sql         # AggregatingMergeTree + MV (event × user attributes)
│   ├── 050_orders_union.sql      # 4 product tables → orders_unified via 4 MVs
│   ├── 060_purchase_enriched.sql # denormalized table + MV + refreshable repair MV
│   ├── 070_governance.sql        # roles, grants, row policies, masked views, quotas
│   └── 080_monitoring.sql        # views over system.* for DQ
├── src/azki_pipeline/
│   ├── contracts.py              # load/validate schemas, schema diff
│   ├── producer.py               # CSV → Kafka (test/fallback path)
│   ├── connect_client.py         # register/validate connectors via REST
│   ├── sql_runner.py             # apply sql/ in order
│   ├── dq/{checks.py,models.py,runner.py}
│   └── backfill/{job.py,transforms.py}
├── tests/
│   ├── conftest.py
│   ├── helpers/{eventually.py,factories.py}
│   ├── fixtures/                 # synthetic CSVs
│   ├── unit/  contract/  integration/  e2e/
├── docs/
│   ├── TEST_PLAN.md
│   ├── architecture.mmd
│   └── report.md                 # 2–4 page technical report
└── prompts/PROMPTS.md
```

## Commands
- `make test-unit` — no Docker, < 30 s
- `make test-contract` — no Docker, parses configs/SQL/schemas
- `make test-integration` — Testcontainers (ClickHouse, Kafka, MySQL)
- `make test-e2e` — full `docker compose` stack
- `make lint` — ruff + sqlglot parse of all `sql/*.sql`

## Skills
Read the matching skill before working in that area: `.codex/skills/<name>/SKILL.md`
- `tdd-workflow` (always) · `clickhouse-sql` · `kafka-connect` · `integration-testing` · `data-quality` · `spark-backfill`
