UV ?= $(if $(wildcard .venv/bin/uv),.venv/bin/uv,uv)
COMPOSE = docker compose -f docker/compose.yml

.PHONY: prepare up down apply-sql seed dq backfill test-unit test-contract test-integration test-e2e lint

prepare:
	$(UV) run python -m azki_pipeline.seed --prepare-only

up: prepare
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) --profile three-brokers down

seed:
	$(UV) run python -m azki_pipeline.seed

apply-sql:
	$(UV) run python -m azki_pipeline.sql_runner

dq:
	$(UV) run python -m azki_pipeline.dq.runner --kafka-bootstrap 127.0.0.1:9092

backfill:
	$(UV) run --extra spark python -m azki_pipeline.backfill.job $(ARGS)

test-unit:
	$(UV) run --extra spark pytest tests/unit -m unit -q

test-contract:
	$(UV) run pytest tests/contract -m contract -q

test-integration:
	$(UV) run --extra integration --extra spark pytest tests/integration -m integration -q

test-e2e:
	$(UV) run --extra integration --extra spark pytest tests/e2e -m e2e -q

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run python -c 'from pathlib import Path; from azki_pipeline.sql_runner import load_sql; [load_sql(path) for path in sorted(Path("sql").glob("*.sql")) if path.read_text().strip()]'
