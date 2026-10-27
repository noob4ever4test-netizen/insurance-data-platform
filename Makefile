UV ?= $(if $(wildcard .venv/bin/uv),.venv/bin/uv,uv)
COMPOSE = docker compose -f docker/compose.yml

.PHONY: prepare up down apply-sql seed dq test-unit test-contract test-integration test-e2e lint

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

test-unit:
	$(UV) run pytest tests/unit -m unit -q

test-contract:
	$(UV) run pytest tests/contract -m contract -q

test-integration:
	$(UV) run --extra integration pytest tests/integration -m integration -q

test-e2e:
	$(UV) run --extra integration pytest tests/e2e -m e2e -q

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run python -c 'from pathlib import Path; from azki_pipeline.sql_runner import load_sql; [load_sql(path) for path in sorted(Path("sql").glob("*.sql")) if path.read_text().strip()]'
