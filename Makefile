UV ?= $(if $(wildcard .venv/bin/uv),.venv/bin/uv,uv)
COMPOSE = docker compose -f docker/compose.yml

.PHONY: prepare up down seed test-unit test-contract test-integration test-e2e lint

prepare:
	$(UV) run python -m azki_pipeline.seed --prepare-only

up: prepare
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) --profile three-brokers down

seed:
	$(UV) run python -m azki_pipeline.seed

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
	$(UV) run python -c 'from pathlib import Path; import sqlglot; [sqlglot.parse(path.read_text(), read="clickhouse") for path in sorted(Path("sql").glob("*.sql")) if path.read_text().strip()]'
