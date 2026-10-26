import json
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

ROOT = Path(__file__).resolve().parents[2]
PHASE3 = sorted((ROOT / "sql").glob("0[0-4]*.sql"))


@pytest.mark.contract
def test_C05_phase3_sql_is_present_and_parses():
    for path in PHASE3:
        assert path.read_text().strip(), f"Missing deployment SQL: {path.name}"
    for path in (ROOT / "sql").rglob("*.sql"):
        if path.parent.name == "mysql":
            continue
        statements = sqlglot.parse(path.read_text(), read="clickhouse")
        assert not any(isinstance(statement, exp.Command) for statement in statements), path


@pytest.mark.contract
def test_C06_storage_and_stream_error_settings():
    creates = [
        statement
        for path in PHASE3
        for statement in sqlglot.parse(path.read_text(), read="clickhouse")
        if isinstance(statement, exp.Create)
    ]
    assert len(creates) >= 15
    for statement in creates:
        text = statement.sql(dialect="clickhouse").lower()
        if "mergetree" in text:
            assert "order by" in text
            assert "partition by" in text
        if "engine=kafka" in text.replace(" ", ""):
            assert "kafka_handle_error_mode = 'stream'" in text
            assert "input_format_skip_unknown_fields = 1" in text


@pytest.mark.contract
def test_C07_materialized_views_use_explicit_columns():
    views = [
        statement
        for path in PHASE3
        for statement in sqlglot.parse(path.read_text(), read="clickhouse")
        if isinstance(statement, exp.Create) and statement.args.get("kind") == "VIEW"
    ]
    assert len(views) >= 5
    for view in views:
        assert not list(view.find_all(exp.Star)), view.sql()


@pytest.mark.contract
def test_C07_aggregate_view_exists_before_event_consumption_is_enabled():
    deployment = "\n".join(path.read_text() for path in PHASE3)

    assert deployment.index("VIEW IF NOT EXISTS core.user_event_agg_mv") < deployment.index(
        "VIEW IF NOT EXISTS raw.user_events_mv"
    )


@pytest.mark.contract
@pytest.mark.parametrize("table_name", ["core.user_events", "raw.kafka_user_events"])
def test_C10_event_table_matches_contract_types(table_name):
    statements = sqlglot.parse((ROOT / "sql" / "010_raw_events.sql").read_text(), "clickhouse")
    table = next(
        (
            statement
            for statement in statements
            if isinstance(statement, exp.Create)
            and isinstance(statement.this, exp.Schema)
            and statement.this.this.sql() == table_name
        ),
        None,
    )
    assert table is not None, f"{table_name} deployment DDL is missing"
    columns = {
        column.name: column.args["kind"].sql("clickhouse")
        for column in table.this.expressions
        if isinstance(column, exp.ColumnDef)
    }
    schema = json.loads((ROOT / "contracts" / "user_event.schema.json").read_text())
    assert set(schema["required"]) <= columns.keys()
    assert columns["user_id"] == "Int64"
    assert columns["premium_amount"] == "Int64"
    assert columns["event_time"].startswith("DateTime")
    assert columns["session_id"] == "String"
    assert columns["event_type"] == "LowCardinality(String)"
    assert columns["channel"] == "LowCardinality(String)"
