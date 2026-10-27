from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import sqlglot

from azki_pipeline.sql_runner import apply_sql, load_sql

FIXTURES = Path(__file__).resolve().parents[2] / "sql" / "test_fixtures"


@pytest.mark.unit
def test_U19_governance_overrides_preserve_literals_and_isolate_roles():
    path = FIXTURES.parent / "070_governance.sql"

    queries = load_sql(path, database_overrides={"gov": "gov_test", "mart": "mart_test"})

    assert "CREATE ROLE IF NOT EXISTS role_partner_1_gov_test" in queries
    assert any(
        "azki_governance_partner_1_gov_test ON mart_test.purchase_events_enriched" in query
        and "partner_id = 'partner_1'" in query
        and "TO role_partner_1_gov_test" in query
        for query in queries
    )


@pytest.mark.unit
def test_U19_governance_isolation_preserves_column_aliases_and_role_literals():
    queries = load_sql(
        FIXTURES / "governance/073_identifier_probe.sql",
        database_overrides={"core": "core_test", "gov": "gov_test"},
    )

    assert queries == ["SELECT 1 AS core, 'role_analyst' AS label FROM core_test.runner_probe"]


@pytest.mark.unit
def test_U19_unsupported_commands_still_rejected(tmp_path):
    path = tmp_path / "070_access.sql"
    path.write_text("CREATE SOMETHING arbitrary")

    with pytest.raises(ValueError, match="unsupported SQL"):
        load_sql(path, database_overrides={"gov": "gov_test"})


@pytest.mark.unit
def test_U19_numbered_files_are_applied_in_numeric_order(tmp_path):
    (tmp_path / "10_table.sql").write_bytes((FIXTURES / "010_table.sql").read_bytes())
    (tmp_path / "2_database.sql").write_bytes((FIXTURES / "002_database.sql").read_bytes())
    (tmp_path / "001_empty.sql").write_text("")
    client = Mock()

    count = apply_sql(client, tmp_path)

    queries = [call.args[0] for call in client.command.call_args_list]
    assert count == 3
    assert sqlglot.parse_one(queries[0], read="clickhouse").args["kind"] == "SCHEMA"
    assert "CREATE TABLE" in queries[1]
    assert queries[2].startswith("SELECT")
    assert "core.runner_probe; unchanged" in queries[1]
    assert "core.runner_probe; unchanged" in queries[2]


@pytest.mark.unit
def test_U19_database_override_rewrites_identifiers_but_preserves_literals():
    client = Mock()

    count = apply_sql(client, FIXTURES, database_overrides={"core": "core_unit"})

    queries = [call.args[0] for call in client.command.call_args_list]
    assert count == 5
    assert queries[0] == "CREATE DATABASE IF NOT EXISTS core_unit"
    assert "core_unit.runner_probe" in queries[1]
    assert "'core.runner_probe; unchanged'" in queries[1]
    assert "FROM core_unit.runner_probe" in queries[2]
    assert "core_unit.runner_probe_dict" in queries[3]
    assert "DB 'core_unit'" in queries[3]
    assert "'core_unit.runner_probe_dict'" in queries[4]


@pytest.mark.unit
@pytest.mark.parametrize("replacement", ["core; DROP DATABASE core", "core.name", ""])
def test_U19_invalid_database_override_is_rejected(replacement):
    client = Mock()

    with pytest.raises(ValueError, match="database"):
        apply_sql(client, FIXTURES, database_overrides={"core": replacement})

    client.command.assert_not_called()


@pytest.mark.unit
def test_U19_server_errors_stop_execution():
    client = Mock()
    client.command.side_effect = RuntimeError("server rejected statement")

    with pytest.raises(RuntimeError, match="server rejected"):
        apply_sql(client, FIXTURES)

    assert client.command.call_count == 1


@pytest.mark.unit
def test_U19_cli_deploys_and_closes_client():
    from azki_pipeline.sql_runner import main

    with patch("azki_pipeline.sql_runner.clickhouse_connect.get_client") as connect:
        assert main(["--sql-dir", str(FIXTURES), "--port", "18123"]) == 0

    connect.assert_called_once_with(host="127.0.0.1", port=18123)
    assert connect.return_value.command.call_count == 5
    connect.return_value.close.assert_called_once_with()
