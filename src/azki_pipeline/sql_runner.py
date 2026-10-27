import argparse
import re
from pathlib import Path
from typing import Protocol

import clickhouse_connect
import sqlglot
from sqlglot import exp

_SQL_DIR = Path(__file__).resolve().parents[2] / "sql"
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_NUMBERED_SQL = re.compile(r"(\d+)_.+\.sql\Z")
_ACCESS_NAME = r"[A-Za-z_][A-Za-z0-9_]*"
_SETTING_VALUE = r"(?:\d+|'QUERY_START')"
_PRIVILEGE = (
    r"(?:ALL|CURRENT GRANTS|SELECT|INSERT|ALTER|CREATE TABLE|CREATE VIEW|CREATE DICTIONARY|"
    r"DROP TABLE|DROP VIEW|DROP DICTIONARY|TRUNCATE|OPTIMIZE|DICTGET)"
)
_ACCESS_SQL = re.compile(
    rf"(?:CREATE ROLE IF NOT EXISTS {_ACCESS_NAME}"
    rf"|DROP (?:USER|ROLE|QUOTA|SETTINGS PROFILE) IF EXISTS {_ACCESS_NAME}"
    rf"|ALTER QUOTA {_ACCESS_NAME} TO {_ACCESS_NAME}(?:, {_ACCESS_NAME})*"
    rf"|GRANT {_PRIVILEGE}(?:, {_PRIVILEGE})* ON (?:\*|{_ACCESS_NAME})\.(?:\*|{_ACCESS_NAME})"
    rf" TO {_ACCESS_NAME}(?:, {_ACCESS_NAME})*(?: WITH GRANT OPTION)?"
    rf"|CREATE USER IF NOT EXISTS {_ACCESS_NAME} IDENTIFIED WITH no_password"
    rf" DEFAULT ROLE {_ACCESS_NAME}"
    rf"|CREATE ROW POLICY IF NOT EXISTS {_ACCESS_NAME} ON {_ACCESS_NAME}\.{_ACCESS_NAME}"
    rf" USING (?:partner_id = 'partner_[12]'|1) TO "
    rf"(?:ALL EXCEPT )?{_ACCESS_NAME}(?:, {_ACCESS_NAME})*"
    rf"|CREATE SETTINGS PROFILE IF NOT EXISTS {_ACCESS_NAME} SETTINGS "
    rf"(?:{_ACCESS_NAME} = {_SETTING_VALUE}(?: MIN \d+)?(?: MAX \d+| READONLY)?)(?:, "
    rf"{_ACCESS_NAME} = {_SETTING_VALUE}(?: MIN \d+)?(?: MAX \d+| READONLY)?)* TO {_ACCESS_NAME}"
    rf"|CREATE QUOTA IF NOT EXISTS {_ACCESS_NAME} KEYED BY user_name "
    rf"FOR INTERVAL 1 HOUR MAX queries = \d+ TO {_ACCESS_NAME})\Z",
    re.IGNORECASE,
)


def _override_access_identifiers(
    query: str, overrides: dict[str, str], *, include_databases: bool = False
) -> str:
    replacements = dict(overrides) if include_databases else {}
    isolated = overrides.get("gov")
    for token in sqlglot.tokenize(query, read="clickhouse"):
        if token.text.startswith(("role_", "azki_governance_")) and isolated and isolated != "gov":
            replacements[token.text] = f"{token.text}_{isolated}"
    for token in reversed(sqlglot.tokenize(query, read="clickhouse")):
        if token.token_type.name in {"VAR", "IDENTIFIER"} and token.text in replacements:
            query = query[: token.start] + replacements[token.text] + query[token.end + 1 :]
    return query


class SQLClient(Protocol):
    def command(self, query: str): ...


def _override_databases(statement: exp.Expression, overrides: dict[str, str]) -> None:
    for table in statement.find_all(exp.Table):
        if table.db in overrides:
            table.set("db", exp.to_identifier(overrides[table.db]))
        elif (
            isinstance(table.parent, exp.Create)
            and table.parent.args.get("kind") in {"SCHEMA", "DATABASE"}
            and table.name in overrides
        ):
            table.set("this", exp.to_identifier(overrides[table.name]))
    for column in statement.find_all(exp.Column):
        if column.db in overrides:
            column.set("db", exp.to_identifier(overrides[column.db]))
    for setting in statement.find_all(exp.DictSubProperty):
        if setting.this.name.upper() == "DB":
            value = setting.args.get("value")
            if isinstance(value, exp.Literal) and value.is_string and value.this in overrides:
                setting.set("value", exp.Literal.string(overrides[value.this]))
    for function in statement.find_all(exp.Anonymous):
        if function.name.lower().startswith("dict") and function.expressions:
            target = function.expressions[0]
            if isinstance(target, exp.Literal) and target.is_string:
                database, separator, name = target.this.partition(".")
                if separator and database in overrides:
                    target.set("this", f"{overrides[database]}.{name}")


def load_sql(path: str | Path, *, database_overrides: dict[str, str] | None = None) -> list[str]:
    """Parse a SQL file and safely override database identifiers and dictionary references."""
    overrides = database_overrides or {}
    for original, replacement in overrides.items():
        if not _IDENTIFIER.fullmatch(original) or not _IDENTIFIER.fullmatch(replacement):
            raise ValueError("Invalid database override identifier")
    queries = []
    for statement in sqlglot.parse(Path(path).read_text(encoding="utf-8"), read="clickhouse"):
        if statement is None:
            continue
        if isinstance(statement, exp.Command):
            query = statement.sql(dialect="clickhouse")
            if not _ACCESS_SQL.fullmatch(" ".join(query.split())):
                raise ValueError(f"Cannot execute unsupported SQL: {Path(path).name}")
            queries.append(_override_access_identifiers(query, overrides, include_databases=True))
            continue
        _override_databases(statement, overrides)
        query = statement.sql(dialect="clickhouse")
        if isinstance(statement, exp.Grant):
            query = _override_access_identifiers(query, overrides)
        queries.append(query)
    return queries


def apply_sql(
    client: SQLClient,
    sql_dir: str | Path = _SQL_DIR,
    *,
    database_overrides: dict[str, str] | None = None,
) -> int:
    """Execute numbered SQL files in numeric order; return the statement count."""
    overrides = database_overrides or {}
    for original, replacement in overrides.items():
        if not _IDENTIFIER.fullmatch(original) or not _IDENTIFIER.fullmatch(replacement):
            raise ValueError("Invalid database override identifier")
    directory = Path(sql_dir)
    if not directory.is_dir():
        raise FileNotFoundError(directory)
    files = []
    for path in directory.glob("*.sql"):
        match = _NUMBERED_SQL.fullmatch(path.name)
        if match:
            files.append((int(match[1]), path.name, path))
    applied = 0
    for _, _, path in sorted(files):
        for query in load_sql(path, database_overrides=overrides):
            client.command(query)
            applied += 1
    return applied


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Apply numbered ClickHouse deployment SQL")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8123)
    parser.add_argument("--sql-dir", type=Path, default=_SQL_DIR)
    args = parser.parse_args(argv)
    client = clickhouse_connect.get_client(host=args.host, port=args.port)
    try:
        applied = apply_sql(client, args.sql_dir)
        print(f"Applied {applied} SQL statements.")
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
