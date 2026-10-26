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
        if overrides and isinstance(statement, exp.Command):
            raise ValueError(
                f"Cannot safely override databases in unsupported SQL: {Path(path).name}"
            )
        _override_databases(statement, overrides)
        queries.append(statement.sql(dialect="clickhouse"))
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
