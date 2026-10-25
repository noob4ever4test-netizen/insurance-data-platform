import re
from pathlib import Path
from typing import Protocol

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
        for statement in sqlglot.parse(path.read_text(encoding="utf-8"), read="clickhouse"):
            if statement is None:
                continue
            if overrides and isinstance(statement, exp.Command):
                raise ValueError(
                    f"Cannot safely override databases in unsupported SQL: {path.name}"
                )
            _override_databases(statement, overrides)
            client.command(statement.sql(dialect="clickhouse"))
            applied += 1
    return applied
