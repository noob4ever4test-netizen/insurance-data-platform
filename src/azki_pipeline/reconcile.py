import argparse
from pathlib import Path

import clickhouse_connect

from azki_pipeline.sql_runner import load_sql

JOBS = Path(__file__).resolve().parents[2] / "sql" / "jobs"


def rebuild_aggregate_partition(client, partition: int, *, database_overrides=None) -> None:
    """Rebuild one month; pause event writers and serialize calls before running."""
    if (
        type(partition) is not int
        or not 190001 <= partition <= 229912
        or not 1 <= partition % 100 <= 12
    ):
        raise ValueError("partition must be an integer YYYYMM")
    overrides = database_overrides or {}
    parameters = {"partition": partition}

    def queries(filename):
        return iter(load_sql(JOBS / filename, database_overrides=overrides))

    for query in queries("001_stage_user_event_partition.sql"):
        client.command(query, parameters=parameters)
    count_query = next(queries("002_count_user_event_partition.sql"))
    populated = client.query(count_query, parameters=parameters).result_rows[0][0]
    filename = (
        "003_replace_user_event_partition.sql" if populated else "004_drop_user_event_partition.sql"
    )
    client.command(next(queries(filename)), parameters=parameters)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild a deduped aggregate month with writers paused"
    )
    parser.add_argument("partition", type=int, help="Month as YYYYMM")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8123)
    args = parser.parse_args(argv)
    client = clickhouse_connect.get_client(host=args.host, port=args.port)
    try:
        rebuild_aggregate_partition(client, args.partition)
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
