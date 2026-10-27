import json
import sys
from datetime import datetime
from urllib.parse import urlparse

import clickhouse_connect

from azki_pipeline.backfill.job import create_spark, run_backfill


def main():
    options = json.load(sys.stdin)
    endpoint = urlparse(options.pop("clickhouse_url"))
    options["start"] = datetime.fromisoformat(options["start"])
    options["end"] = datetime.fromisoformat(options["end"])
    client = clickhouse_connect.get_client(host=endpoint.hostname, port=endpoint.port)
    spark = create_spark(kafka=True)
    try:
        summaries = [run_backfill(spark, client, **options) for _ in range(2)]
        print(json.dumps(summaries))
    finally:
        spark.stop()
        client.close()


if __name__ == "__main__":
    main()
