# Local streaming stack

Run from the repository root with Docker Desktop or Docker Engine and Compose v2:

```sh
uv sync
make up
make apply-sql
make seed
make test-e2e
```

`make up` prepares local secrets and synthetic order CSVs, builds Connect with pinned
Debezium MySQL and spooldir plugins, and waits for every default service's healthcheck.
The first MySQL startup creates the six source tables and loads synthetic data. Its
initialization directory mounts the numbered SQL from `sql/mysql/` directly. The Debezium user
has snapshot/replication privileges and SELECT access to the source database.

`make seed` validates and registers the MySQL connector, waits until all fixture users
have appeared on `mysql.azki.users` and in ClickHouse with matching dictionary dimensions,
then registers spooldir and stages the synthetic
event CSV by an atomic rename. Each seed invocation replays the event file; it does
not deduplicate source events. Apply SQL before seeding so downstream materialized views
exist before the first Kafka records arrive. The event forwarding views are activated
in `040_agg_part1.sql`, after the aggregate view has been installed.

Dictionary refreshes occur every 1–5 seconds; events from independent writers that arrive
before their users use `unknown` dimensions and require reconciliation. The seed command
waits for both the users and dictionary before staging its CSV, and fails with a bounded
timeout if they are incomplete. E02 uses the same readiness helper. Use `--clickhouse-host`
and `--clickhouse-port` when the HTTP endpoint differs from `127.0.0.1:8123`.

| Service | Host address | Container address |
|---|---|---|
| Kafka | `127.0.0.1:9092` | `kafka:29092` |
| Schema Registry | `http://localhost:8081` | `http://schema-registry:8081` |
| Connect | `http://localhost:8083` | `http://connect:8083` |
| Kafka UI | `http://localhost:8080` | `http://kafka-ui:8080` |
| MySQL | `localhost:3306` | `mysql:3306` |
| ClickHouse HTTP / native | `localhost:8123` / `localhost:9000` | `clickhouse:8123` / `clickhouse:9000` |

Host ports bind to loopback. Internal and host Kafka listeners are separate so both
Connect and Python clients receive usable broker metadata.
Host Kafka listeners advertise IPv4 loopback explicitly, matching Docker's port bindings
even on hosts where `localhost` resolves to IPv6 first.

The default cluster has one combined KRaft controller/broker. To add two brokers:

```sh
docker compose -f docker/compose.yml --profile three-brokers up -d --wait
```

The additional host listeners are `127.0.0.1:9094` and `127.0.0.1:9096`. This profile
keeps the original single controller and replication factor 1; it is a local scaling
option. `make down` stops both default and profile services and preserves volumes.

Credentials are generated once under `docker/secrets/` with mode 0600 and are ignored
by Git. Connect reads its password through FileConfigProvider; committed connector
JSON contains only a file reference. Keep these files with the persistent MySQL volume
because the MySQL entrypoint applies initial credentials only on the first startup.
`docker/generated/` is also ignored: it contains deterministic synthetic order and
financial CSVs derived from purchase rows in `tests/fixtures/user_events.csv`. All
four product types get matching session/time keys and clearly synthetic policy numbers.
Real files under `data/` are never mounted by this stack.

Spooldir uses explicit Connect schemas, extracts the numeric user ID as its Kafka
key, and writes schema-free JSON values. Its input, finished, and error directories
share a persistent volume. Parser failures can quarantine files in `error.path`;
Connect error-tolerance and DLQ settings are also explicit in the connector config.
The file parser's error directory is the reliable quarantine for spooldir parsing errors;
source connector DLQ behavior should not be assumed to cover every file-level error.
Debezium emits flattened records, source/envelope timestamp metadata, and rewritten
delete flags. The users Kafka table accepts Debezium DATE values as days since the Unix
epoch, converts them to Date, and uses `__ts_ms` as the version and `__deleted` as the
tombstone. Users live in one partition so an attribute change cannot split user versions.
The dictionary reads a FINAL, non-deleted view. Order-table conversion is part of Phase 4.

Events use UTC DateTime, a materialized `cityHash64` key, and ReplacingMergeTree with
microsecond ingestion timestamps. Query `core.user_events FINAL` for deduplicated results.
The aggregate MV counts inserts, including duplicate deliveries. Read aggregate states
with `countMerge`, `uniqMerge`, `sumMerge`, and `avgMerge`, grouping by all five dimensions.
Malformed JSON and type errors are routed to `dq.kafka_dead_letter` with the original
message, error, topic, partition, and offset. Additional JSON fields are accepted.

To reconcile one month, stop the two ClickHouse Kafka tables and all direct event writers,
reload the dictionary, rebuild the partition, then reattach both tables. For example,
with no direct writers active:

```sh
docker compose -f docker/compose.yml exec -T clickhouse clickhouse-client --multiquery --query \
  'DETACH TABLE raw.kafka_user_events; DETACH TABLE raw.kafka_users; SYSTEM RELOAD DICTIONARY core.users_dict;'
uv run python -m azki_pipeline.reconcile 202510
docker compose -f docker/compose.yml exec -T clickhouse clickhouse-client --multiquery --query \
  'ATTACH TABLE raw.kafka_users; ATTACH TABLE raw.kafka_user_events;'
```

Rebuilds must run serially: they share `mart.user_event_agg_rebuild`. The job stages states
from `core.user_events FINAL` using current dictionary values, then atomically replaces
the requested target partition; an empty source drops that target partition. Other months
are preserved. A failed staging step leaves the target untouched; rerun the same month to
retry. Always reattach ingestion tables after a failed job. Kafka retains records while
consumption is paused. To repair late user arrivals, first let CDC catch up in ClickHouse,
then pause consumption, reload the dictionary, and rebuild every affected event month.
The job does not enforce the maintenance window or coordinate concurrent processes.

Unit and contract tests need no Docker. E01 starts the Compose stack lazily, uses unique
connector/topic names and spool directories, validates both plugins through REST,
requires running tasks, and checks the users snapshot and typed events through Kafka.
It removes its connectors afterward and retains the services for subsequent work.
Failures include Compose logs in the pytest report. C04 now checks the deployed Kafka
topics without an expected failure. E02 checks fixture counts, orphans, and duplicate
reconciliation through ClickHouse; E03 checks MySQL insert/update/delete propagation.
Integration tests use isolated Testcontainers and unique databases, topics, and groups;
they read persisted targets and never select from a Kafka engine table.
