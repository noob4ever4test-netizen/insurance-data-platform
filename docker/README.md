# Local streaming stack

Run from the repository root with Docker Desktop or Docker Engine and Compose v2:

```sh
uv sync
make up
make seed
make test-e2e
```

`make up` prepares local secrets and synthetic order CSVs, builds Connect with pinned
Debezium MySQL and spooldir plugins, and waits for every default service's healthcheck.
The first MySQL startup creates the six source tables and loads synthetic data. Its
initialization directory mounts the numbered SQL from `sql/mysql/` directly. The Debezium user
has snapshot/replication privileges and SELECT access to the source database.

`make seed` validates and registers the MySQL connector, waits until all fixture users
have appeared on `mysql.azki.users`, then registers spooldir and stages the synthetic
event CSV by an atomic rename. Each seed invocation replays the event file; it does
not deduplicate source events. ClickHouse ingestion and reconciliation land in Phase 3.

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
delete flags. Its DATE and DATETIME fields retain Debezium logical encodings; Phase 3
must convert them when writing ClickHouse users/order tables.

Unit and contract tests need no Docker. E01 starts the Compose stack lazily, uses unique
connector/topic names and spool directories, validates both plugins through REST,
requires running tasks, and checks the users snapshot and typed events through Kafka.
It removes its connectors afterward and retains the services for subsequent work.
Failures include Compose logs in the pytest report. C04 is a strict expected failure
until Phase 3 adds the ClickHouse Kafka tables, as required by `prompts/PROMPTS.md`.
