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
and waits for all fixture purchase orders, financial rows, and their dictionary entries.
It then registers spooldir and stages the synthetic
event CSV by an atomic rename. Each seed invocation replays the event file; it does
not deduplicate source events. Apply SQL before seeding so downstream materialized views
exist before the first Kafka records arrive. The event forwarding views are activated
at the end of `060_purchase_enriched.sql`, after both event materialized views are installed.
When upgrading an already active Phase 3 deployment, pause event ingestion while applying
the new DDL, and run historical purchase repair for months ingested before Phase 4.

Dictionary refreshes occur every 1–5 seconds; events from independent writers that arrive
before their users use `unknown` dimensions and require reconciliation. The seed command
waits for users, orders/financial rows, and matching dictionary values before staging its CSV,
and fails with a bounded
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
The users dictionary reads a FINAL, non-deleted view. The four order CDC topics and
financial topic also land through Kafka engines, converting the supplied MySQL DATETIME
millisecond values to UTC. Every CDC engine has a dead-letter MV. Product MVs union
common fields and a `Map(String, String)` of product attributes into `core.orders_unified`.

Purchase inserts use `core.purchase_orders_dict`, a dictionary keyed by `(user_id, session_id)`
whose source prejoins current orders with current financial rows. The financial row must
also match order ID and product; sessions with multiple matching orders are excluded.
The order must be within one hour of the event. The event MV performs dictionary lookups
without scanning a large join for each event block. Like the users dictionary, it refreshes
every 1–5 seconds. Its full reload still joins the latest source tables and uses memory
proportional to the lookup size; measure these costs before scaling beyond this local stack.

Read deduplicated active purchases with:

```sql
SELECT event_time, user_id, session_id, order_id, product_type, payment_status, paid_amount
FROM mart.purchase_events_enriched FINAL
WHERE _is_deleted = 0;
```

`mart.purchase_events_repair_mv` runs every minute and appends versioned corrections for
FINAL purchase events from the first day of two months ago in UTC onward. It handles
late orders/financial rows, changed attributes, deleted dimensions and duplicate events.
Unmatched or deleted attributions produce tombstones, which retract previously enriched
rows. Older months are retained. Physical versions accumulate between merges, so active
queries must use FINAL and the deletion filter. Source-event removal itself is not a
dimension deletion and is not retracted by this source-driven repair.

After CDC has caught up, an immediate refresh can be requested with:

```sh
docker compose -f docker/compose.yml exec -T clickhouse clickhouse-client --multiquery --query \
  'SYSTEM RELOAD DICTIONARY core.purchase_orders_dict; SYSTEM REFRESH VIEW mart.purchase_events_repair_mv; SYSTEM WAIT VIEW mart.purchase_events_repair_mv;'
```

To repair an older month (also useful after upgrading Phase 3), reload both dictionaries
and execute the checked-in parameterized SQL from the repository root:

```sh
docker compose -f docker/compose.yml exec -T clickhouse clickhouse-client --multiquery --query \
  'SYSTEM RELOAD DICTIONARY core.users_dict; SYSTEM RELOAD DICTIONARY core.purchase_orders_dict;'
docker compose -f docker/compose.yml exec -T clickhouse clickhouse-client --param_partition=202510 \
  < sql/jobs/010_repair_purchase_partition.sql
```

The monthly purchase repair appends replacements/tombstones with a new version; rerunning
it leaves the same logical FINAL result. It does not swap the aggregate staging table and
does not require the serialized aggregate rebuild procedure described below.

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
topics without an expected failure. E02 snapshots all six source tables and checks fixture
counts, orphans, duplicate reconciliation and purchase enrichment through ClickHouse;
E03 checks MySQL insert/update/delete propagation.
Integration tests use isolated Testcontainers and unique databases, topics, and groups;
they read persisted targets and never select from a Kafka engine table.
