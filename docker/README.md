# Local streaming stack

See the [project overview](../README.md) for the architecture, quick start, and
documentation index. This guide covers local operations and recovery procedures.

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

## Governance and account provisioning

`make apply-sql` installs Phase 5 roles, masked views, partner policies, the analyst
settings profile, and a quota definition. It creates no application login accounts.
Use an access administrator to provision authenticated users and assign the appropriate
default role. Keep credentials outside this repository. The deployment account needs
access management and the ability to delegate the installed privileges; `role_admin`
receives that installer's current grants, including the ability to delegate them.

Assign analysts `role_analyst` and explicitly bind their user names to
`azki_governance_analyst_quota`. The quota starts with `TO NONE` so account provisioning
can supply the complete analyst user allowlist. Role assignment alone is insufficient
for quota provisioning in this deployment. Updating that allowlist must retain existing
analysts. The quota allows 1,000 queries per hour per user; the settings profile limits
queries to 10 seconds, 10,000 result rows, and 256 MiB. Zero cannot disable these limits.
Analyst query logging is mandatory and cannot be suppressed by sampling or duration filters.
Queries over system tables or constants can be exempt from quota accounting; I10 exhausts
the quota with actual governed-table reads.

Analysts query `gov.purchase_events_masked`. It reads FINAL, excludes deleted rows,
replaces national ID, phone, policy number, and plate number with `***`, and preserves
only the allowlisted non-PII product attributes (car value, coverage limit, property value).
New product attributes require a masking review. Its SQL SECURITY DEFINER executes with
the deployment account's privileges; retain that account and its source SELECT access.
Analysts receive no source PII, dictionary, lookup-view, or query-log permissions.

`role_partner_1` and `role_partner_2` have explicit non-PII column grants on
`mart.purchase_events_enriched`, with server-enforced policies for their partner IDs.
Read FINAL and filter `_is_deleted = 0` for the current logical snapshot. Partner roles
must be assigned individually to partner accounts; assigning both combines their access.
Partner roles do not receive the analyst's global masked view. Engineers can read and
maintain pipeline tables, including PII, but cannot administer accounts through that role.
Admins can inspect `system.query_log`; flush logs before an immediate audit query.

SQL under `sql/test_fixtures/governance/` creates passwordless, temporary test users only
inside disposable integration containers. It is excluded from numbered deployment SQL.
Tests namespace and remove both database objects and server-wide access entities.

## Data-quality checks

After `make apply-sql`, run `make dq` to collect, evaluate, and append all twelve checks
to `dq.check_results`. The runner prints one JSON CheckResult per line and exits 1 when
any check FAILs, or 0 for PASS/WARN-only results. Connection, configuration, and persistence
errors also fail the command; the runner closes its ClickHouse client on completion.
Thresholds are in `src/azki_pipeline/dq/thresholds.yaml`; pass `--thresholds` to use another
validated configuration. Limits are inclusive: equality stays in the lower severity,
WARN starts above `warn`, and FAIL starts above `fail`.

For independent reconciliation, supply a JSON object mapping UTC hour strings, such as
`2026-10-27T11:00:00Z`, to nonnegative integer counts of distinct source event keys.
Include zero-count hours when they need reconciliation. Counts must come from a CSV,
producer manifest, or independent Kafka reconciliation and use the same event-key rules;
do not derive the source counts from the ClickHouse sink being checked. Only the requested
hours are compared, including historical hours, with sink events before the run's UTC
cutoff. A manifest for a current partial hour needs the same cutoff at the source.

```sh
uv run python -m azki_pipeline.dq.runner \
  --kafka-bootstrap 127.0.0.1:9092 \
  --event-topic azki.user_events --consumer-group azki-events \
  --source-counts /path/to/hourly-counts.json
```

Broker lag uses Kafka Admin low/high watermarks and committed offsets, rather than
assuming ClickHouse consumer offsets are broker high watermarks. Missing broker
configuration produces WARN; broker access/collection errors produce FAIL. Source
reconciliation without a nonempty independent manifest, missing heartbeat/sample evidence, and
insufficient seven-day history produce WARN rather than a false healthy result.
The historical demo fixtures can legitimately FAIL freshness even when services are healthy.

The independent `azki-dq-samples` group reads the event topic as JSONAsString and retains
payloads, including unknown fields and parser errors, in `dq.event_samples` for one day
(MergeTree TTL cleanup is asynchronous). Drift checks inspect up to the latest 1,000
messages captured in the past hour. They record field names/types rather than payload
values in check results. This bounded sample can miss unsampled or expired drift; it is
not a schema-registry compatibility gate. Raw samples stay restricted to engineer/admin
access. The extra reader does not change the event ingestion consumer group's offsets.

`azki-dq-heartbeats` reads `__debezium-heartbeat.mysql` into a separate one-day table.
CDC freshness measures the heartbeat emission timestamp through ClickHouse, not a
fabricated users.updated_at or a guarantee about every database transaction. An idle
source can lack heartbeat evidence. Offset gaps measure continuity in the retained audit
stream, not proof of loss from compacted/transactional Kafka topics. Physical duplicate
counts can decrease as ReplacingMergeTree merges; source reconciliation compares logical
FINAL event counts instead. Volume uses the last completed UTC hour against seven prior
same-hour counts, a median/MAD baseline, and a configurable positive MAD floor.

The engineer role can query the required system Kafka/parts/table metadata and part log,
read pipeline data, and append DQ results. Part-log absence produces WARN; denied or failed
collection produces FAIL. DQ grants do not add access management or query-log access.

## Spark backfill

Install a Java 17 JDK and set `JAVA_HOME` to its home before running Spark or the
unit/integration/E2E test targets. `uv` installs the `spark` extra for those targets.
Use the project's PySpark 3.5 distribution: unset an inherited `SPARK_HOME` pointing
at a different Spark release. The local test fixture explicitly selects the installed
PySpark distribution and the current virtualenv's Python interpreter.

Apply the deployment SQL, then inspect a recovery range without changing event or mart data:

```sh
make apply-sql
env -u SPARK_HOME make backfill ARGS='--source csv --path tests/fixtures/user_events.csv --start 2025-10-16T00:00:00Z --end 2025-10-17T00:00:00Z --dry-run'
```

`--start` is inclusive and `--end` exclusive. Both require timezone-aware, whole-second
timestamps; filtering uses UTC after parsing. CSV timestamps default to UTC;
`--timezone Asia/Tehran` interprets timezone-free CSV values in that zone before hashing.
Invalid rows are counted and omitted in `fill-gaps`. Successful runs print a JSON summary
(`rows_read`, `invalid`, `missing`, `written`, status and run metadata) and appends an
audit row to `dq.backfill_runs`, including dry-runs and runtime failures. Failed runs
exit nonzero. `written` counts acknowledged event inserts or completed source partition
swaps, including batches acknowledged before a later failure. A lost HTTP response can
make this count conservative; rerunning compares FINAL keys to avoid another logical
event. Only the error class is persisted; payloads and exception text are excluded.

Before applying a recovery, pause all producers/Connect event sources, drain the event
consumer, stop the purchase refreshable view, and serialize backfill/reconciliation
jobs. Keep these writers stopped until the job finishes. `--writers-paused` asserts
this operating condition; the CLI does not acquire a distributed lock or stop services.
Resume the sources and refresh afterward. This also prevents a race between checking
existing keys and inserting missing ones. The default `fill-gaps` mode inserts only
absent event keys, then rebuilds each affected month in both marts from FINAL events.
A second successful fill run writes zero events and can still repair the marts.

```sh
env -u SPARK_HOME make backfill ARGS='--source csv --path tests/fixtures/user_events.csv --start 2025-10-16T00:00:00Z --end 2025-10-17T00:00:00Z --writers-paused'
```

For `--mode replace-partition`, supply an authoritative snapshot covering complete
UTC calendar months and month-aligned bounds, such as `2025-10-01T00:00:00Z` to
`2025-11-01T00:00:00Z`. The job rejects any invalid source rows before changing live
partitions. It loads `core.user_events_staging`, atomically replaces each selected
month, and rebuilds aggregate and purchase partitions. An empty snapshot intentionally
drops the selected months. Events outside the range and other months are preserved.
Replacement does not fire insert MVs, which is why both marts are repaired explicitly.
Each partition swap is atomic; the entire sequence across tables/months is not a
transaction. A failed run is audited and must be rerun before writers resume.

Kafka replay uses `--source kafka --kafka-bootstrap 127.0.0.1:9092 --topic TOPIC`
with the same range/mode flags. It reads a bounded batch across every partition,
retains topic/partition/offset, and fails on unavailable start timestamps or lost
offsets. These source bounds refer to Kafka record timestamps; the transforms also
filter payload event times. Use a complete CSV export to recover late events whose
Kafka record timestamps fall outside the requested event-time range. Spark downloads
the matching `spark-sql-kafka` Maven connector on first Kafka use. CSV replay does
not require that connector. Existing ClickHouse keys for the bounded range are
materialized on the driver before Spark's left-anti join; large recoveries should
be split into smaller ranges or use a distributed key export in production.
