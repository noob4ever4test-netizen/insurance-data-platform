# Insurance data platform — technical report

The platform ingests CSV events and MySQL CDC into ClickHouse, maintains aggregate
states and enriched purchases, governs analytical access, and detects and repairs
data loss. Kafka provides replayable transport; ClickHouse performs the streaming
transformations. Spark is reserved for bounded recovery. The implementation uses
synthetic fixtures; confidential source rows remain in gitignored `data/`.

See the [architecture diagram](architecture.mmd), [operating guide](../docker/README.md),
[test plan](TEST_PLAN.md), and [implementation evidence](implementation-notes.md).
This report describes the completed design; production extensions below are proposals.

## Architecture and trade-offs

Spooldir publishes `user_events.csv` through Kafka Connect; a validating Python
producer provides a fallback. Debezium snapshots and streams MySQL `users`, four
product tables, and `financial_order`, retaining versions and deletion markers.
The Compose environment supplies Kafka KRaft, Connect, Schema Registry, Kafka UI,
MySQL, and ClickHouse with pinned images and healthchecks. Connect currently emits
schemaless JSON: Schema Registry is available but does not enforce these contracts.

`raw` Kafka-engine tables feed materialized views (MVs). Valid records reach `core`;
stream parse errors retain their message, topic, partition, and offset in
`dq.kafka_dead_letter`. `core.user_events` uses `ReplacingMergeTree(ingested_at)`
with monthly event-time partitions and sort key
`(event_type, user_id, event_time, session_id)`. Its materialized UInt64 event key
matches Python's typed ClickHouse CityHash over user, session, timestamp, and type.
Latest CDC state uses versioned replacement and tombstones. Event provenance permits
reconciliation without inventing a source event ID or order ID.

This ClickHouse-native approach keeps incremental processing beside its serving
tables and avoids operating a second streaming computation cluster. It fits these
insert-driven aggregates and dictionary lookups. Spark/Flink streaming would offer
a place for stateful event-time joins, watermarks, and richer coordination, with
additional infrastructure and state management. The present design instead accepts
eventual enrichment, explicit repair, and operational serialization during recovery.

`mart.user_event_agg` stores count, distinct-user/session, sum, and average states
in `AggregatingMergeTree`, grouped by date, type, channel, city, and device. Queries
merge those states; user attributes come from `core.users_dict`, with unknown users
mapped to `unknown`. Dictionaries reflect current dimensions, not historical
attributes as of event time. Seed waits for dimension readiness before publishing events.

Four product MVs implement the union into `core.orders_unified`, retaining common
columns and product attributes. `core.purchase_order_lookup` prejoins FINAL,
non-deleted orders and financial rows by product/order identity and user/session.
Because events carry no order ID, attribution requires exactly one matched order
for `(user_id, session_id)` and an event/order time difference within ±1 hour.
Ambiguous matches remain unenriched rather than multiplying purchases. The composite
purchase dictionary refreshes every 1–5 seconds; event MVs perform lookups instead
of executing large joins per inserted block. Reloads still scan/join snapshots and
consume memory proportional to their size.

## Correctness, repair, and performance

Insert MVs observe their source's new blocks; later user, order, or financial changes
do not rewrite existing results. The purchase refreshable MV runs every minute,
appending corrected versions or tombstones for events from the first day of two
months ago onward. Historical purchases require explicit monthly repair. Purchase
reads use FINAL with `_is_deleted = 0`; incremental aggregate reads merge states.

ReplacingMergeTree deduplication is eventual, and FINAL event uniqueness does not
undo aggregate states emitted by duplicate deliveries. Serialized monthly
reconciliation stages aggregates from FINAL events and atomically replaces or drops
the affected aggregate partition, also applying current user attributes. Append-only
purchase refresh cannot retract a physically deleted source event by itself; the
backfill purchase staging/swap handles that case. These are explicit convergence
mechanisms, not a claim of transactional exactly-once delivery across Kafka and ClickHouse.

Monthly partitions, type-first ordering, LowCardinality dimensions, aggregate states,
and dictionary lookups are the implemented optimizations. The recorded I09 fixture
contained one historical and one current event partition. On ClickHouse 25.3.6.56:

```sql
EXPLAIN indexes = 1
SELECT event_key FROM core.user_events FINAL
WHERE event_type = 'purchase'
  AND event_time >= toStartOfMonth(now('UTC') - INTERVAL 2 MONTH);
```

The normalized scan reported `MinMax: Parts 1/2; Granules 1/2`, followed by
`Partition: Parts 1/1; Granules 1/1` and
`PrimaryKey: event_type, event_time; Parts 1/1; Granules 1/1`.
This demonstrates exclusion of the historical part, not throughput or latency at
production scale. No extra projection or skip index was justified by the evidence.

## Governance

Admin inherits the deployer's delegable privileges. Engineers maintain pipeline
tables and collect DQ metrics without access management. Analysts query an explicit
SQL SECURITY DEFINER view under `gov`; national ID, phone, policy number, and plate
number are masked with `***`, while product attributes use a safe allowlist.
Analysts have no raw PII, dictionary, or query-log access. Partner roles receive
safe column grants and row policies on the purchase mart scoped to partner identity.
An unrestricted policy preserves authorized admin, engineer, and definer reads.

Analyst settings enforce 10 seconds, 10,000 result rows, and 256 MiB, with constraints
preventing disabling the limits. A 1,000-query hourly quota requires explicit binding
to provisioned analyst identities; the deployment's quota initially targets NONE.
Login provisioning is an operator responsibility, and secrets are excluded from Git.
I10 authenticates as each role, verifies masking, denial, partner reassignment, active
limits and quota exhaustion, and checks attributed query-log records.

## Data-quality plan

Pure Python evaluators classify collected metrics as PASS/WARN/FAIL using validated
YAML thresholds. The runner persists all twelve results to `dq.check_results`, emits
JSON, and exits nonzero when any check fails. Equality stays at the lower severity.
Unavailable optional evidence WARNs; collection errors FAIL.

| Check | Evidence and decision |
|---|---|
| Freshness | Maximum event and ingestion ages; warn above 120 s, fail above 600 s; check future clocks |
| Consumer lag | Broker watermarks minus committed offsets per partition; worst lag above 100/1,000 warns/fails |
| CDC heartbeat | Debezium heartbeat age above 30/120 s warns/fails |
| Offset gaps | Missing offsets inside each retained audit topic/partition range; any gap fails |
| Source reconciliation | Independent hourly distinct source counts versus FINAL sink counts; any mismatch fails |
| Volume | Last closed UTC hour versus seven prior same-hour counts; median/MAD score above 3/6 warns/fails |
| Orphans | Missing user dictionary entries after users load; ratios above 1%/10% warn/fail |
| Schema drift | Added fields warn; missing required fields, changed types, or malformed payloads fail |
| Dead letters | Errors per audited message; any error warns, rate above 5% fails |
| Insert health | Active parts above 100/300 per partition warn/fail; part-log insert errors fail |
| Duplicates | Physical rows minus unique event keys; ratios above 1%/5% warn/fail |
| Purchase enrichment | Recent purchases without active enriched output; ratios above 1%/5% warn/fail |

A separate Kafka group stores original JSON with a one-day TTL; drift checks inspect
the latest 1,000 samples captured in the last hour. Unseen drift can escape detection.
Audit-offset continuity is diagnostic, not proof of transactional Kafka completeness;
independent source counts provide the sink completeness check. Short history, absent
manifests, or absent heartbeat evidence explicitly WARN instead of claiming health.

## Backfill and production operation

The Spark CLI accepts CSV or bounded Kafka input, an aware UTC `[start, end)` range,
`fill-gaps` or `replace-partition`, and dry-run. It validates contracts, normalizes
CSV timezones, deduplicates by key while retaining the first payload, and represents
UInt64 keys as Decimal(20,0). Fill mode anti-joins existing FINAL keys and inserts only
missing events in 1,000-row batches. Repeating a successful fill writes zero events.
Each affected month's aggregate and purchase partitions are rebuilt.

Replacement requires an authoritative snapshot of complete UTC months. It rejects
invalid source rows, stages deduplicated events in `core.user_events_staging`, and
replaces each partition; an empty snapshot intentionally drops the selected months.
Both marts are repaired because partition replacement does not fire insert MVs.
Swaps are atomic per table partition, not across tables or the entire run.

Operators must pause event writers and purchase refreshes and serialize repair jobs;
`--writers-paused` acknowledges this condition rather than acquiring a lock. Failed
runs must be repaired before writers resume. `dq.backfill_runs` records counts,
status, range, and timings, including acknowledged batches before failure. Kafka
bounds use record timestamps, then filter payload event times; late events outside
that record-time window require a complete export. Existing keys are collected to
the driver, so large recoveries need smaller ranges or a distributed key export.

For production, add ReplicatedMergeTree tables coordinated by ClickHouse Keeper,
tested replication/failover and backup restores, and measured sharding and distributed
query layouts. Harden Kafka replication, retention, authentication, and TLS; apply
Schema Registry compatibility with supported Avro/Protobuf ingestion rather than
assuming today's JSON path enforces it. Schedule DQ and recovery with Airflow,
durable source manifests, alert routing, exclusive maintenance locks, and explicit
dependencies for pausing, draining, repairing, verifying, and resuming writers.
Load tests must measure merge pressure, dictionary memory/reload cost, FINAL scans,
and repair duration before selecting capacity or adding indexes.

## Test pyramid and validation

Counts below are executed parametrized cases, not unique plan IDs. Phase 7 established
the runtime baseline; Phase 8 adds ten documentation contract cases for local evidence
links and connected ingestion, DQ, and backfill paths.

| Layer | Cases | Evidence |
|---|---:|---|
| Unit | 195 | Pure checks, contracts, helpers, and local Spark transforms; Phase 7: 15.62 s |
| Contract | 46 | SQL/config/schema validation plus Phase 8 documentation checks |
| Integration | 52 | Testcontainers, real broker replay and ClickHouse behavior; Phase 7: 486.57 s |
| E2E | 6 | Full Compose CDC, outage recovery, drift, and DQ loss/backfill cycle; Phase 7: 267.59 s |

E06 proves all twelve DQ checks PASS, deleting one hour causes reconciliation FAIL,
and Spark recovery restores PASS. Interrupted-insert tests audit 1,000 acknowledged
writes and recover only the remaining gap. The runtime baseline has no skips or
expected failures. A prior E2E setup hit the local ClickHouse 1 GiB memory cap;
restart preceded the successful run without raising the limit. Test durations include
infrastructure and synthetic workloads; they are not capacity benchmarks.
Phase 8's 46 contract cases and lint pass; Mermaid 11.12.0 parses the diagram.
