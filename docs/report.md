# Technical report

## Notes

### Phase 0: scaffold and data contract

- Python 3.11, uv dependency management, strict pytest markers, Ruff lint/format checks,
  and numbered SQL placeholders establish the layout. Docker, Connect, SQL, and Python
  modules outside this phase remain placeholders. `seed` reports that Phase 2 is required.
  Install uv, then run `uv sync` and `make test-unit`, `make test-contract`, and `make lint`.
  Integration tooling and Spark are optional extras so these checks require neither Docker
  nor a JVM. Empty integration/e2e directories contain no future-phase tests.
- Compared only the header records of `data/user_events.csv` and `data/users.csv` with
  the documented and synthetic headers: both match exactly. Real rows were not copied;
  the existing synthetic fixture files were unchanged. `.gitignore` excludes `data/`.
- Draft 2020-12 JSON Schemas require all documented columns, Int64 user IDs and premiums,
  nonnegative premiums, and the three permitted event types. City, device, and channel
  values remain open strings. Extra fields remain available for later drift detection.
  Format checking must be enabled; the event timestamp uses a custom
  `clickhouse-datetime` checker because its space-separated, timezone-free CSV form is
  not RFC 3339. Contract tests register that checker and validate actual calendar dates.
- `event_key` matches ClickHouse's typed variadic hash: `Int64, String, DateTime, String`.
  This requires seeded integer mixing, CityHash v1.0.2 for UTF-8 strings, then ordered
  Hash128to64 reductions. Hashing concatenated fields would give another result. The
  conflicting xxhash/SHA-1 suggestion in the data contract was corrected accordingly.
  Naive timestamps mean UTC; aware datetimes normalize to UTC before hashing.
- U20's independently obtained vector was queried from the ClickHouse public playground
  (reported version `26.11.1.707`) using only synthetic values:

  ```sql
  SELECT cityHash64(toInt64(3), toString('s_1'),
                    toDateTime('2025-10-16 17:17:33', 'UTC'), toString('policy_click'));
  -- 14560372016242613758
  ```

  Reference implementation: ClickHouse `src/Functions/FunctionsHashing.h`
  (`IntHash64Impl`, `ImplCityHash64`, and `executeIntType`). Future ClickHouse schemas
  must preserve these argument types; hashing the timestamp as a String changes the key.
- Factories return independent synthetic dictionaries with overrides. `eventually` retries
  assertion failures or boolean `False`, returns successful results (including `None` from
  assertion callbacks), preserves the last assertion on timeout, and propagates other
  exceptions. It uses a monotonic deadline and bounded Event waits. Its unit tests use a
  fake clock and wait function, so no test sleeps or needs a service.
- TDD evidence: the first implementation checks failed on missing public symbols;
  schema checks then reported **12 failed, 7 passed** against empty schemas. After
  implementation, the unit layer reported **27 passed**, and the contract layer
  **19 passed**, including C01. C01 passed from the outset because the supplied headers
  already matched. No tests were skipped, weakened, or removed.
  Final `make test-unit`, `make test-contract`, and `make lint` passed; Ruff checked and
  formatted all 33 Python files. SQL placeholders are empty, so SQL parsing currently
  has no statements to validate. A malformed-query probe confirmed that the parser's
  default immediate error mode rejects invalid SQL.
- Later phases must document the purchase-to-order join assumption: match
  `(user_id, session_id)` with order `created_at` within one hour of event time.
  There is no event `order_id` and no user `updated_at`; use Debezium `ts_ms` for CDC
  versions/lag. Unknown users in fixtures remain intentional.

### Phase 1: contracts and fallback producer

- Implemented schema loading and validation with calendar-aware CSV/UTC format checkers.
  CSV timestamps retain their original contract; wire validation selects a separate UTC
  timestamp definition within the same schema. No Phase 0 timestamp assertions were relaxed.
  `schema_diff` returns sorted added/removed fields, changed field types, and required-field
  changes. Union-type order does not count as a type change.
- The fallback producer streams CSV rows, converts IDs/premiums to integers, validates them,
  normalizes timestamps, and sends UTF-8 JSON keyed by the UTF-8 user ID. An explicit
  timezone overrides `EVENTS_TZ`; UTC is the default. It retains source order, late events,
  unknown users, exact duplicates, and extra columns for later pipeline/DQ handling.
- Invalid headers, field counts, numeric values, timestamps, enum values, premiums, and
  CSV quoting are collected with a source line number and reason. Recoverable parser errors
  do not prevent subsequent valid rows from publishing. Raw rows are not stored in the
  result. Kafka errors remain operational failures: delivery callbacks are checked, flush
  must drain the queue, and unresolved messages raise a timeout. The CLI enables producer
  idempotence and `acks=all`, prints a JSON summary, and returns nonzero for rejected rows.
- Run the fallback path after a broker is available:

  ```sh
  uv run python -m azki_pipeline.producer tests/fixtures/user_events.csv \
    --bootstrap-servers localhost:9092 --timezone UTC
  ```

- `apply_sql` executes numbered files in numeric order, splits statements with SQLGlot's
  ClickHouse parser, and applies database overrides to parsed identifiers, dictionary source
  databases, and dictionary-function references. It preserves unrelated string literals,
  including embedded semicolons; no DDL is assembled by concatenation. Invalid override
  identifiers are rejected and server errors propagate. SQLGlot is now a runtime dependency.
  Numbered runner fixtures live under `sql/test_fixtures/`; the default deployment runner
  scans only the top-level `sql/` directory.
- TDD evidence: new tests initially reported three missing-symbol collection errors.
  The first implementation passed all 62 selected tests (including existing U20 cases).
  A further malformed-quoting regression failed because DictReader retained the previous
  successful line number; using the underlying parser's physical line number fixed it.
  One test input was corrected before implementation: CSV serializes a null channel as
  an allowed empty string, so the invalid-input case now uses a null user ID. No existing
  tests were weakened, removed, or skipped.
- Scope follows the Phase 1 executable prompt: U01–U06, U19, and U20. The heading also
  mentions C10, but it requires the deployment table DDL scheduled for Phase 3. C10 remains
  explicitly pending rather than asserting against empty files or introducing Phase 3 SQL.
  All producer and SQL-client checks use test doubles; broker/server integration remains
  for later phases. Final layer results: 77 unit tests and 19 contract tests, with lint clean.

### Phase 2: Compose and source connectors

- Added a pinned Compose stack: Confluent Kafka/Schema Registry/Connect 7.9.2,
  MySQL 8.0.43, ClickHouse 25.3.6.56, and Kafka UI 0.7.2. Every service has a
  healthcheck. The custom Connect image installs Debezium MySQL 2.7.4.Final and
  spooldir 2.0.65. The default is a combined KRaft controller/broker; the
  `three-brokers` profile adds two brokers while retaining one controller and
  replication factor 1. All three broker healthchecks were verified on Docker.
- `make up` prepares ignored local secrets and synthetic CSVs before starting services.
  Passwords are stable across runs and mode 0600; committed configs refer to the
  password through FileConfigProvider. `make down` preserves volumes. Operational
  commands, ports, profile limitations, and credential persistence are documented in
  `docker/README.md`. No real data files are mounted or copied.
- MySQL runs ROW/FULL binlogging and GTIDs. Numbered files in `sql/mysql/` are mounted
  directly into its initialization directory, with a mysql-client-only local infile
  setting. It loads nine synthetic users, four rows in each of the four product tables,
  and sixteen financial rows. Product tables include common synthetic PII, product
  attributes, and indexes on `(user_id, session_id, created_at)`. Orders derive from
  distinct fixture purchases and use the same session/time keys; duplicates remain
  in the source event fixture. Fresh initialization was verified independently in a
  temporary MySQL database, then that temporary container was removed.
- The MySQL connector snapshots all six tables, retains timestamp metadata and
  rewritten delete flags, and emits heartbeat messages. Spooldir has explicit typed
  schemas, extracts user ID as a numeric Kafka key, and uses separate persistent input,
  finished, and error paths. File parsing quarantine is distinct from Connect's
  configured DLQ; do not assume source DLQ settings handle every file parser failure.
- `ConnectClient` validates configs through REST before idempotent registration,
  includes the required connector name in the request config, and waits for both the
  connector and nonempty running tasks. Failed tasks surface their trace; polling has
  a deadline and retries transient status errors. `make seed` registers MySQL first,
  waits for all synthetic users on the CDC topic, then registers spooldir and stages
  an event file by atomic rename. Each seed call intentionally replays that file.
- TDD evidence: the initial configs produced seven failing contract assertions, with
  C04 a strict expected failure as required by `prompts/PROMPTS.md`; client/bootstrap
  tests initially failed on missing modules. Added regressions exposed the validation
  endpoint's required name, direct MySQL initialization mounts, and IPv4 advertised
  host listeners. Each regression was red before its fix. Local checks currently
  report 117 passed and the planned C04 xfail. C04 remains pending until Phase 3 SQL.
- Runtime validation found and resolved a mounted shell initializer issue by mounting
  SQL directly, and a mysqladmin incompatibility by restricting local-infile to the
  `[mysql]` option group. Docker Hub downloads exceeded the first E01 startup timeout;
  downloads completed separately. The first plugin download failed Confluent Hub's
  checksum check; a retry passed, and the official archive was independently verified
  against publisher MD5 `7e5487912947e1c21245aa1b72d81c65`. Verification was not disabled.
- E01 uses unique connector/topic names and spool directories, validates real plugins,
  requires running tasks, and consumes both the complete users snapshot and all fixture
  events. Kafka host listeners use explicit `127.0.0.1` because IPv6-first localhost
  resolution failed against IPv4-only bindings. Polling assertions retain collected
  counts; key/type checks run afterward so an error cannot be hidden by later empty polls.
  Test failures attach Compose logs. A remaining timeout was traced to subscribing before
  spooldir created its topic: default metadata refresh could outlast the test deadline.
  E01 now creates its unique topics first; the bootstrap snapshot consumer also refreshes
  topic metadata every second. The seed metadata setting has a red/green regression test.
  Final results: **91 unit tests passed**, **26 contract tests passed**, the planned
  **C04 strict xfail**, **E01 passed** (29.49 seconds), and **lint passed**. All six default
  services and both optional brokers were healthy. Services remain running for subsequent phases.
  The documented `make seed` command completed successfully, followed by
  `make test-e2e` (**1 passed in 27.70 seconds**) with the registered default connectors
  also active. Existing synthetic fixture files were unchanged; Git ignores generated
  credentials and order CSVs.

### Phase 3: ClickHouse Part 1 streaming pipeline

- Added numbered deployment SQL `001`–`040`: Kafka event/users engines, persisted events
  and versioned users, parsing-error quarantine, a latest non-deleted users view,
  a users dictionary, aggregate states, and an orphan-count/ratio view. The aggregate
  MV is installed before the Kafka event forwarding views to prevent events bypassing
  aggregation during deployment. Apply all SQL before enabling producers/connectors.
- Events use UTC timestamps and a materialized CityHash key. Runtime tests compare
  ClickHouse keys against Python for all synthetic fixture events. ReplacingMergeTree
  deduplicates by the full event identity in ORDER BY; query FINAL for stable results.
  Kafka topic, partition, offset, and microsecond ingestion time remain available for
  reconciliation. Extra JSON fields are accepted; malformed JSON/type errors retain
  the original message and error in the dead-letter table without blocking good events.
- Debezium DATE integers are converted from epoch days; `__ts_ms` supplies the users
  version and rewritten `__deleted` supplies tombstones. Users use one partition so
  updates cannot strand versions in different partitions. The dictionary reads FINAL
  rows with deleted users excluded and refreshes every 1–5 seconds. The seed command
  now waits for Kafka snapshot completion, ClickHouse users, and dictionary values
  matching current users before staging the event CSV. Independent writers can still
  produce orphans, which receive `unknown` dimensions and are measured explicitly.
- Aggregates store count, distinct users/sessions, premium sum, and premium average
  states by the five contract dimensions. The Python fixture oracle checks every
  bucket, including duplicates, an orphan, and late data. Reads merge states and group
  by every dimension. Source inserts trigger the MV; later user changes and eventual
  source deduplication cannot retroactively change those states.
- Added `python -m azki_pipeline.reconcile YYYYMM` and numbered SQL under `sql/jobs/`.
  The job stages one month from FINAL events using current dictionary dimensions and
  atomically replaces that aggregate partition. Empty source months remove obsolete
  aggregate partitions. Tests prove repeatability, late-user re-enrichment, and
  preservation of other months. Operators must pause Kafka consumption/direct writers
  and serialize rebuilds because the staging table is shared; the job does not enforce
  a maintenance window. Recovery commands and the ingestion restart procedure are in
  `docker/README.md`. Added `make apply-sql` for idempotent deployment.
- TDD evidence: C05–C07/C10 initially produced four expected assertion failures against
  empty SQL, and I01–I05 produced five missing-deployment failures in a real ClickHouse
  container. Rebuild tests initially produced six missing-module failures. Additional
  regressions demonstrated missing CLI support, unsafe rebuild database overrides,
  premature Kafka activation, missing container-network listeners, and seed readiness
  limited to Kafka; each was fixed after observing failure. The direct-insert fixture
  was corrected to use timezone-aware UTC datetimes because the local driver otherwise
  interpreted naive timestamps in the host timezone. No assertion was weakened.
- I11–I13 use a real KRaft Testcontainer and ClickHouse Kafka engines; E02–E03 use
  isolated Compose connectors/topics/databases and the real Debezium/spooldir paths.
  E02 reconciles fixture duplicates and checks the expected orphan ratio; E03 verifies
  synthetic MySQL inserts, updates, deletes, and dictionary enrichment. Tests use
  polling, never read Kafka tables directly, and clean up their connectors/databases.
  C04 is now a normal passing assertion. Phase 4–7 implementation remains pending.
- Final verification: `make test-unit` **101 passed**, `make test-contract` **34 passed**,
  `make test-integration` **10 passed**, `make test-e2e` **3 passed**, and `make lint`
  passed. No tests were skipped or marked expected-failure in this run. Integration
  completed in 29.85 seconds; E2E completed in 50.78 seconds.

### Phase 4: Denormalized purchase events

- Added four versioned product tables and four incremental union MVs in `050`, preserving
  common order fields and synthetic PII plus a `Map(String, String)` of product-specific
  attributes. Identity is `(product_type, order_id)` in the union so product IDs cannot
  collide. Source and union dimensions use a constant partition and immutable replacement
  keys, so changes to user, session, partner, or timestamp replace the same order.
- All four product topics and `financial_order` now have Kafka landing tables, CDC MVs,
  and dead-letter MVs. The supplied MySQL DATETIME columns emit Debezium millisecond
  timestamps, converted explicitly to UTC; a Kafka regression caught an initial incorrect
  microsecond conversion. Every source retains topic, partition, offset, CDC version, and
  tombstones. C04 now checks all six CDC topics as well as the event topic.
- `core.purchase_order_lookup` prejoins FINAL non-deleted orders with FINAL non-deleted
  financial rows. It requires matching order ID/product as well as user/session. Exactly
  one fully matched order per session is required; ambiguous sessions are excluded rather
  than multiplied or attributed arbitrarily. This is an explicit attribution assumption,
  because events have no order ID. The ±1 h order/event sanity filter includes its endpoints.
- `core.purchase_orders_dict` loads that prejoin into a hashed composite-key lookup,
  refreshing every 1–5 seconds. The purchase event MV only performs dictionary lookups:
  it avoids rebuilding the large order/financial join for every event block. Full dictionary
  reloads still scan/join dimension snapshots and use memory proportional to their size;
  this design amortizes that work rather than eliminating it. The enriched row retains
  separate event premium, order premium, payment amount/status, user attributes, and PII.
- `mart.purchase_events_enriched` replaces by event identity and enrichment timestamp.
  Reads require FINAL and `_is_deleted = 0`. Duplicate source deliveries cannot create
  multiple active purchases. Non-purchase events never write rows. Missing, ambiguous,
  out-of-window, or deleted attributions write tombstones that can replace stale output.
- Incremental MVs do not react to dimension-side inserts. A real refreshable MV runs
  every minute, appending corrected versions from deduplicated events dated from the first
  day of two months ago (UTC) onward. Older output remains untouched. I09 proves late order
  and financial arrivals require a refresh, then verifies repeatability, attribute updates,
  and order/financial deletions. An explicit parameterized monthly SQL job repairs older
  purchases. This source-driven repair does not retract purchases whose source events were
  physically removed; the tested retractions are caused by dimension tombstones.
- Seed now waits for joined order/financial rows and matching dictionary values before
  staging events. This also makes the older synthetic fixtures safe despite falling
  outside the automatic repair window. E02 snapshots all six actual MySQL source tables,
  runs Debezium/spooldir, and checks every fixture purchase's ID, product, paid amount,
  and converted timestamp. Event consumption is activated at the end of `060`, after both
  incremental event MVs exist. Active Phase 3 upgrades need paused event ingestion and
  explicit repair of previously ingested months, as documented in `docker/README.md`.
- TDD evidence: the initial I06–I09/contract run had **26 expected failures, 5 passes**
  against missing Phase 4 DDL. New regressions exposed premature event consumption,
  incorrect CDC timestamp units, missing historical repair SQL, and missing order readiness
  in seed; each was observed red before implementation. No existing assertion was removed
  or weakened. Test topics/groups are isolated for every added CDC engine.
- Performance evidence uses the I09 fixture containing one old and one current monthly
  event partition. The query below is the source scan used by automatic repair; normalized
  EXPLAIN output from ClickHouse 25.3.6.56 is recorded here:

  ```sql
  EXPLAIN indexes = 1
  SELECT event_key FROM core.user_events FINAL
  WHERE event_type = 'purchase'
    AND event_time >= toStartOfMonth(now('UTC') - INTERVAL 2 MONTH);
  ```

  ```text
  ReadFromMergeTree (core.user_events)
    MinMax: event_time; Parts: 1/2; Granules: 1/2
    Partition: toYYYYMM(event_time); Parts: 1/1; Granules: 1/1
    PrimaryKey: event_type, event_time; Parts: 1/1; Granules: 1/1
  ```

  The existing monthly partition and sorted key exclude the historical part. No additional
  projection or skip index is added without evidence of further benefit. These synthetic
  tests validate pruning and correctness; they are not a production throughput benchmark.
- Final verification: `make test-unit` **103 passed**, `make test-contract` **35 passed**,
  `make test-integration` **40 passed** (212.66 seconds), `make test-e2e` **3 passed**
  (89.41 seconds), and `make lint` passed. No skips or expected failures. Phase 4 is
  complete; governance remains Phase 5.

### Phase 5 — Governance

- `070_governance.sql` installs admin, engineer, analyst, and two partner roles.
  Admin inherits the deployer's delegable rights; engineers receive pipeline table
  privileges without access management. Login provisioning remains an operator concern;
  deployment creates no passwordless application users or committed credentials.
- Analysts use an explicit-column SQL SECURITY DEFINER view over FINAL, active purchases.
  National ID, phone, policy number, and plate number become `***`. Only car value,
  coverage limit, and property value survive the product-attribute allowlist. Raw PII,
  dictionary functions, the dimension lookup, and query logs remain inaccessible.
- Partners have explicit safe column grants and row policies on the purchase mart.
  A separate unrestricted policy preserves administrator, engineer, and definer reads.
  I10 connects as all five roles and checks both initial isolation and reassignment of
  an existing order to another partner after purchase repair, using FINAL reads.
- Analyst profiles enforce a 10-second execution limit, 10,000 result rows, and 256 MiB,
  with lower constraints preventing zero from disabling those limits. Logging is enabled
  without sampling or minimum-duration suppression. I10 verifies forbidden setting changes,
  actual result-limit failure, and an attributed QueryFinish record in system.query_log.
- The hourly 1,000-query quota is keyed per user. Its deployment definition targets NONE;
  identity provisioning explicitly binds analyst user names, as the integration fixture
  does before opening authenticated connections. The test exhausts the quota through real
  masked-table reads and confirms admin remains able to query. Constant/system queries
  can be exempt from quota accounting. The operating guide documents required bindings.
- TDD started with missing-governance C08 and I10 failures and a U19 failure for safe
  access-entity isolation. Further failing assertions exposed lost non-PII attributes,
  limits that could be disabled with zero, ineffective quota probes, and audit suppression.
  SQLGlot does not parse several valid RBAC constructs. C05's blanket rejection of its
  Command fallback was incorrect for these constructs: it now delegates to a bounded
  grammar validator for supported access statements, while other SQL retains AST checks.
  U19 still rejects unsupported commands and preserves literal/identifier isolation;
  ClickHouse 25.3.6.56 validates and enforces the deployed governance statements.
- Integration and E2E database overrides also namespace roles, profiles, quotas, policies,
  and temporary test users. Cleanup drops server-wide access entities after their databases.
- Final verification: `make test-unit` **106 passed**, `make test-contract` **36 passed**,
  `make test-integration` **41 passed** (215.04 seconds), `make test-e2e` **3 passed**
  (86.36 seconds), and `make lint` passed. The final I10 rerun also passed, including
  repeated governance deployment. No skips or expected failures. Phase 5 is complete;
  data-quality checks and backfill remain for the next phase.

### Phase 6 — Data quality

- Pure evaluators produce typed CheckResults with PASS/WARN/FAIL, observed metrics,
  thresholds, non-payload details, and one timezone-aware run timestamp. YAML thresholds
  reject unknown/missing settings, non-finite limits, and reversed severities. Equality
  stays in the lower severity. The CLI persists every result and exits nonzero on FAIL.
  Missing optional evidence is WARN; a missing or failed collector is FAIL, never omitted.
- `080_monitoring.sql` and parameterized queries under `sql/dq/` separate collection
  from evaluation. A second Kafka group retains original event JSON for one day, because
  successful Kafka-engine rows do not otherwise preserve unknown fields. Sampling is
  limited to 1,000 recently captured messages; drift outside that sample is not guaranteed
  to be detected. New fields WARN; missing required fields, changed declared types,
  malformed JSON, and non-object payloads FAIL. Results retain field names, not raw values.
- The twelve checks and their evidence are:

  | Check | Evidence and decision |
  |---|---|
  | Freshness | Both maximum event and ingestion times; missing evidence WARNs, excessive future timestamps FAIL |
  | Consumer lag | Actual broker low/high watermarks and group committed offsets; report total, threshold worst partition |
  | CDC lag | Retained Debezium heartbeat emission timestamp; no invented users.updated_at |
  | Offset gaps | Distinct offsets within each retained audit topic/partition range |
  | Source reconciliation | Independent, validated hourly source-count manifest versus FINAL sink counts for those hours |
  | Volume | Last closed UTC hour versus seven preceding same-hour counts, median/MAD with a positive floor |
  | Orphans | dictHas on current users, gated until users are loaded |
  | Schema drift | Original JSON samples versus declared required fields and JSON types |
  | Dead letters | Event-topic errors per recently audited message; known errors cannot pass with missing receipts |
  | Insert health | Maximum active parts per partition, part-log throughput/errors and availability |
  | Duplicates | Physical row count minus unique event keys; ratio falls as replacements merge |
  | Purchase enrichment | Recent FINAL purchases without an active enriched event key |

- Gaps in a retained audit stream are a diagnostic, not a guarantee about transactional
  or compacted Kafka logs. Independent source reconciliation is needed to establish sink
  completeness. Heartbeat age measures that path's freshness, not every source transaction.
  A new deployment without seven days of history, no source manifest, or an idle heartbeat
  cannot honestly claim all PASS. These conditions are explicit WARNs with evidence.
- I14 verifies exact seeded metrics and twelve persisted results, historical manifest
  scoping, actual broker offsets over two partitions, real Kafka heartbeat ingestion, and
  execution as engineer without access management. E05 publishes an added field and a
  following normal event through the full Compose stack: both persist, drift WARNs on
  utm_campaign, and no event enters dead-letter storage.
- TDD began with a missing CheckResult symbol, missing monitoring table, and missing E05
  collector. Regressions exposed unchecked partition offset errors, incorrect historical
  reconciliation scope, insufficient engineer system-table grants, and dead-letter evidence
  that could be hidden by a missing denominator. The duplicate test fixture needed separate
  insert blocks to prevent within-block replacement; its exact count assertion was retained.
  U07–U14, I14, and E05 are complete. Spark/backfill and recovery E2E cases remain Phase 7.
- Final verification: `make test-unit` **178 passed**, `make test-contract` **36 passed**,
  `make test-integration` **46 passed** (326.82 seconds), `make test-e2e` **4 passed**
  (135.28 seconds), and `make lint` passed. No skips or expected failures. An earlier
  E05 retry reached the long-running local Compose ClickHouse server's 1 GiB memory cap;
  restarting that service preceded the successful full E2E run. These are functional
  checks against synthetic data, not production capacity measurements.
