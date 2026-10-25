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
