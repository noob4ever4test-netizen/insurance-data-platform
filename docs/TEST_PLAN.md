# Test plan

Status: `todo | red | green`. Test function name = `test_<ID>_<desc>`.
Columns follow `docs/data_contract.md` (real headers). `event_key` is derived — see contract.

## Unit (`-m unit`, no Docker)
| ID | Module | Behaviour | Status |
|---|---|---|---|
| U01 | contracts | valid event row passes `user_event.schema.json` | green |
| U02 | contracts | parametrized invalid rows fail: missing `event_time`, null `user_id`, bad timestamp, unknown `event_type`, negative `premium_amount` | green |
| U20 | contracts | `event_key(user_id, session_id, event_time, event_type)` is deterministic and equals the ClickHouse `cityHash64` value for a fixed vector | green |
| U03 | producer | Kafka key = `user_id`, value = JSON matching contract | green |
| U04 | producer | `YYYY-MM-DD HH:MM:SS` parsed with configurable tz and emitted as UTC ISO-8601 | green |
| U05 | producer | malformed CSV row → routed to error list, not raised | green |
| U06 | contracts | `schema_diff(old, new)` reports added / removed / type-changed fields | green |
| U07 | dq.freshness | PASS / WARN / FAIL at threshold boundaries | todo |
| U08 | dq.lag | consumer lag per partition aggregated; FAIL if any partition over threshold | todo |
| U09 | dq.offset_gaps | detects gap from (min, max, count) per partition | todo |
| U10 | dq.volume | anomaly vs trailing median ± k·MAD; no false alarm on flat series | todo |
| U11 | dq.orphans | orphan ratio computed and thresholded | todo |
| U12 | dq.schema_drift | new field → WARN, removed/required type change → FAIL | todo |
| U13 | dq.duplicates | duplicates = count − uniqExact(event_key) | todo |
| U14 | dq.runner | exit code non-zero iff any FAIL; results serialized | todo |
| U15 | backfill.transforms | parse + validate drops invalid rows and counts them | todo |
| U16 | backfill.transforms | dedupe by `event_key` (keep first); Spark `event_key` == Python `event_key` on same rows | todo |
| U17 | backfill.transforms | missing rows = left-anti join on `event_key` vs existing keys | todo |
| U18 | backfill.transforms | date-range filter is inclusive/exclusive as documented | todo |
| U19 | sql_runner | applies files in numeric order; DB overrides substitute safely; deployment CLI closes its client | green |
| U21 | helpers.factories | event factory has contract fields and supports overrides | green |
| U22 | helpers.factories | user factory has contract fields and supports overrides | green |
| U23 | helpers.factories | each factory call returns independent data | green |
| U24 | helpers.eventually | successful checks return immediately, including assertion callbacks | green |
| U25 | helpers.eventually | retry assertions and false predicates until success | green |
| U26 | helpers.eventually | deadline limits waits; timeout preserves last assertion | green |
| U27 | helpers.eventually | unexpected exceptions propagate immediately | green |
| U28 | helpers.eventually | invalid timeout or polling interval rejected | green |
| U29 | connect_client | validates and registers configs; waits for connector/tasks; propagates failures and times out | green |
| U30 | seed | stable private secrets and synthetic product/financial rows; waits for Kafka snapshot and ClickHouse users/dictionary before staging events | green |
| U31 | reconcile | validates partition/database identifiers; stages deduped aggregate states and atomically replaces or drops one month | green |

## Contract (`-m contract`, no Docker)
| ID | Target | Behaviour | Status |
|---|---|---|---|
| C01 | fixtures | synthetic CSV headers == `event_time,user_id,session_id,event_type,channel,premium_amount` and `user_id,signup_date,city,device_type`; also == `data/*.csv` headers when present | green |
| C02 | connect/*.json | valid JSON, required keys, consistent converters | green |
| C03 | connect/*.json | no plaintext secrets (only `${file:...}`) | green |
| C04 | cross-file | Connect topic names == `kafka_topic_list` in SQL; Phase 3 removes the expected failure | green |
| C05 | sql/*.sql | deployment, job, and fixture SQL parses with sqlglot ClickHouse dialect; MySQL initialization is excluded | green |
| C06 | sql/*.sql | every explicit MergeTree has ORDER BY and PARTITION BY; Kafka tables have `kafka_handle_error_mode='stream'` | green |
| C07 | sql/*.sql | no `SELECT *` inside MVs; aggregate MV is installed before Kafka event forwarding starts | green |
| C08 | 070_governance | PII columns of order tables (`national_id`, `phone`, `plate_no`, `policy_no`) never granted to `role_analyst`; analysts only via `gov.*` | todo |
| C09 | compose.yml | all services have healthchecks; images pinned (no `latest`) | green |
| C10 | contracts | ClickHouse Kafka and persisted event table columns ⊇ contract fields with compatible types | green |
| C11 | contracts | schemas accept synthetic rows, enforce required fields/types/dates/enum/nonnegative premiums, and allow evolving dimensions | green |

## Integration (`-m integration`, Testcontainers)
| ID | Containers | Behaviour | Status |
|---|---|---|---|
| I01 | CH | all deployment `sql/` applies cleanly twice (idempotent) | green |
| I02 | CH | insert users then events → `count`, `uniq(user_id)`, `uniq(session_id)`, `sum/avg(premium_amount)` per (event_date, event_type, channel, city, device_type) equal Python fixture values; event keys match Python | green |
| I03 | CH | late event (old timestamp) lands in correct aggregate bucket | green |
| I04 | CH | event for a user_id absent from users → city/device_type = `'unknown'`; orphan collector reports ratio | green |
| I05 | CH | users ReplacingMergeTree: update + delete → latest state via FINAL; dictionary reflects it after reload | green |
| I06 | CH | 4 product tables → `orders_unified` contains union with correct `product_type` | todo |
| I07 | CH | purchase event + order with same (user_id, session_id) + financial row → one enriched row with all columns | todo |
| I08 | CH | non-purchase events never enter enriched table | todo |
| I09 | CH | order arriving after purchase event → fixed by repair/refreshable MV | todo |
| I10 | CH | governance: analyst sees masked PII, row policy limits partner rows, quota applies | todo |
| I11 | Kafka+CH | produce UTC events with additional fields → persisted events with topic, partition, offset | green |
| I12 | Kafka+CH | malformed JSON or timestamp → `dq.kafka_dead_letter`, pipeline keeps flowing | green |
| I13 | Kafka+CH / CH | duplicate delivery → one row in FINAL; repeatable partition repair also re-enriches late users, preserves other months, and clears empty months | green |
| I14 | CH | each DQ collector returns metrics of expected shape against seeded data | todo |
| I15 | CH+Spark | backfill `fill-gaps` fills a deleted hour; second run is a no-op | todo |
| I16 | CH+Spark | backfill `replace-partition` atomically swaps partition and rebuilds aggregates | todo |

## E2E (`-m e2e`, docker compose)
| ID | Behaviour | Status |
|---|---|---|
| E01 | stack healthy; connectors validate and reach RUNNING | green |
| E02 | seed users (Debezium snapshot complete in ClickHouse and dictionary ready) **before** events → counts reconcile with source; orphan ratio == expected from fixture | green |
| E03 | insert/update/delete a synthetic user in MySQL → latest state visible in ClickHouse via Debezium; updated city available in dictionary | green |
| E04 | stop Connect, produce, restart → no loss, no duplicates in FINAL view | todo |
| E05 | new events carry an extra column (e.g. `utm_campaign`) → drift check WARNs, rows still ingested (`input_format_skip_unknown_fields=1`) | todo |
| E06 | full DQ runner on healthy stack → all PASS; after deleting an hour → FAIL; after backfill → PASS | todo |
