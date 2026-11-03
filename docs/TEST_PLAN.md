# Test plan

Status: `todo | red | green`. Test function name = `test_<ID>_<desc>`.
Columns follow `docs/data_contract.md` (real headers). `event_key` is derived — see contract.

## Unit (`-m unit`, no Docker)
| ID | Module | Behaviour | Status |
|---|---|---|---|
| U33 | demo | start preserves populated stacks and restores existing optional brokers; stop retains topology; reset requires confirmation; replay waits for committed offsets and fixture keys (including quoted UInt64 JSON); repairs resume consumers after failure; maintenance SQL is allowlisted; DQ displays persisted results on check failure | green |
| U01 | contracts | valid event row passes `user_event.schema.json` | green |
| U02 | contracts | parametrized invalid rows fail: missing `event_time`, null `user_id`, bad timestamp, unknown `event_type`, negative `premium_amount` | green |
| U20 | contracts | `event_key(user_id, session_id, event_time, event_type)` is deterministic and equals the ClickHouse `cityHash64` value for a fixed vector | green |
| U03 | producer | Kafka key = `user_id`, value = JSON matching contract | green |
| U04 | producer | `YYYY-MM-DD HH:MM:SS` parsed with configurable tz and emitted as UTC ISO-8601 | green |
| U05 | producer | malformed CSV row → routed to error list, not raised | green |
| U06 | contracts | `schema_diff(old, new)` reports added / removed / type-changed fields | green |
| U07 | dq.freshness | event/ingestion freshness and CDC heartbeat age; threshold boundaries, missing evidence, future clocks | green |
| U08 | dq.lag | consumer lag per partition aggregated; worst-partition thresholds, uncommitted offsets, broker offset errors | green |
| U09 | dq.offset_gaps | detects gap from (min, max, distinct offset count) per topic/partition | green |
| U10 | dq.volume | anomaly vs trailing same-hour median ± k·MAD; flat-series floor and insufficient history | green |
| U11 | dq.orphans | orphan ratio computed and thresholded after users load; empty events | green |
| U12 | dq.schema_drift | new field → WARN, missing required/type changes or malformed samples → FAIL; no payload values in results | green |
| U13 | dq.duplicates | physical duplicates = count − uniqExact(event_key); ratio boundaries | green |
| U14 | dq.runner | validated thresholds/source manifests; all 12 checks, additional rate/load/reconciliation checks, persistence, JSON CLI and failure exit codes | green |
| U15 | backfill.transforms | CSV/JSON validation separates invalid rows, counts rejects, normalizes CSV timezone; rejects malformed JSON and wrong wire types | green |
| U16 | backfill.transforms | dedupe by `event_key` (keep first); Spark `event_key` == Python `event_key` on same rows | green |
| U17 | backfill.transforms | missing rows = left-anti join on `event_key` vs existing keys | green |
| U18 | backfill.transforms | UTC date-range filter is [start, end); invalid/naive bounds rejected | green |
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
| U30 | seed | stable private secrets and synthetic product/financial rows; reads all snapshot partitions from the beginning without group assignment; waits for ClickHouse users, joined orders/financial rows, and matching dictionaries before staging events | green |
| U31 | reconcile | validates partition/database identifiers; stages deduped aggregate states and atomically replaces or drops one month | green |
| U32 | backfill.job | validates options before database access; bounded Kafka batch options; CLI prints JSON and closes resources | green |

## Contract (`-m contract`, no Docker)
| ID | Target | Behaviour | Status |
|---|---|---|---|
| C01 | fixtures | synthetic CSV headers == `event_time,user_id,session_id,event_type,channel,premium_amount` and `user_id,signup_date,city,device_type`; also == `data/*.csv` headers when present | green |
| C02 | connect/*.json | valid JSON, required keys, consistent converters | green |
| C03 | connect/*.json | no plaintext secrets (only `${file:...}`) | green |
| C04 | cross-file | event topic and all six Connect CDC topics have matching `kafka_topic_list` in SQL | green |
| C05 | sql/*.sql | deployment, job, and fixture SQL parses with sqlglot ClickHouse dialect; MySQL initialization is excluded | green |
| C06 | sql/*.sql | every explicit MergeTree has ORDER BY and PARTITION BY; Kafka tables have `kafka_handle_error_mode='stream'` | green |
| C07 | sql/*.sql | no `SELECT *` inside MVs; aggregate and purchase MVs are installed before Kafka event forwarding starts | green |
| C08 | 070_governance | PII columns of order tables (`national_id`, `phone`, `plate_no`, `policy_no`) never granted to `role_analyst`; analysts only via `gov.*` | green |
| C09 | compose.yml | all services have healthchecks; images pinned (no `latest`) | green |
| C10 | contracts | ClickHouse Kafka and persisted event table columns ⊇ contract fields with compatible types | green |
| C11 | contracts | schemas accept synthetic rows, enforce required fields/types/dates/enum/nonnegative premiums, and allow evolving dimensions | green |
| C12 | documentation | root GitHub README and report links resolve; existing guides linked; phase notes preserved; architecture connects source, governed consumer, DQ, and Spark recovery paths | green |

## Integration (`-m integration`, Testcontainers)
| ID | Containers | Behaviour | Status |
|---|---|---|---|
| I01 | CH | all deployment `sql/` applies cleanly twice (idempotent) | green |
| I02 | CH | insert users then events → `count`, `uniq(user_id)`, `uniq(session_id)`, `sum/avg(premium_amount)` per (event_date, event_type, channel, city, device_type) equal Python fixture values; event keys match Python | green |
| I03 | CH | late event (old timestamp) lands in correct aggregate bucket | green |
| I04 | CH | event for a user_id absent from users → city/device_type = `'unknown'`; orphan collector reports ratio | green |
| I05 | CH | users ReplacingMergeTree: update + delete → latest state via FINAL; dictionary reflects it after reload | green |
| I06 | CH / Kafka+CH | 4 product MVs preserve common fields, typed extras, updates/deletes; Kafka CDC converts MySQL DATETIME milliseconds and retains offsets | green |
| I07 | CH | all products enrich one FINAL purchase row with event/user/order/financial fields; enforce ±1 h, full financial identity, and unambiguous session attribution | green |
| I08 | CH | non-purchase events never enter enriched table | green |
| I09 | CH | refresh repairs late orders/financial rows, updates/deletes and duplicates; recent scan prunes old partitions, explicit monthly SQL repairs history | green |
| I10 | CH | governance: analyst sees masked PII, row policy limits partner rows, quota applies | green |
| I11 | Kafka+CH | produce UTC events with additional fields → persisted events with topic, partition, offset | green |
| I12 | Kafka+CH | malformed JSON or timestamp → `dq.kafka_dead_letter`, pipeline keeps flowing | green |
| I13 | Kafka+CH / CH | duplicate delivery → one row in FINAL; repeatable partition repair also re-enriches late users, preserves other months, and clears empty months | green |
| I14 | CH / Kafka+CH | all DQ metric shapes and persisted results; historical source-manifest hours, real broker group offsets and heartbeat MV; engineer can run without access management | green |
| I15 | CH+Spark / Kafka | fills deleted hour, dry-run preserves events, second run writes zero; bounded two-partition Kafka replay retains offsets; interrupted batches audit acknowledged writes and retry only missing keys | green |
| I16 | CH+Spark | staged monthly replacement/drop preserves other months, rebuilds aggregates and purchases, is repeatable, rejects invalid sources before mutation | green |

## E2E (`-m e2e`, docker compose)
| ID | Behaviour | Status |
|---|---|---|
| E01 | stack healthy; connectors validate and reach RUNNING | green |
| E02 | seed users and all product/financial tables (ClickHouse and dictionaries ready) **before** events → event/aggregate counts, orphans and enriched purchases reconcile with fixtures through real CDC | green |
| E03 | insert/update/delete a synthetic user in MySQL → latest state visible in ClickHouse via Debezium; updated city available in dictionary | green |
| E04 | stop Connect, stage synthetic CSV during outage, restart → all events arrive once in FINAL view | green |
| E05 | new events carry an extra column (e.g. `utm_campaign`) → drift check WARNs, both evolved and subsequent events are ingested without dead letters | green |
| E06 | full DQ runner on healthy stack → all 12 PASS; after deleting an hour → reconciliation FAIL; after Spark backfill → all PASS | green |
