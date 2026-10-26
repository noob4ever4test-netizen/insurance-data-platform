# Data contract (from sample CSVs)

## user_events.csv
| column | type | notes |
|---|---|---|
| event_time | DateTime `YYYY-MM-DD HH:MM:SS`, no tz | treat as UTC by default; tz configurable (`EVENTS_TZ`, e.g. Asia/Tehran) and documented |
| user_id | Int64 | FK to users.user_id; **sample ids (610–4987) do not overlap users sample (1–9)** → expect orphans on the sample; verify on full data |
| session_id | String (`s_<n>`) | |
| event_type | enum: `purchase`, `policy_click`, `quote_view` | contract rejects others (WARN in drift check, not drop) |
| channel | LowCardinality String: Google Ads, Organic, Referral, Social, SMS | |
| premium_amount | Int64 (Rials) | present on all event types; `>= 0` |

The CSV contract keeps the space-separated timestamp shown above. The Python fallback producer
validates that representation before interpreting it in `EVENTS_TZ` (default `UTC`), with
`--timezone` taking precedence. Kafka JSON carries canonical UTC `YYYY-MM-DDTHH:MM:SSZ` instead.
`validate_event(row)` checks CSV-typed records; `validate_event(row, wire=True)` selects the
`$defs.utc_event_time` definition in `user_event.schema.json` and checks normalized JSON.
Both custom timestamp format checkers validate calendar dates. Other fields have the same
contract in both representations. New columns remain available for later drift detection;
an unknown `event_type` still fails the enum and is reported as a rejected producer row.

**No `event_id` column.** Derive a deterministic key:
`event_key = cityHash64(user_id, session_id, event_time, event_type)` with ClickHouse argument types
`Int64, String, DateTime, String`. Python uses `clickhouse-cityhash` (CityHash v1.0.2) for UTF-8 strings,
ClickHouse's seeded integer mixing for the user ID and DateTime's UTC epoch seconds, and ordered
`Hash128to64` combination. Naive timestamps mean UTC; aware Python datetimes normalize to UTC.
Spark must use this same algorithm; hashing a joined string with xxhash or SHA-1 produces a different key.
Used for dedup, duplicate DQ check and backfill anti-join.

**No order_id on events** → purchase events link to order tables on `(user_id, session_id)` (orders are assumed to carry
the session that created them) with `created_at` within ±1 h of `event_time` as a sanity filter. State this assumption in the report.

## users.csv
| column | type | notes |
|---|---|---|
| user_id | Int64 | PK |
| signup_date | Date | |
| city | LowCardinality String: Mashhad, Isfahan, Shiraz, Tabriz, Tehran | |
| device_type | LowCardinality String: Desktop, Mobile, Tablet | |

No `updated_at` → ReplacingMergeTree version = Debezium `ts_ms`; CDC-lag check uses Debezium heartbeat/`ts_ms`, not a source column.
No PII in users. PII for governance tests lives in the (invented) order tables: `national_id`, `phone`, `plate_no` (third),
`policy_no`; `premium_amount` is treated as financially sensitive (partner roles see only their rows).

## Aggregation (Part 1)
`mart.user_event_agg` keyed by `event_date, event_type, channel, city, device_type`:
`count`, `uniq(user_id)`, `uniq(session_id)`, `sum(premium_amount)`, `avg(premium_amount)`.

## Purchase enrichment (Part 2, derived tables)

The invented product tables retain their native fields and are unioned by four MVs into
`core.orders_unified`. Identity is `(product_type, order_id)`; mutable user, session,
timestamp and partner attributes are excluded from the replacement key. Shared columns
include the synthetic PII and order premium. Product-specific values are represented as
`Map(String, String)` entries: `plate_no`, `car_value`, `coverage_limit`, `property_value`.
The source numeric columns remain Int64; map values use decimal strings.

Purchase attribution requires exactly one non-deleted order with a matching non-deleted
financial row for `(user_id, session_id)`. Financial identity also matches the order's
`product_type` and `order_id`, preventing collisions across products. Sessions with
multiple fully matched orders are left unenriched rather than attributed arbitrarily.
The order timestamp must be within ±3600 seconds of the event, inclusive. Neither a
payment-status filter nor a premium-equality constraint is imposed: event premium,
order premium, financial amount and payment status are retained separately.

The supplied MySQL DATETIME columns have zero fractional precision. Debezium's default
adaptive-time encoding represents these as milliseconds since the Unix epoch, converted
to UTC DateTime in the ClickHouse CDC MVs. A future migration to DATETIME(4–6) would
require changing this conversion to the connector's microsecond representation.

`mart.purchase_events_enriched` uses the event identity for replacement, preserving
one active row per event despite duplicate deliveries or repaired attributes. Read with
`FINAL WHERE _is_deleted = 0`. Purchase inserts without a valid attribution write a
tombstone; subsequent repair can replace it with a complete row. The minute refresh
recomputes FINAL purchases from the first day of two months ago in UTC onward, including
future-dated events. Older corrections require the explicit monthly repair SQL.
