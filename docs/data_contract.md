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
