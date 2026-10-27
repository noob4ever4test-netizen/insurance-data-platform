CREATE TABLE IF NOT EXISTS core.user_events_staging AS core.user_events;

CREATE TABLE IF NOT EXISTS mart.purchase_events_backfill AS mart.purchase_events_enriched;

CREATE TABLE IF NOT EXISTS dq.backfill_runs
(
    run_id UUID,
    source LowCardinality(String),
    mode LowCardinality(String),
    status LowCardinality(String),
    range_start DateTime('UTC'),
    range_end DateTime('UTC'),
    rows_read UInt64,
    invalid UInt64,
    missing UInt64,
    written UInt64,
    error String,
    started_at DateTime64(6, 'UTC'),
    finished_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (started_at, run_id);
