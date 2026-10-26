CREATE TABLE IF NOT EXISTS mart.user_event_agg
(
    event_date Date,
    event_type LowCardinality(String),
    channel LowCardinality(String),
    city LowCardinality(String),
    device_type LowCardinality(String),
    event_count AggregateFunction(count),
    user_count AggregateFunction(uniq, Int64),
    session_count AggregateFunction(uniq, String),
    premium_sum AggregateFunction(sum, Int64),
    premium_avg AggregateFunction(avg, Int64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, channel, city, device_type);

CREATE TABLE IF NOT EXISTS mart.user_event_agg_rebuild AS mart.user_event_agg;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.user_event_agg_mv TO mart.user_event_agg AS
SELECT toDate(event_time) AS event_date, event_type, channel,
    dictGetOrDefault('core.users_dict', 'city', user_id, 'unknown') AS city,
    dictGetOrDefault('core.users_dict', 'device_type', user_id, 'unknown') AS device_type,
    countState() AS event_count, uniqState(user_id) AS user_count,
    uniqState(session_id) AS session_count, sumState(premium_amount) AS premium_sum,
    avgState(premium_amount) AS premium_avg
FROM core.user_events
GROUP BY event_date, event_type, channel, city, device_type;
