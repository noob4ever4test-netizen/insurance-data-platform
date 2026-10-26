TRUNCATE TABLE mart.user_event_agg_rebuild;

INSERT INTO mart.user_event_agg_rebuild
SELECT toDate(event_time) AS event_date, event_type, channel,
    dictGetOrDefault('core.users_dict', 'city', user_id, 'unknown') AS city,
    dictGetOrDefault('core.users_dict', 'device_type', user_id, 'unknown') AS device_type,
    countState() AS event_count, uniqState(user_id) AS user_count,
    uniqState(session_id) AS session_count, sumState(premium_amount) AS premium_sum,
    avgState(premium_amount) AS premium_avg
FROM core.user_events FINAL
WHERE toYYYYMM(event_time) = {partition:UInt32}
GROUP BY event_date, event_type, channel, city, device_type;
