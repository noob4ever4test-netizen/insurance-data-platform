SELECT count() AS rows, uniqExact(event_key) AS unique_keys FROM core.user_events
WHERE event_time >= {window_start:DateTime('UTC')}
    AND event_time < {as_of:DateTime64(6, 'UTC')};
