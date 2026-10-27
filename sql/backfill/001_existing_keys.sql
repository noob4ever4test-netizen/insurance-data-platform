SELECT event_key FROM core.user_events FINAL
WHERE event_time >= {start:DateTime('UTC')} AND event_time < {end:DateTime('UTC')};
