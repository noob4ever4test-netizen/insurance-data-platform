SELECT count() AS purchases,
    countIf(event_key NOT IN (
        SELECT event_key FROM mart.purchase_events_enriched FINAL WHERE _is_deleted = 0
    )) AS unmatched
FROM core.user_events FINAL
WHERE event_type = 'purchase' AND event_time >= {window_start:DateTime('UTC')}
    AND event_time < {as_of:DateTime64(6, 'UTC')};
