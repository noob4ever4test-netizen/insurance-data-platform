SELECT count()
FROM mart.user_event_agg_rebuild
WHERE toYYYYMM(event_date) = {partition:UInt32};
