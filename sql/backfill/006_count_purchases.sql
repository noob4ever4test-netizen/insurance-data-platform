SELECT count() FROM mart.purchase_events_backfill
WHERE toYYYYMM(event_date) = {partition:UInt32};
