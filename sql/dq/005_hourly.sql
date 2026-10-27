SELECT toStartOfHour(event_time) AS hour, count() AS events FROM core.user_events FINAL
WHERE toStartOfHour(event_time) IN {source_hours:Array(DateTime('UTC'))}
    AND event_time < {as_of:DateTime64(6, 'UTC')}
GROUP BY hour ORDER BY hour;
