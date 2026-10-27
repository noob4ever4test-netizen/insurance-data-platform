SELECT hour, events FROM dq.hourly_event_metrics
WHERE hour >= {target_hour:DateTime('UTC')} - INTERVAL 7 DAY
    AND hour <= {target_hour:DateTime('UTC')}
    AND toHour(hour) = toHour({target_hour:DateTime('UTC')})
ORDER BY hour;
