SELECT raw_message FROM dq.event_samples FINAL
WHERE topic = {event_topic:String} AND captured_at >= {window_start:DateTime('UTC')}
    AND captured_at < {as_of:DateTime64(6, 'UTC')}
ORDER BY captured_at DESC, partition, offset LIMIT 1000;
