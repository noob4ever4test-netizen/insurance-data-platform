SELECT
    (SELECT count() FROM dq.kafka_dead_letter
     WHERE topic = {event_topic:String} AND ingested_at >= {window_start:DateTime('UTC')}
        AND ingested_at < {as_of:DateTime64(6, 'UTC')}) AS errors,
    (SELECT uniqExact(tuple(partition, offset)) FROM dq.event_samples FINAL
     WHERE topic = {event_topic:String} AND captured_at >= {window_start:DateTime('UTC')}
        AND captured_at < {as_of:DateTime64(6, 'UTC')}) AS received;
