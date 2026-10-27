SELECT topic, partition, min_offset, max_offset, offset_count
FROM dq.offset_metrics WHERE topic = {event_topic:String} ORDER BY topic, partition;
