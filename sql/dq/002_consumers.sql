SELECT database, table, consumer_id, topics, partitions, offsets
FROM dq.kafka_consumer_metrics WHERE database = {raw_db:String};
