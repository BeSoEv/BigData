CREATE TABLE IF NOT EXISTS cinema.kafka_events
(
    event_id String,
    event_time String,
    user_id Int64,
    event_type String,
    content_id Nullable(Int64),
    watch_time_seconds Nullable(Int64),
    query Nullable(String),
    available_at String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'cinema.events',
    kafka_group_name = 'cinema_clickhouse_events',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;