CREATE TABLE IF NOT EXISTS cinema.batch_minute_metrics
(
    minute DateTime64(0, 'UTC'),
    content_id Int64,
    views_count UInt64,
    watch_seconds UInt64,
    run_id String,
    interval_start DateTime64(0, 'UTC'),
    interval_end DateTime64(0, 'UTC'),
    processed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, content_id);