CREATE TABLE IF NOT EXISTS cinema.batch_daily_top5
(
    day Date,
    content_id Int64,
    category String,
    views_count UInt64,
    rank UInt8,
    run_id String,
    interval_start DateTime64(0, 'UTC'),
    interval_end DateTime64(0, 'UTC'),
    processed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, content_id);