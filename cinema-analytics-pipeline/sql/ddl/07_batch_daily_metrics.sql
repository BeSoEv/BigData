CREATE TABLE IF NOT EXISTS cinema.batch_daily_metrics
(
    day Date,
    country String,
    subscription_tier String,
    views_count UInt64,
    watch_seconds UInt64,
    run_id String,
    interval_start DateTime64(0, 'UTC'),
    interval_end DateTime64(0, 'UTC'),
    processed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, country, subscription_tier);