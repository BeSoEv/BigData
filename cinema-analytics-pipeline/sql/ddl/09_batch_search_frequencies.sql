CREATE TABLE IF NOT EXISTS cinema.batch_search_frequencies
(
    day Date,
    word String,
    word_count UInt64,
    run_id String,
    interval_start DateTime64(0, 'UTC'),
    interval_end DateTime64(0, 'UTC'),
    processed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, word);