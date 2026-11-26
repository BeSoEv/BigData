CREATE TABLE IF NOT EXISTS cinema.realtime_views
(
    minute DateTime64(0, 'UTC'),
    content_id Int64,
    unique_view_events AggregateFunction(
        groupUniqArray,
        Tuple(String, UInt64)
    )
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, content_id);