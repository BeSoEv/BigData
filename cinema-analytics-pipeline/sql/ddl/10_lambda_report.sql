CREATE TABLE IF NOT EXISTS cinema.report_watermark
(
    updated_at DateTime64(3, 'UTC'),
    watermark DateTime64(0, 'UTC')
)
ENGINE = MergeTree
ORDER BY updated_at;


CREATE VIEW IF NOT EXISTS cinema.lambda_report
AS
WITH
    (
        SELECT argMax(watermark, updated_at)
        FROM cinema.report_watermark
    ) AS T
SELECT
    minute,
    content_id,
    sum(views_count) AS views_count,
    sum(watch_seconds) AS watch_seconds
FROM
(
    SELECT
        minute,
        content_id,
        views_count,
        watch_seconds
    FROM cinema.batch_minute_metrics
    WHERE minute < T

    UNION ALL

    SELECT
        minute,
        content_id,
        views_count,
        watch_seconds
    FROM cinema.realtime_views_report
    WHERE minute >= T
)
GROUP BY
    minute,
    content_id;