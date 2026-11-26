CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.mv_kafka_events_to_realtime_views
TO cinema.realtime_views
AS
SELECT
    -- время до минуты
    toStartOfMinute(
        parseDateTime64BestEffort(event_time, 0, 'UTC')
    ) AS minute,
    assumeNotNull(content_id) AS content_id,
    --форм. состояние уник. просмотров
    --дедуп по (event_id, watch_time_seconds)
    groupUniqArrayState(
        tuple(
            event_id,
            toUInt64(assumeNotNull(watch_time_seconds))
        )
    ) AS unique_view_events

FROM cinema.kafka_events
WHERE event_type = 'view'
  AND isNotNull(content_id)
  AND isNotNull(watch_time_seconds)
  AND watch_time_seconds >= 0
GROUP BY
    minute,
    content_id;