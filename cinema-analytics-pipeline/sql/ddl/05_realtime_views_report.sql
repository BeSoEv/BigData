CREATE VIEW IF NOT EXISTS cinema.realtime_views_report
AS
SELECT
    minute,
    content_id,
    --уник. просмотры
    length(unique_events) AS views_count,
    --сум. время просмотра
    arraySum(
        arrayMap(
            --извлекаем watch_time_seconds из каждого кортежа (event_id, watch_time_seconds) в unique_events
            event -> tupleElement(event, 2),
            unique_events
        )
    ) AS watch_seconds
FROM
(
    SELECT
        minute,
        content_id,
        --объедин. агрег. сост
        groupUniqArrayMerge(unique_view_events) AS unique_events
    FROM cinema.realtime_views
    GROUP BY
        minute,
        content_id
);