
#!/bin/bash

set -e

SQL_FILES=(
    sql/ddl/01_database.sql
    sql/ddl/02_kafka_events.sql
    sql/ddl/03_realtime_views.sql
    sql/ddl/05_realtime_views_report.sql
    sql/ddl/04_realtime_views_mv.sql
    sql/ddl/06_batch_minute_metrics.sql
    sql/ddl/07_batch_daily_metrics.sql
    sql/ddl/08_batch_daily_top5.sql
    sql/ddl/09_batch_search_frequencies.sql
)

for file in "${SQL_FILES[@]}"; do
    echo "Executing $file"

    docker compose exec -T clickhouse clickhouse-client \
        --multiquery < "$file"
done

echo "ClickHouse initialization completed"
