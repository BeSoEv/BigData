import os
import uuid
from datetime import datetime, timezone

import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

from spark_session import create_spark_session

CATALOGUE_PATH = "s3a://cinema/catalogue/catalogue.jsonl"
SCD2_PATH = "s3a://cinema/curated/user_scd2/"


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ["CLICKHOUSE_DATABASE"],
    )

def validate_publish_interval(interval_start, interval_end):
    start = datetime.fromisoformat(interval_start.replace("Z", "+00:00"))
    end = datetime.fromisoformat(interval_end.replace("Z", "+00:00"))

    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("Границы интервала должны содержать часовой пояс")

    if start.utcoffset() != timezone.utc.utcoffset(start):
        raise ValueError("interval_start должен быть задан в UTC")

    if end.utcoffset() != timezone.utc.utcoffset(end):
        raise ValueError("interval_end должен быть задан в UTC")

    if start >= end:
        raise ValueError("interval_start должен быть меньше interval_end")

    if any((start.hour, start.minute, start.second, start.microsecond,
            end.hour, end.minute, end.second, end.microsecond)):
        raise ValueError("Пакетный интервал должен начинаться и заканчиваться в полночь UTC")

    return start, end

def delete_previous_results(client, interval_start, interval_end):
    start, end = validate_publish_interval(interval_start, interval_end)

    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")

    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    # поминутная витрина: удаляем строго по DateTime
    client.command(
        f"""
        ALTER TABLE cinema.batch_minute_metrics
        DELETE WHERE
            minute >= toDateTime64('{start_str}', 0, 'UTC')
            AND minute < toDateTime64('{end_str}', 0, 'UTC')
        """,
        settings={"mutations_sync": 2},
    )

    # дневные витрины: удаляем целые UTC-дни
    for table in (
        "batch_daily_metrics",
        "batch_daily_top5",
        "batch_search_frequencies",
    ):
        client.command(
            f"""
            ALTER TABLE cinema.{table}
            DELETE WHERE
                day >= toDate('{start_date}')
                AND day < toDate('{end_date}')
            """,
            settings={"mutations_sync": 2},
        )

def add_run_metadata(
    dataframe,
    run_id,
    interval_start,
    interval_end,
    processed_at,
):
    return (
        dataframe
        .withColumn("run_id", F.lit(run_id))
        .withColumn(
            "interval_start",
            F.to_timestamp(F.lit(interval_start)),
        )
        .withColumn(
            "interval_end",
            F.to_timestamp(F.lit(interval_end)),
        )
        .withColumn(
            "processed_at",
            F.to_timestamp(F.lit(processed_at)),
        )
    )


def insert_dataframe(
    client,
    dataframe,
    table,
    columns,
    batch_size=5000,
):
    rows = dataframe.select(*columns).toLocalIterator()
    batch = []
    inserted_count = 0

    for row in rows:
        batch.append(tuple(row[column] for column in columns))

        if len(batch) >= batch_size:
            client.insert(
                table,
                batch,
                column_names=columns,
            )
            inserted_count += len(batch)
            batch = []

    if batch:
        client.insert(
            table,
            batch,
            column_names=columns,
        )
        inserted_count += len(batch)

    return inserted_count


def publish_batch_results(
    results,
    interval_start,
    interval_end,
):
    validate_publish_interval(interval_start, interval_end)

    run_id = str(uuid.uuid4())
    processed_at = datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")

    client = get_clickhouse_client()

    table_config = {
        "minute_metrics": {
            "table": "batch_minute_metrics",
            "columns": [
                "minute",
                "content_id",
                "views_count",
                "watch_seconds",
            ],
        },
        "daily_metrics": {
            "table": "batch_daily_metrics",
            "columns": [
                "day",
                "country",
                "subscription_tier",
                "views_count",
                "watch_seconds",
            ],
        },
        "daily_top5": {
            "table": "batch_daily_top5",
            "columns": [
                "day",
                "content_id",
                "category",
                "views_count",
                "rank",
            ],
        },
        "search_frequencies": {
            "table": "batch_search_frequencies",
            "columns": [
                "day",
                "word",
                "word_count",
            ],
        },
    }

    # Все четыре DataFrame уже рассчитаны и проверены
    # внутри run_batch_processing.
    prepared = {}

    for name, config in table_config.items():
        dataframe = results[name]

        dataframe = add_run_metadata(
            dataframe,
            run_id,
            interval_start,
            interval_end,
            processed_at,
        )

        columns = config["columns"] + [
            "run_id",
            "interval_start",
            "interval_end",
            "processed_at",
        ]

        prepared[name] = (dataframe, config["table"], columns)

    # Удаляем старые результаты за интервал.
    delete_previous_results(
        client,
        interval_start,
        interval_end,
    )

    # Публикуем новые результаты.
    inserted_counts = {}

    for name, (dataframe, table, columns) in prepared.items():
        inserted_counts[name] = insert_dataframe(
            client,
            dataframe,
            table,
            columns,
        )

    print(f"Публикация завершена. run_id={run_id}")

    for name, count in inserted_counts.items():
        print(f"{name}: {count} строк")

    return run_id

# очистка и дедуп
def clean_events(spark: SparkSession):
    source_path = "s3a://cinema/raw/cinema.events/partition=*/*.jsonl"
    events = spark.read.json(source_path)
    events = (events
              .filter(F.col("event_id").isNotNull())
              .dropDuplicates(["event_id"])
              .withColumn("event_time", F.to_timestamp("event_time"))
              .filter(F.col("event_time").isNotNull())
              )

    return events

def get_views(events):
    return events.filter(F.col("event_type") == "view")

def get_search(events):
    return events.filter(F.col("event_type") == "search")

# join просмотров с каталоогом
def join_views_catalogue(views, catalogue):
    return views.join(catalogue, "content_id", "left")

# исторический join с scd2
def join_views_scd2(views, scd2):
    cond = ((views.user_id == scd2.user_id) &
            (views.event_time >= scd2.valid_from) &
            ((views.event_time < scd2.valid_to) | scd2.valid_to.isNull()))
    res = (
        views.alias("v")
        .join(scd2.alias("s"), cond, "left")
        .select("v.event_id",
                "v.event_time",
                "v.user_id",
                "v.content_id",
                "v.watch_time_seconds",
                "s.country",
                "s.subscription_tier")
    )

    return res

# общие поминутные показатели
def build_minute_metrics(views):
    minute_metrics = (
        views
        .withColumn("minute", F.date_trunc("minute", F.col("event_time")))
        .groupBy("minute", "content_id")
        .agg(
            F.count("*").alias("views_count"),
            F.sum("watch_time_seconds").alias("watch_seconds")
        )
        .orderBy("minute", "content_id")
    )

    return minute_metrics

# дневная витрина по стране и тарифу
def build_daily_metrics(views_with_scd2):
    daily_metrics = (
        views_with_scd2
        .withColumn("day", F.to_date(F.col("event_time")))
        .groupBy("day", "country", "subscription_tier")
        .agg(
            F.count("*").alias("views_count"),
            F.sum("watch_time_seconds").alias("watch_seconds")
        )
        .orderBy("day", "country", "subscription_tier")
    )
    return daily_metrics

# дневной топ-5 контента
def build_daily_top5(views_with_catalogue):
    daily_content = (
        views_with_catalogue
        .withColumn("day", F.to_date(F.col("event_time")))
        .groupBy("day", "content_id", "category")
        .agg(F.count("*").alias("views_count"))
    )

    window = Window.partitionBy("day").orderBy(
        F.desc("views_count"),
        F.asc("content_id")
    )

    top = (
        daily_content
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= 5)
        .orderBy("day", "rank")
    )

    return top

# частоты поисковых слов
def build_search_frequencies(searches):
    freqs = (
        searches
        .filter(F.col("query").isNotNull() & (F.trim(F.col("query")) != ""))
        .withColumn("day", F.to_date(F.col("event_time")))
        .withColumn(
            "word",
            F.explode(
                F.split(F.lower(F.col("query")), r"\s+")
            )
        )
        .filter(F.length(F.col("word")) > 4)
        .groupBy("day", "word")
        .agg(F.count("*").alias("word_count"))
        .orderBy("day", F.desc("word_count"), F.asc("word"))
    )

    return freqs

def _validate_unique_key(dataframe, key_columns, description):
    duplicates = (
        dataframe.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        raise ValueError(
            f"Проверка не пройдена: {description}"
            f"Найдены дубликаты по ключу {key_columns}"
        )


def _validate_join(joined, source_views, required_columns, description):
    source_count = source_views.select("event_id").distinct().count()
    joined_count = joined.count()
    distinct_joined_count = joined.select("event_id").distinct().count()

    if joined_count != source_count or distinct_joined_count != source_count:
        raise ValueError(
            f"Проверка JOIN с {description} не пройдена: "
            f"исходных просмотров={source_count}, строк после JOIN={joined_count}, "
            f"уникальных event_id после JOIN={distinct_joined_count}. "
            "Ожидалось ровно одно соответствие на каждый просмотр"
        )

    missing_condition = None
    for column in required_columns:
        condition = F.col(column).isNull()
        missing_condition = condition if missing_condition is None else (missing_condition | condition)

    missing_count = joined.filter(missing_condition).count()
    if missing_count:
        raise ValueError(
            f"Проверка JOIN с {description} не пройдена: "
            f"{missing_count} просмотров не имеют полного соответствия"
        )

def run_batch_processing(
    spark: SparkSession,
    interval_start: str | None = None,
    interval_end: str | None = None,
):
    if interval_start is not None and interval_end is not None:
        if interval_start >= interval_end:
            raise ValueError("interval_start должен быть меньше interval_end")

    events = clean_events(spark)
    if interval_start is not None:
        events = events.filter(F.col("event_time") >= F.to_timestamp(F.lit(interval_start)))
    if interval_end is not None:
        events = events.filter(F.col("event_time") < F.to_timestamp(F.lit(interval_end)))

    views = get_views(events)
    searches = get_search(events)

    catalogue = spark.read.json(CATALOGUE_PATH)
    scd2 = spark.read.parquet(SCD2_PATH)

    # до join проверяем уникальность справочников по ключам соединения
    _validate_unique_key(catalogue, ["content_id"], "каталог content_id")

    views_with_catalogue = join_views_catalogue(views, catalogue)
    _validate_join(
        views_with_catalogue,
        views,
        ["category"],
        "каталогом",
    )

    views_with_scd2 = join_views_scd2(views, scd2)
    _validate_join(
        views_with_scd2,
        views,
        ["country", "subscription_tier"],
        "историческим SCD2",
    )

    minute_metrics = build_minute_metrics(views)
    daily_metrics = build_daily_metrics(views_with_scd2)
    daily_top5 = build_daily_top5(views_with_catalogue)
    search_frequencies = build_search_frequencies(searches)

    # Проверяем уникальность бизнес-ключей всех четырёх витрин.
    _validate_unique_key(minute_metrics, ["minute", "content_id"], "поминутная витрина")
    _validate_unique_key(daily_metrics, ["day", "country", "subscription_tier"], "дневная витрина")
    _validate_unique_key(daily_top5, ["day", "content_id"], "дневной топ-5")
    _validate_unique_key(search_frequencies, ["day", "word"], "частоты поисковых слов")

    return {
        "minute_metrics": minute_metrics,
        "daily_metrics": daily_metrics,
        "daily_top5": daily_top5,
        "search_frequencies": search_frequencies,
    }



if __name__ == "__main__":
    spark = create_spark_session("batch-processing")

    try:
        interval_start = "2026-01-01T00:00:00Z"
        interval_end = "2026-01-04T00:00:00Z"

        results = run_batch_processing(
            spark,
            interval_start,
            interval_end,
        )

        publish_batch_results(
            results,
            interval_start,
            interval_end,
        )

    finally:
        spark.stop()
