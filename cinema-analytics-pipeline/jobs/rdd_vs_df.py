
import json
from datetime import datetime

from pyspark.sql import functions as F

from spark_session import create_spark_session
from batch_processing import (
    clean_events,
    get_views,
    get_search,
    join_views_catalogue,
    build_daily_top5,
    build_search_frequencies,
)


EVENTS_PATH = "s3a://cinema/raw/cinema.events/partition=*/*.jsonl"
CATALOGUE_PATH = "s3a://cinema/catalogue/catalogue.jsonl"

# rdd
def parse_event(line):
    return json.loads(line)

def get_event_day(event):
    event_time = event.get("event_time")
    if event_time is None:
        return None

    try:
        if event_time.endswith("Z"):
            event_time = event_time[:-1] + "+00:00"
        return datetime.fromisoformat(event_time).date()

    except (ValueError, TypeError):
        return None


def calculate_rdd(events_rdd, catalogue_rdd):
    # дедуп по event_id
    unique_events = (
        events_rdd
        .filter(lambda e: e.get("event_id") is not None)
        .map(lambda e: (e["event_id"], e))
        .reduceByKey(lambda a, b: a)
        .values()
        .map(lambda e: (e, get_event_day(e)))
        .filter(lambda x: x[1] is not None)
        .map(lambda x: {**x[0], "_event_day": x[1]})
    )

    catalogue = dict(
        catalogue_rdd
        .map(lambda c: (int(c["content_id"]), c.get("category")))
        .collect()
    )

    # частоты поисковых слов
    search_words = (
        unique_events
        .filter(lambda e: e.get("event_type") == "search")
        .filter(lambda e: e.get("query") is not None)
        .flatMap(
            lambda e: [
                ((e["_event_day"], word), 1)
                for word in e["query"].lower().split()
                if len(word) > 4
            ]
        )
    )

    search_frequencies = (
        search_words
        .reduceByKey(lambda a, b: a + b)
        .map(lambda x: (str(x[0][0]), x[0][1], x[1]))
        .collect()
    )

    # количество просмотров по дням и content_id
    daily_views = (
        unique_events
        .filter(lambda e: e.get("event_type") == "view")
        .map(
            lambda e: (
                (e["_event_day"], int(e["content_id"])),
                1
            )
        )
        .reduceByKey(lambda a, b: a + b)
    )

    # дневной топ-5 контента
    daily_top5 = (
        daily_views
        .map(
            lambda x: (
                x[0][0],
                (x[0][1], catalogue.get(x[0][1]), x[1])
            )
        )
        .groupByKey()
        .flatMap(
            lambda x: [
                (str(x[0]), content_id, category, views_count, rank)
                for rank, (content_id, category, views_count)
                in enumerate(
                    sorted(
                        x[1],
                        key=lambda item: (-item[2], item[0])
                    )[:5],
                    start=1
                )
            ]
        )
        .collect()
    )

    return search_frequencies, daily_top5


def main():
    spark = create_spark_session("rdd-vs-dataframe")

    try:
        # df
        events = clean_events(spark)
        views = get_views(events)
        searches = get_search(events)

        catalogue_df = spark.read.json(CATALOGUE_PATH)
        views_with_catalogue = join_views_catalogue(views, catalogue_df)
        df_search = build_search_frequencies(searches)
        df_top5 = build_daily_top5(views_with_catalogue)

        df_search_result = {
            (str(row["day"]), row["word"], row["word_count"])
            for row in df_search.collect()
        }

        df_top5_result = {
            (
                str(row["day"]),
                int(row["content_id"]),
                row["category"],
                int(row["views_count"]),
                int(row["rank"])
            )
            for row in df_top5.collect()
        }

        # rdd
        events_rdd = spark.sparkContext.textFile(EVENTS_PATH).map(parse_event)
        catalogue_rdd = spark.sparkContext.textFile(CATALOGUE_PATH).map(parse_event)
        rdd_search, rdd_top5 = calculate_rdd(events_rdd, catalogue_rdd)

        rdd_search_result = set(rdd_search)
        rdd_top5_result = set(rdd_top5)

        print("\nSEARCH FREQUENCIES\n")
        print("DataFrame:", len(df_search_result), "rows")
        print("RDD:", len(rdd_search_result), "rows")
        print("Results match:", df_search_result == rdd_search_result)

        print("\nDAILY TOP-5\n")
        print("DataFrame:", len(df_top5_result), "rows")
        print("RDD:", len(rdd_top5_result), "rows")
        print("Results match:", df_top5_result == rdd_top5_result)

        assert df_search_result == rdd_search_result, (
            "rdd_vs_df: частоты поисковых слов не совпадают"
        )

        assert df_top5_result == rdd_top5_result, (
            "rdd_vs_df: топ-5 не совпадают"
        )

        print("\nrdd_vs_df: все совпадает")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
