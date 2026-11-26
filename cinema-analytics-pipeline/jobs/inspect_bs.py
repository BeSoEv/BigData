from spark_session import create_spark_session
from pyspark.sql import functions as F

def main():
    spark = create_spark_session("inspect-batch-sources")

    try:
        events_path = (
            "s3a://cinema/raw/"
            "cinema.events/partition=*/*.jsonl"
        )
        catalogue_path = "s3a://cinema/catalogue/catalogue.jsonl"

        events = spark.read.json(events_path)
        duplicates = (
            events
            .groupBy("event_id")
            .agg(
                F.count("*").alias("copies"),
                F.countDistinct(
                    F.struct(
                        "event_time",
                        "user_id",
                        "event_type",
                        "content_id",
                        "watch_time_seconds",
                        "query",
                    )
                ).alias("unique_payloads"),
            )
            .filter(F.col("copies") > 1)
        )

        conflicts = duplicates.filter(
            F.col("unique_payloads") > 1
        )

        print("Количество event_id с разным содержимым:", conflicts.count())
        catalogue = spark.read.json(catalogue_path)

        print("=== EVENTS SCHEMA ===")
        events.printSchema()
        print("Количество событий:", events.count())
        events.show(5, truncate=False)

        print("=== CATALOGUE SCHEMA ===")
        catalogue.printSchema()
        print("Количество записей каталога:", catalogue.count())
        catalogue.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
