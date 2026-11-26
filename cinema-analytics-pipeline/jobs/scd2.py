from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from spark_session import create_spark_session



def validate_scd2_intervals(scd2):
    # Проверяем уникальность ключа (user_id, valid_from).
    duplicates = (
        scd2.groupBy("user_id", "valid_from")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count() > 0:
        raise ValueError(
            "SCD2: обнаружены дубликаты по ключу (user_id, valid_from)"
        )

    # Проверяем корректность границ интервалов.
    invalid_intervals = scd2.filter(
        F.col("user_id").isNull()
        | F.col("valid_from").isNull()
        | F.col("country").isNull()
        | F.col("subscription_tier").isNull()
        | (
            F.col("valid_to").isNotNull()
            & (F.col("valid_to") <= F.col("valid_from"))
        )
    )

    if invalid_intervals.limit(1).count() > 0:
        raise ValueError("SCD2: обнаружены некорректные интервалы")

    # Проверяем отсутствие пересечений интервалов.
    window = Window.partitionBy("user_id").orderBy("valid_from")

    checked = scd2.withColumn(
        "next_valid_from",
        F.lead("valid_from").over(window),
    )

    overlaps = checked.filter(
        F.col("next_valid_from").isNotNull()
        & (
            F.col("valid_to").isNull()
            | (F.col("valid_to") > F.col("next_valid_from"))
        )
    )

    if overlaps.limit(1).count() > 0:
        raise ValueError("SCD2: обнаружены пересекающиеся интервалы")

    print("SCD2: проверки пройдены")


def build_scd2(spark: SparkSession):
    source_path = (
        "s3a://cinema/raw/"
        "cinema.profile_changes/partition=*/*.jsonl"
    )
    target_path = "s3a://cinema/curated/user_scd2/"

    changes = spark.read.json(source_path)
    changes = changes.select(
        F.col("change_id").cast("string"),
        F.col("user_id").cast("long"),
        F.to_timestamp("effective_at").alias("effective_at"),
        F.col("country").cast("string"),
        F.col("subscription_tier").cast("string"),
    )

    required_fields = [
        "change_id",
        "user_id",
        "effective_at",
        "country",
        "subscription_tier",
    ]

    null_condition = None
    for column in required_fields:
        condition = F.col(column).isNull()
        null_condition = (
            condition if null_condition is None
            else null_condition | condition
        )

    if changes.filter(null_condition).limit(1).count() > 0:
        raise ValueError("Обнаружены NULL в обязательных полях")


    changes = changes.dropDuplicates(["change_id"])
    duplicate_timestamps = (
        changes
        .groupBy("user_id", "effective_at")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicate_timestamps.limit(1).count() > 0:
        raise ValueError(
            "Обнаружены несколько изменений одного пользователя "
            "с одинаковым effective_at"
        )
    window = Window.partitionBy("user_id").orderBy("effective_at")
    changes = (
        changes
        .withColumn("prev_country", F.lag("country").over(window))
        .withColumn("prev_tier", F.lag("subscription_tier").over(window))
    )

    changes = changes.filter(
        F.col("prev_country").isNull()
        | (F.col("country") != F.col("prev_country"))
        | (F.col("subscription_tier") != F.col("prev_tier"))
    )

    changes = (
        changes
        .withColumn("valid_from", F.col("effective_at"))
        .withColumn("valid_to", F.lead("effective_at").over(window))
        .select(
            "user_id",
            "country",
            "subscription_tier",
            "valid_from",
            "valid_to",
        )
    )

    validate_scd2_intervals(changes)
    changes.write.mode("overwrite").parquet(target_path)


if __name__ == "__main__":
    spark = create_spark_session("build-user-scd2")
    try:
        build_scd2(spark)
    finally:
        spark.stop()
