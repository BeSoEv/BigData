import os
from datetime import datetime, timezone, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from spark_session import create_spark_session


def read_events(spark, events_path):
    events_schema = StructType([
        StructField("event_time", TimestampType(), nullable=False),
        StructField("user_id", LongType(), nullable=False),
        StructField("user_fio", StringType(), nullable=False),
        StructField("user_email", StringType(), nullable=False),
        StructField("user_login", StringType(), nullable=False),
    ])

    events = (
        spark.read
        .schema(events_schema)
        .option("header", "true")
        .csv(events_path)
    )
    return events

def validate_events(events):
    required_columns = [
        "event_time",
        "user_id",
        "user_fio",
        "user_email",
        "user_login",
    ]

    nulls = [F.col(column).isNull() for column in required_columns]
    invalid_rows = events.filter(
        nulls[0] |
        nulls[1] |
        nulls[2] |
        nulls[3] |
        nulls[4]
    )

    if invalid_rows.count() > 0:
        raise ValueError("В исходных событиях обнаружены NULL в обязательных полях")

def build_scd2(events, data_interval_end):
    events_end = events.filter(F.col("event_time") < F.lit(data_interval_end))
    w = Window.partitionBy("user_id").orderBy("event_time")
    events_prev = events_end.select(
        "*",
        F.lag("event_time").over(w).alias("prev_event_time"),
        F.lag("user_fio").over(w).alias("prev_user_fio"),
        F.lag("user_email").over(w).alias("prev_user_email"),
        F.lag("user_login").over(w).alias("prev_user_login"),
    )
    users_history = events_prev.filter(
        F.col("prev_event_time").isNull() |
        (~F.col("user_fio").eqNullSafe(F.col("prev_user_fio"))) |
        (~F.col("user_email").eqNullSafe(F.col("prev_user_email"))) |
        (~F.col("user_login").eqNullSafe(F.col("prev_user_login")))
    ).drop("prev_event_time", "prev_user_fio", "prev_user_email", "prev_user_login")
    users_history = (
        users_history
        .withColumn("valid_from", F.col("event_time"))
        .withColumn("valid_to", F.lead("event_time").over(w))
        .withColumn("is_current", F.col("valid_to").isNull())
        .select(
            F.col("user_id").cast("bigint"),
            F.col("user_fio").cast("string"),
            F.col("user_email").cast("string"),
            F.col("user_login").cast("string"),
            F.col("valid_from").cast("timestamp"),
            F.col("valid_to").cast("timestamp"),
            F.col("is_current").cast("boolean")
        )
    )
    return users_history

def add_scd2_meta(
        users_history,
        snapshot_date,
        dag_id,
        run_id,
        task_id,
        data_interval_start,
        data_interval_end,
        loaded_at
):
    users_history_meta = (
        users_history
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
        .withColumn("_dag_id", F.lit(dag_id).cast("string"))
        .withColumn("_run_id", F.lit(run_id).cast("string"))
        .withColumn("_task_id", F.lit(task_id).cast("string"))
        .withColumn("_data_interval_start", F.lit(data_interval_start).cast("timestamp"))
        .withColumn("_data_interval_end", F.lit(data_interval_end).cast("timestamp"))
        .withColumn("_loaded_at", F.lit(loaded_at).cast("timestamp"))

    )
    return users_history_meta

def write_scd2(users_history, output_path):
    users_history.write.mode("overwrite").parquet(output_path)

def run_user_scd2(
        data_interval_start,
        data_interval_end,
        dag_id,
        run_id,
        task_id
):
    if data_interval_end - data_interval_start != timedelta(days=1):
        raise ValueError("Расчётный интервал должен составлять одни сутки")

    spark = create_spark_session("scd2")

    try:
        events_path = f"s3a://{os.environ['S3_BUCKET']}/raw/demo/events/"
        events = read_events(spark, events_path)
        validate_events(events)
        users_history = build_scd2(events, data_interval_end)
        snapshot_date = data_interval_start.date().isoformat()
        loaded_at = datetime.now(timezone.utc)
        users_history_meta = add_scd2_meta(
            users_history,
            snapshot_date,
            dag_id,
            run_id,
            task_id,
            data_interval_start,
            data_interval_end,
            loaded_at
        )
        output_path = f"s3a://{os.environ['S3_BUCKET']}/user_scd2/snapshot_date={snapshot_date}/"
        write_scd2(users_history_meta, output_path)

    finally:
        spark.stop()
