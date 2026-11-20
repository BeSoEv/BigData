import os
from datetime import datetime, timezone, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from spark_session import create_spark_session


def read_campaigns(spark, campaigns_path):
    campaigns_schema = StructType([
        StructField("campaign_id", LongType(), nullable=False),
        StructField("email_domain", StringType(), nullable=False),
        StructField("send_at", TimestampType(), nullable=False),
        StructField("campaign_name", StringType(), nullable=False),
    ])
    campaigns = (
        spark.read
        .schema(campaigns_schema)
        .option("header", "true")
        .csv(campaigns_path)
    )
    return campaigns


def read_scd2_snapshot(spark, snapshot_path):
    return spark.read.parquet(snapshot_path)


def build_campaign_recipients(spark, data_interval_start, data_interval_end):
    campaigns_path = f"s3a://{os.environ['S3_BUCKET']}/raw/demo/campaigns.csv"
    campaigns = read_campaigns(spark, campaigns_path)
    campaigns = campaigns.filter(
        (F.col("send_at") >= F.lit(data_interval_start)) &
        (F.col("send_at") < F.lit(data_interval_end))
    )
    snapshot_date = data_interval_start.date().isoformat()
    snapshot_path = f"s3a://{os.environ['S3_BUCKET']}/user_scd2/snapshot_date={snapshot_date}/"
    users_history = read_scd2_snapshot(spark, snapshot_path)
    history_with_domain = users_history.withColumn(
        "email_domain",
        F.lower(F.split(F.col("user_email"), "@").getItem(1))
    )

    recipients = (
        campaigns.alias("c")
        .join(
            history_with_domain.alias("u"),
            (F.lower(F.col("c.email_domain")) == F.col("u.email_domain")) &
            (F.col("u.valid_from") <= F.col("c.send_at")) &
            (F.col("u.valid_to").isNull() | (F.col("c.send_at") < F.col("u.valid_to"))),
            "inner"
        )
        .select(
            F.col("c.campaign_id").cast("bigint"),
            F.col("c.campaign_name").cast("string"),
            F.col("c.send_at").cast("timestamp"),
            F.col("c.email_domain").cast("string"),
            F.col("u.user_id").cast("bigint"),
            F.col("u.user_fio").cast("string"),
            F.col("u.user_email").cast("string"),
            F.col("u.user_login").cast("string"),
            F.col("u.valid_from").cast("timestamp").alias("user_valid_from"),
            F.col("u.valid_to").cast("timestamp").alias("user_valid_to"),
        )
    )
    return recipients

def add_recipients_meta(
        recipients,
        calculation_date,
        dag_id,
        run_id,
        task_id,
        data_interval_start,
        data_interval_end,
        loaded_at
):
    recipients_meta = (
        recipients
        .withColumn("calculation_date", F.lit(calculation_date).cast("date"))
        .withColumn("_dag_id", F.lit(dag_id).cast("string"))
        .withColumn("_run_id", F.lit(run_id).cast("string"))
        .withColumn("_task_id", F.lit(task_id).cast("string"))
        .withColumn("_data_interval_start", F.lit(data_interval_start).cast("timestamp"))
        .withColumn("_data_interval_end", F.lit(data_interval_end).cast("timestamp"))
        .withColumn("_loaded_at", F.lit(loaded_at).cast("timestamp"))

    )
    return recipients_meta

def validate_recipients(recipients):
    duplicates = (
        recipients
        .groupBy("campaign_id", "user_id")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.count() > 0:
        raise ValueError(
            "В campaign_recipients есть дубликаты по (campaign_id, user_id)"
        )

    required_columns = [
        "campaign_id",
        "campaign_name",
        "send_at",
        "email_domain",
        "user_id",
        "user_fio",
        "user_email",
        "user_login",
        "user_valid_from",
        "calculation_date",
        "_dag_id",
        "_run_id",
        "_task_id",
        "_data_interval_start",
        "_data_interval_end",
        "_loaded_at",
    ]

    nulls = F.lit(False)
    for column in required_columns:
        nulls = nulls | F.col(column).isNull()

    if recipients.filter(nulls).count() > 0:
        raise ValueError("В campaign_recipients есть NULL в обязательных полях")

def write_recipients(recipients, output_path):
    recipients.write.mode("overwrite").parquet(output_path)

def run_campaign_recipients(
        data_interval_start,
        data_interval_end,
        dag_id,
        run_id,
        task_id
):
    if data_interval_end - data_interval_start != timedelta(days=1):
        raise ValueError("Расчётный интервал должен составлять одни сутки")

    spark = create_spark_session("campaign-recipients")

    try:
        recipients = build_campaign_recipients(
            spark,
            data_interval_start,
            data_interval_end
        )
        calculation_date = data_interval_start.date().isoformat()
        loaded_at = datetime.now(timezone.utc)

        recipients_meta = add_recipients_meta(
            recipients,
            calculation_date,
            dag_id,
            run_id,
            task_id,
            data_interval_start,
            data_interval_end,
            loaded_at
        )
        validate_recipients(recipients_meta)
        output_path = f"s3a://{os.environ['S3_BUCKET']}/campaign_recipients/calculation_date={calculation_date}/"
        write_recipients(recipients_meta, output_path)

    finally:
        spark.stop()


