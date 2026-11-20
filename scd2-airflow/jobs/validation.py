import os

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from datetime import datetime
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType

def validate_scd2(spark, snapshot_date):
    snapshot_path = f"s3a://{os.environ['S3_BUCKET']}/user_scd2/snapshot_date={snapshot_date}/"
    users_history = spark.read.parquet(snapshot_path)

    print(f"\nПроверка scd2 за {snapshot_date}")

    # количество строк и пользователей
    rows_count, users_count = users_history.agg(F.count("*"), F.countDistinct("user_id")).first()
    print(f"scd2 снимок {snapshot_date}: {rows_count=}, {users_count=}")

    # пересечение интервалов
    w = Window.partitionBy("user_id").orderBy("valid_from")
    history_with_next = users_history.withColumn(
        "next_valid_from",
        F.lead("valid_from").over(w)
    )
    overlaps = history_with_next.filter(
        F.col("next_valid_from").isNotNull() &
        (F.col("valid_to").isNull() | (F.col("valid_to").cast("timestamp") > F.col("next_valid_from")))
    )
    if overlaps.count() > 0:
        raise ValueError(f"scd2 снимок {snapshot_date}: пересекающиеся интервалы")

    # дубликаты ключа
    duplicates = users_history.groupBy("user_id", "valid_from").count().filter(F.col("count") > 1)
    if duplicates.count() > 0:
        raise ValueError(f"scd2 снимок {snapshot_date}: дубликаты по ключу")

    # is_current=True при valid_to=Null
    invalid_cur = users_history.filter(
        (F.col("is_current") & F.col("valid_to").isNotNull()) |
        (~F.col("is_current") & F.col("valid_to").isNull())
    )
    if invalid_cur.count() > 0:
        raise ValueError(f"scd2 снимок {snapshot_date}: несогласованность is_current и valid_to")

    # одна текущая версия на пользователя
    vers_count = (
        users_history
        .groupBy("user_id")
        .agg(F.sum(F.col("is_current").cast("int")).alias("count"))
        .filter(F.col("count") != 1)
    )
    if vers_count.count() > 0:
        raise ValueError(f"scd2 снимок {snapshot_date}: не ровно одна текущая версия на пользователя")

    # лишние версии при неизменных атрибутах
    history_with_prev = users_history.select(
        "*",
        F.lag("user_fio").over(w).alias("prev_user_fio"),
        F.lag("user_email").over(w).alias("prev_user_email"),
        F.lag("user_login").over(w).alias("prev_user_login"),
        F.lag("valid_from").over(w).alias("prev_valid_from")
    )
    unchanged_versions = history_with_prev.filter(
        F.col("prev_valid_from").isNotNull()
        & F.col("user_fio").eqNullSafe(F.col("prev_user_fio"))
        & F.col("user_email").eqNullSafe(F.col("prev_user_email"))
        & F.col("user_login").eqNullSafe(F.col("prev_user_login"))
    )
    if unchanged_versions.count() > 0:
        raise ValueError(f"scd2 снимок {snapshot_date}: лишние версии при неизменных атрибутах")

    print(f"scd2 за {snapshot_date}: все проверки пройдены")

def validate_campaign_recipients(spark, calculation_date):
    recipients_path = f"s3a://{os.environ['S3_BUCKET']}/campaign_recipients/calculation_date={calculation_date}/"
    recipients = spark.read.parquet(recipients_path)

    print(f"\nПроверка витрины за {calculation_date}")

    # количество строк и получателей по campaign_id,
    campaign_count = recipients.groupBy("campaign_id").count().orderBy("campaign_id")
    rows_count = recipients.count()
    print(f"Витрина за {calculation_date}: {rows_count=}")
    print(f"Количество получателей по campaign_id:")
    campaign_count.show(25)

    # дубли по (campaign_id, user_id)
    duplicates = recipients.groupBy("campaign_id", "user_id").count().filter(F.col("count") > 1)
    if duplicates.count() > 0:
        raise ValueError(f"Витрина за {calculation_date}: дубли по (campaign_id, user_id)")

    # соответствие доменов
    recipients_with_domain = recipients.withColumn(
        "user_email_domain",
        F.lower(F.split(F.col("user_email"), "@").getItem(1))
    )
    invalid_domains = recipients_with_domain.filter(F.col("user_email_domain") != F.lower(F.col("email_domain")))
    if invalid_domains.count() > 0:
        raise ValueError(f"Витрина за {calculation_date}: несоответствие домена email домену кампании")

    # соответсвие исторического интервала
    invalid_intervals = recipients.filter(
        (F.col("user_valid_from") > F.col("send_at")) |
        (F.col("user_valid_to").isNotNull() & (F.col("send_at") >= F.col("user_valid_to")))
    )
    if invalid_intervals.count() > 0:
        raise ValueError(f"Витрина за {calculation_date}: несоответствие исторического инетрвала")

    print(f"Витрина за {calculation_date}: все проверки пройдены")

def validate_on_testdata(spark):
    """Покажите небольшие контрольные примеры
    JOIN, включая смену домена, точное
    попадание в valid_from / valid_to и
    дату до первого события пользователя."""

    print("\nПроверка join на тестовых данных")

    users_schema = StructType([
        StructField("user_id", LongType(), nullable=False),
        StructField("user_fio", StringType(), nullable=False),
        StructField("user_email", StringType(), nullable=False),
        StructField("user_login", StringType(), nullable=False),
        StructField("valid_from", TimestampType(), nullable=False),
        StructField("valid_to", TimestampType(), nullable=True),
    ])

    users_data = [
        # первая версия пользователя 1: домен a.com
        (1, "иванов иван иванович", "user1@a.com", "iii",
         datetime(2026, 1, 1, 10, 0), datetime(2026, 1, 1, 20, 0)),

        # вторая версия пользователя 1: домен изменился на b.com
        (1, "иванов иван иванович", "user1@b.com", "iii",
         datetime(2026, 1, 1, 20, 0), None),

        # единственная версия пользователя 2
        (2, "петров петр петрович", "user2@c.com", "ppp",
         datetime(2026, 1, 1, 15, 0), datetime(2026, 1, 1, 23, 0)),
    ]
    users = spark.createDataFrame(users_data, schema=users_schema)

    campaigns_schema = StructType([
        StructField("campaign_id", LongType(), nullable=False),
        StructField("email_domain", StringType(), nullable=False),
        StructField("send_at", TimestampType(), nullable=False),
        StructField("campaign_name", StringType(), nullable=False),
    ])

    campaigns_data = [
        # попадание в valid_from первой версии юзера 1
        (101, "a.com", datetime(2026, 1, 1, 10, 0), "campaign 101"),

        # попадание в valid_to 1ой версии юзера 1, не должно записаться
        (102, "a.com", datetime(2026, 1, 1, 20, 0), "campaign 102"),

        # попадание в valid_from 2ой версии юзера 1
        (103, "b.com", datetime(2026, 1, 1, 20, 0), "campaign 103"),

        # отправка до первого события юзера 2
        (104, "c.com", datetime(2026, 1, 1, 14, 0), "campaign 104"),

        # попадание в valid_from юзера 2
        (105, "c.com", datetime(2026, 1, 1, 15, 0), "campaign 105"),

        # попадание в valid_to юзера 2, не должно записаться
        (106, "c.com", datetime(2026, 1, 1, 23, 0), "campaign 106"),
    ]

    campaigns = spark.createDataFrame(campaigns_data, schema=campaigns_schema)

    # та же логика join, что и в build_campaign_recipients()
    users_with_domain = users.withColumn(
        "email_domain",
        F.lower(F.split(F.col("user_email"), "@").getItem(1)),
    )

    recipients = (
        campaigns.alias("c")
        .join(
            users_with_domain.alias("u"),
            (F.lower(F.col("c.email_domain")) == F.col("u.email_domain")) &
            (F.col("u.valid_from") <= F.col("c.send_at")) &
            (F.col("u.valid_to").isNull() | (F.col("c.send_at") < F.col("u.valid_to"))),
            "inner",
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

    actual = {
        (row["campaign_id"], row["user_id"])
        for row in recipients.collect()
    }

    expected = {
        (101, 1),
        (103, 1),
        (105, 2),
    }

    if actual != expected:
        raise ValueError(f"Ошибка join на тестовых данных.Ожидалось: {expected}, получено: {actual}")

    if recipients.count() != len(actual):
        raise ValueError("В результатах контрольного join обнаружены дубликаты")

    print("Проверка join на тестовых данных успешно заверщена")


def validate_meta(spark, snapshot_date, calculation_date):
    meta_cols = [
        "_dag_id",
        "_run_id",
        "_task_id",
        "_data_interval_start",
        "_data_interval_end",
        "_loaded_at"
    ]

    snapshot_path = f"s3a://{os.environ['S3_BUCKET']}/user_scd2/snapshot_date={snapshot_date}/"
    users_history = spark.read.parquet(snapshot_path)
    recipients_path = f"s3a://{os.environ['S3_BUCKET']}/campaign_recipients/calculation_date={calculation_date}/"
    recipients = spark.read.parquet(recipients_path)

    tables = {
        "SCD2": users_history,
        "Campaign recipients": recipients
    }
    print(f"\nПроверка метаданных: snapshot_date={snapshot_date}, calculation_date={calculation_date}")

    # null в метаполях
    for table_name, df in tables.items():
        missing_columns = set(meta_cols) - set(df.columns)
        if missing_columns:
            raise ValueError(f"{table_name}: отсутствуют метаполя {missing_columns}")

        null_condition = None
        for column in meta_cols:
            condition = F.col(column).isNull()

            if null_condition is None:
                null_condition = condition
            else:
                null_condition = null_condition | condition

        if df.filter(null_condition).count() > 0:
            raise ValueError(f"{table_name}: NULL в метаполях")

        # сводка
        summary = (
            df
            .groupBy(
                "_dag_id",
                "_run_id",
                "_task_id",
                "_data_interval_start",
                "_data_interval_end"
            )
            .agg(
                F.count("*").alias("rows_count"),
                F.min("_loaded_at").alias("min_loaded_at"),
                F.max("_loaded_at").alias("max_loaded_at")
            )
            .orderBy(
                "_data_interval_start",
                "_dag_id",
                "_run_id",
                "_task_id"
            )
        )

        print(f"\nСводка метаданных: {table_name}")
        summary.show(truncate=False)
        inconsistent_loaded_at = summary.filter(F.col("min_loaded_at") != F.col("max_loaded_at"))

        if inconsistent_loaded_at.count() > 0:
            raise ValueError(f"{table_name}: _loaded_at различается внутри одной попытки")

    # согласованность интервалов и _run_id между задачами каждого запуска
    run_columns = [
        "_dag_id",
        "_run_id",
        "_data_interval_start",
        "_data_interval_end"
    ]

    scd2_runs = users_history.select(*run_columns).distinct()
    recipient_runs = recipients.select(*run_columns).distinct()

    scd2_only = scd2_runs.exceptAll(recipient_runs)
    recipients_only = recipient_runs.exceptAll(scd2_runs)

    if scd2_only.count() > 0 or recipients_only.count() > 0:
        raise ValueError("Несогласованность метаданных между SCD2 и витриной: различаются _dag_id, _run_id или границы интервала")

    print("Метаданные: все проверки пройдены")

def compare_parquet_data(
    spark,
    before_path,
    after_path,
    table_name,
    exclude_columns=None,
):
    exclude_columns = exclude_columns or []

    print(f"\nСравнение данных: {table_name}")
    print(f"До: {before_path}")
    print(f"После: {after_path}")

    before = spark.read.parquet(before_path)
    after = spark.read.parquet(after_path)

    if set(before.columns) != set(after.columns):
        raise ValueError(f"{table_name}: наборы столбцов до и после различаются")

    missing_meta_before = set(exclude_columns) - set(before.columns)
    missing_meta_after = set(exclude_columns) - set(after.columns)
    if missing_meta_before or missing_meta_after:
        raise ValueError(f"{table_name}: отсутствуют метаполя. До: {missing_meta_before}, после: {missing_meta_after}")

    columns = [column for column in before.columns if column not in exclude_columns]
    if not columns:
        raise ValueError(f"{table_name}: нет столбцов для сравнения")

    before_types = {
        field.name: field.dataType
        for field in before.schema.fields
    }
    after_types = {
        field.name: field.dataType
        for field in after.schema.fields
    }

    for column in columns:
        if before_types[column] != after_types[column]:
            raise ValueError(f"{table_name}: тип столбца {column} различается: {before_types[column]} и {after_types[column]}")

    before_data = before.select(*columns)
    after_data = after.select(*columns)

    before_only = before_data.exceptAll(after_data)
    after_only = after_data.exceptAll(before_data)

    before_only_count = before_only.count()
    after_only_count = after_only.count()

    print(f"Строк только до: {before_only_count}")
    print(f"Строк только после: {after_only_count}")

    if before_only_count > 0 or after_only_count > 0:
        raise ValueError(f"{table_name}: данные различаются")

    print(f"Проверка {table_name}: данные совпадают")

# сохрание бизнес-данных после перезапуска за 23.01
def validate_rerun_consistency(spark, results_dir="results"):
    meta_cols = [
        "_dag_id",
        "_run_id",
        "_task_id",
        "_data_interval_start",
        "_data_interval_end",
        "_loaded_at",
    ]
    tables = ["user_scd2", "campaign_recipients"]
    partition_date = "2026-01-23"

    for table_name in tables:
        before_path = os.path.join(results_dir, table_name, partition_date, "before_rerun")
        after_path = os.path.join(results_dir, table_name, partition_date, "after_rerun")
        compare_parquet_data(
            spark=spark,
            before_path=before_path,
            after_path=after_path,
            table_name=f"{table_name}, {partition_date}",
            exclude_columns=meta_cols,
        )

    print("Проверка повторного расчёта завершена успешно")

# сохрание всех полей в тч мета для 24 и 25.01
def validate_unchanged_partitions(spark, results_dir="results"):
    tables = {
        "user_scd2": "snapshot_date",
        "campaign_recipients": "calculation_date",
    }
    dates = ["2026-01-24", "2026-01-25"]

    for table_name, partition_column in tables.items():
        for partition_date in dates:
            original_path = os.path.join(
                results_dir,
                table_name,
                partition_date
            )
            current_path = f"s3a://{os.environ['S3_BUCKET']}/{table_name}/{partition_column}={partition_date}/"
            compare_parquet_data(
                spark=spark,
                before_path=original_path,
                after_path=current_path,
                table_name=f"{table_name}, {partition_date}",
            )

    print("\nПроверка неизменности остальных партиций завершена успешно")
