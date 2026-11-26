
from datetime import timedelta, datetime, timezone

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from spark_session import create_spark_session
from scd2 import build_scd2
from batch_processing import (
    run_batch_processing,
    publish_batch_results,
    get_clickhouse_client,
)

def advance_report_watermark(interval_start, interval_end):
    start = datetime.fromisoformat(
        interval_start.replace("Z", "+00:00")
    )
    end = datetime.fromisoformat(
        interval_end.replace("Z", "+00:00")
    )

    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("Интервалы должны содержать часовой пояс")

    if start.utcoffset() != timezone.utc.utcoffset(start):
        raise ValueError("interval_start должен быть в UTC")

    if end.utcoffset() != timezone.utc.utcoffset(end):
        raise ValueError("interval_end должен быть в UTC")

    if start >= end:
        raise ValueError("Некорректный расчётный интервал")

    if start.second or start.microsecond or end.second or end.microsecond:
        raise ValueError("Границы интервала должны совпадать с целой минутой")

    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)

    client = get_clickhouse_client()

    result = client.query("""
        SELECT argMax(watermark, updated_at)
        FROM cinema.report_watermark
    """)

    current_t = result.result_rows[0][0]

    if current_t.tzinfo is None:
        current_t = current_t.replace(tzinfo=timezone.utc)
    else:
        current_t = current_t.astimezone(timezone.utc)

    # исторический пересчет не должен двигать T назад
    if end <= current_t:
        print("Исторический пересчёт: T не изменяется")
        return

    # запрещаем пропуск или пересечение интервалов
    if start != current_t:
        raise ValueError(
            f"Нельзя продвинуть T: текущая граница {current_t}, "
            f"а интервал начинается с {start}"
        )

    client.insert(
        "report_watermark",
        [(datetime.now(timezone.utc), end)],
        column_names=["updated_at", "watermark"],
    )

    print(f"Граница T успешно обновлена: {end}")

def build_scd2_task():
    spark = create_spark_session("airflow-build-scd2")
    try:
        build_scd2(spark)
    finally:
        spark.stop()


def calculate_and_publish_task(**context):
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    interval_start = conf.get(
        "interval_start",
        context["data_interval_start"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    interval_end = conf.get(
        "interval_end",
        context["data_interval_end"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    if bool(conf.get("interval_start")) != bool(conf.get("interval_end")):
        raise ValueError(
            "Для пересчёта нужно передать оба параметра: "
            "interval_start и interval_end"
        )

    spark = create_spark_session("airflow-batch-processing")

    try:
        results = run_batch_processing(
            spark,
            interval_start=interval_start,
            interval_end=interval_end,
        )

        publish_batch_results(
            results,
            interval_start,
            interval_end,
        )

        advance_report_watermark(interval_start, interval_end)
    finally:
        spark.stop()


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="cinema_batch_pipeline",
    description="Build SCD2 and publish batch analytics marts",
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["cinema", "spark", "batch"],
) as dag:

    build_scd2_operator = PythonOperator(
        task_id="build_scd2",
        python_callable=build_scd2_task,
    )

    calculate_and_publish = PythonOperator(
        task_id="calculate_and_publish",
        python_callable=calculate_and_publish_task,
    )

    build_scd2_operator >> calculate_and_publish
