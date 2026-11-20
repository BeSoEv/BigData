import os
from datetime import datetime
from contextlib import redirect_stdout, redirect_stderr

from spark_session import create_spark_session
from validation import (
    validate_scd2,
    validate_campaign_recipients,
    validate_on_testdata,
    validate_meta,
    validate_rerun_consistency,
    validate_unchanged_partitions,
)

def run_validations():
    spark = create_spark_session("validate_results")

    try:
        dates = [
            "2026-01-23",
            "2026-01-24",
            "2026-01-25",
        ]

        # проверка scd2 и витрины по каждой дате
        for date in dates:
            print(f"\n{'=' * 60}")
            print(f"Проверка данных за {date}")
            print(f"{'=' * 60}")

            validate_scd2(spark, date)
            validate_campaign_recipients(spark, date)
            validate_meta(spark, date, date)

        # контрольные примеры исторического JOIN
        validate_on_testdata(spark)

        # проверка повторного расчёта за 23.01
        validate_rerun_consistency(spark, results_dir="/opt/airflow/results")

        # проверка неизменности партиций за 24 и 25.01
        validate_unchanged_partitions(spark, results_dir="/opt/airflow/results")
        print("\nВсе проверки успешно завершены")
    finally:
        spark.stop()

def main():
    report_dir = "/opt/airflow/results/validation_reports"
    os.makedirs(report_dir, exist_ok=True)

    report_name = (
        f"validation_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
    )
    report_path = os.path.join(report_dir, report_name)

    with open(report_path, "w", encoding="utf-8") as report:
        with redirect_stdout(report), redirect_stderr(report):
            print(f"Дата формирования отчёта: {datetime.now()}")
            print("Проверка результатов HW 2.3\n")

            try:
                run_validations()
            except Exception:
                import traceback
                traceback.print_exc()
                raise

    print(f"Отчёт сохранён: {report_path}")


if __name__ == "__main__":
    main()
