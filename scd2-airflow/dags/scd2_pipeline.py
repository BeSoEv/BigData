from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from scd2 import run_user_scd2
from campaign_recipients import run_campaign_recipients


def build_user_scd2_task(**context):
    run_user_scd2(
        data_interval_start=context["data_interval_start"],
        data_interval_end=context["data_interval_end"],
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
    )


def build_campaign_recipients_task(**context):
    run_campaign_recipients(
        data_interval_start=context["data_interval_start"],
        data_interval_end=context["data_interval_end"],
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        task_id=context["task"].task_id,
    )


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="scd2_campaign_recipients",
    description="Build user SCD2 and campaign recipients",
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        "0 0 * * *",
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=pendulum.datetime(2026, 1, 23, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 25, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["scd2", "campaigns"],
) as dag:

    build_user_scd2 = PythonOperator(
        task_id="build_user_scd2",
        python_callable=build_user_scd2_task,
        do_xcom_push=False,
    )

    build_campaign_recipients = PythonOperator(
        task_id="build_campaign_recipients",
        python_callable=build_campaign_recipients_task,
        do_xcom_push=False,
    )

    build_user_scd2 >> build_campaign_recipients
