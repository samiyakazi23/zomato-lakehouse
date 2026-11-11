"""Generates the historical order history with Spark (runs once automatically on first start).

Scales to billions. Trigger from the UI with "Trigger DAG w/ config", e.g.
    {"orders": 1000000000, "days": 365}
The job works one month at a time and records each finished month, so if it is interrupted
(laptop sleeps, container restarts) a retry or re-trigger resumes where it stopped.
Set "resume": false to throw away existing history and start over.
"""
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor

from zomato_common import DEFAULT_ARGS, SPARK_SUBMIT, scalar


def dimensions_ready():
    try:
        n = scalar("SELECT count(*) FROM dims.riders")
        logging.info("dims.riders has %s rows", n)
        return n > 0
    except Exception as e:                      # log the real reason instead of waiting silently
        logging.warning("reference data not ready yet: %s: %s", type(e).__name__, e)
        return False


with DAG(
    dag_id="zomato_historical_backfill",
    description="Distributed generation of millions of historical orders",
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "orders": Param(int(os.getenv("BACKFILL_ORDERS", "5000000")), type="integer", minimum=1000),
        "days": Param(int(os.getenv("BACKFILL_DAYS", "365")), type="integer", minimum=7),
        "months_per_chunk": Param(1, type="integer", minimum=1, maximum=12),
        "resume": Param(True, type="boolean"),
    },
    tags=["zomato", "backfill", "spark"],
    doc_md=__doc__,
) as dag:
    wait_for_dims = PythonSensor(
        task_id="wait_for_reference_data",
        python_callable=dimensions_ready,
        poke_interval=20,
        timeout=60 * 30,
        mode="reschedule",
    )
    backfill = BashOperator(
        task_id="spark_generate_history",
        bash_command=(f"{SPARK_SUBMIT} historical_backfill.py --orders {{{{ params.orders }}}} "
                      "--days {{ params.days }} --months-per-chunk {{ params.months_per_chunk }} "
                      "{{ '' if params.resume else '--no-resume' }}"),
        pool="spark_pool",
        priority_weight=100, weight_rule="absolute",   # first in line for the shared Spark slot
        retries=5,                       # each retry resumes from the last finished month
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(hours=24),
    )
    refresh = TriggerDagRunOperator(task_id="refresh_gold_and_marts", trigger_dag_id="zomato_lakehouse_batch",
                                    wait_for_completion=True, poke_interval=30)
    train = TriggerDagRunOperator(task_id="train_ml_models", trigger_dag_id="zomato_ml_training")

    wait_for_dims >> backfill >> refresh >> train
