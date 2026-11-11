"""Every 30 min: silver -> gold (Spark) -> analytics marts (dbt build = run + test) -> quality gate."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from zomato_common import DBT_ENV, DEFAULT_ARGS, SPARK_SUBMIT, dbt, run_quality_checks

with DAG(
    dag_id="zomato_lakehouse_batch",
    description="Spark gold aggregates + dbt marts + data quality",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["zomato", "batch", "spark", "dbt"],
    doc_md=__doc__,
) as dag:
    spark_gold = BashOperator(
        task_id="spark_build_gold",
        bash_command=f"{SPARK_SUBMIT} gold_aggregates.py",
        pool="spark_pool",
        priority_weight=10, weight_rule="absolute",
        execution_timeout=timedelta(minutes=60),
    )
    dbt_build = BashOperator(
        task_id="dbt_build_marts",
        bash_command=dbt("build"),
        env=DBT_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=20),
    )
    quality = PythonOperator(task_id="data_quality_gate", python_callable=run_quality_checks)
    docs = BashOperator(task_id="dbt_docs_generate", bash_command=dbt("docs generate"), env=DBT_ENV,
                        append_env=True, retries=0)

    spark_gold >> dbt_build >> quality >> docs
