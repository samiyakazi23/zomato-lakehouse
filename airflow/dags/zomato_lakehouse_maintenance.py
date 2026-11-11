"""Nightly Delta Lake housekeeping (OPTIMIZE + Z-ORDER + VACUUM) and Postgres retention for 1-minute tables."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from zomato_common import DEFAULT_ARGS, SPARK_SUBMIT, pg_conn


def prune_realtime(days=3):
    conn = pg_conn()
    with conn, conn.cursor() as cur:
        for t in ("city_orders_1m", "cuisine_orders_1m", "order_status_1m", "rider_activity_1m"):
            cur.execute(f"DELETE FROM realtime.{t} WHERE window_start < now() - interval '{days} days'")
        cur.execute(f"DELETE FROM realtime.stream_progress WHERE ts < now() - interval '{days} days'")
        cur.execute(f"DELETE FROM realtime.order_risk WHERE event_ts < now() - interval '{days} days'")
    conn.close()


with DAG(
    dag_id="zomato_lakehouse_maintenance",
    schedule="30 21 * * *",   # 03:00 IST
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["zomato", "maintenance", "delta"],
    doc_md=__doc__,
) as dag:
    optimize = BashOperator(task_id="delta_optimize_vacuum", bash_command=f"{SPARK_SUBMIT} delta_maintenance.py",
                            pool="spark_pool", priority_weight=1, weight_rule="absolute",
                            execution_timeout=timedelta(hours=1))
    prune = PythonOperator(task_id="prune_realtime_tables", python_callable=prune_realtime)
    optimize >> prune
