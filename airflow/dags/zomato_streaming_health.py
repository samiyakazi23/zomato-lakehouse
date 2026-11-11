"""Every 5 min: verifies the always-on streaming pipeline is alive and fresh.

Checks that the Spark streaming application is registered with the cluster, that every streaming
query committed a micro-batch recently, and that real-time windows are landing in Postgres.
A failed run here is the "page the on-call engineer" signal.
"""
import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from zomato_common import DEFAULT_ARGS, scalar

log = logging.getLogger(__name__)
EXPECTED_QUERIES = 11   # 10 data queries + rt_late_risk (ML scoring)


def check_spark_app():
    if os.getenv("SPARK_MASTER", "").startswith("local"):
        # lite mode: no cluster, ask the streaming driver's own REST API
        with urllib.request.urlopen("http://spark-streaming:4040/api/v1/applications", timeout=10) as r:
            apps = [a["name"] for a in json.load(r)]
        log.info("Streaming driver reports apps: %s", apps)
        if not apps:
            raise RuntimeError("Streaming application is not running")
        return
    with urllib.request.urlopen("http://spark-master:8080/json/", timeout=10) as r:
        state = json.load(r)
    apps = [a["name"] for a in state.get("activeapps", [])]
    log.info("Active Spark apps: %s | alive workers: %s", apps, state.get("aliveworkers"))
    if not any("streaming" in a for a in apps):
        raise RuntimeError("Streaming application is not running on the Spark cluster")


def check_query_freshness():
    stale = scalar(f"""
        SELECT {EXPECTED_QUERIES} - count(DISTINCT query_name)
        FROM realtime.stream_progress WHERE ts > now() - interval '3 minutes'""")
    if stale > 0:
        raise RuntimeError(f"{stale} streaming queries have not committed a batch in 3 minutes")


def check_realtime_windows():
    lag = scalar("SELECT EXTRACT(EPOCH FROM now() - max(window_start))::int FROM realtime.city_orders_1m")
    log.info("Latest real-time window is %s seconds old", lag)
    if lag is None or lag > 300:
        raise RuntimeError(f"Real-time aggregates are stale ({lag}s)")


with DAG(
    dag_id="zomato_streaming_health",
    description="Liveness + freshness checks for the streaming pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["zomato", "streaming", "monitoring"],
    doc_md=__doc__,
) as dag:
    app = PythonOperator(task_id="spark_streaming_app_running", python_callable=check_spark_app)
    fresh = PythonOperator(task_id="all_queries_committing", python_callable=check_query_freshness)
    windows = PythonOperator(task_id="realtime_windows_fresh", python_callable=check_realtime_windows)
    app >> [fresh, windows]
