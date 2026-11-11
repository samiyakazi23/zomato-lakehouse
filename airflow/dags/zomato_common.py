"""Shared settings and checks for the Zomato DAGs."""
import logging
import os
from datetime import timedelta

log = logging.getLogger(__name__)

SPARK_SUBMIT = "bash /opt/jobs/submit.sh"
DBT_DIR = "/opt/dbt/zomato"
DBT_ENV = {"DBT_TARGET_PATH": "/tmp/dbt/target", "DBT_LOG_PATH": "/tmp/dbt/logs", "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}


def dbt(cmd):
    return f"cd {DBT_DIR} && /opt/dbt-venv/bin/dbt --no-use-colors {cmd} --profiles-dir {DBT_DIR}"


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def pg_conn():
    import psycopg2
    return psycopg2.connect(host=os.getenv("PG_HOST", "postgres"), port=os.getenv("PG_PORT", "5432"),
                            dbname=os.getenv("PG_DB", "zomato"), user=os.getenv("PG_USER", "zomato"),
                            password=os.getenv("PG_PASSWORD", "zomato"))


def scalar(sql):
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]
    finally:
        conn.close()


QUALITY_CHECKS = [
    ("gold.daily_city_metrics is populated", "SELECT count(*) > 0 FROM gold.daily_city_metrics"),
    ("no negative GMV", "SELECT count(*) = 0 FROM gold.daily_city_metrics WHERE gmv < 0"),
    ("cancel rate within [0, 1]",
     "SELECT count(*) = 0 FROM gold.daily_city_metrics WHERE cancel_rate < 0 OR cancel_rate > 1"),
    ("one row per city per day",
     "SELECT count(*) = 0 FROM (SELECT order_date, city FROM gold.daily_city_metrics "
     "GROUP BY 1, 2 HAVING count(*) > 1) d"),
    ("restaurant rankings built", "SELECT count(*) > 0 FROM analytics.mart_restaurant_rankings"),
    ("today's live orders reached gold",
     "SELECT count(*) > 0 FROM gold.daily_city_metrics WHERE order_date = (now() AT TIME ZONE 'Asia/Kolkata')::date"),
]
SOFT_CHECKS = {"today's live orders reached gold"}  # warn only (streaming may have just started)


def run_quality_checks():
    failed = []
    for name, sql in QUALITY_CHECKS:
        ok = bool(scalar(sql))
        log.info("%-40s %s", name, "PASS" if ok else "FAIL")
        if not ok and name not in SOFT_CHECKS:
            failed.append(name)
    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")
