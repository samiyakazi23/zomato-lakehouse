"""Daily (and after the first backfill): train the Spark MLlib models and check their quality.

1. late-delivery classifier: Logistic Regression vs Random Forest vs Gradient-Boosted Trees on delivered
   orders; the best model by ROC-AUC is saved to the lake and used by the streaming job to score live orders
2. customer clustering: K-means on customer behaviour, k chosen by silhouette score
3. model quality gate: fails the run (and keeps the previous serving tables visible in Airflow) if the
   selected classifier is no better than chance or clustering produced no groups
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from zomato_common import DEFAULT_ARGS, SPARK_SUBMIT, scalar

log = logging.getLogger(__name__)


def model_quality_gate():
    auc = scalar("SELECT max(auc) FROM ml.late_model_metrics WHERE selected")
    clusters = scalar("SELECT count(*) FROM ml.customer_clusters")
    log.info("selected late-delivery model ROC-AUC %.4f, %d customer clusters", auc or 0, clusters or 0)
    if auc is None or auc < 0.6:
        raise ValueError(f"late-delivery model ROC-AUC {auc} is below 0.6")
    if not clusters:
        raise ValueError("customer clustering produced no clusters")


with DAG(
    dag_id="zomato_ml_training",
    description="Spark MLlib: late-delivery classifier + K-means customer clusters",
    schedule="30 21 * * *",            # 03:00 IST, after the nightly maintenance window
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["zomato", "ml", "spark"],
    doc_md=__doc__,
) as dag:
    late = BashOperator(
        task_id="train_late_delivery_model",
        bash_command=f"{SPARK_SUBMIT} ml_models.py --only late --sample-rows 1000000",
        pool="spark_pool", priority_weight=5, weight_rule="absolute",
        execution_timeout=timedelta(hours=2),
    )
    clusters = BashOperator(
        task_id="train_customer_clusters",
        bash_command=f"{SPARK_SUBMIT} ml_models.py --only clusters --k-min 3 --k-max 8",
        pool="spark_pool", priority_weight=5, weight_rule="absolute",
        execution_timeout=timedelta(hours=1),
    )
    gate = PythonOperator(task_id="model_quality_gate", python_callable=model_quality_gate)

    late >> clusters >> gate
