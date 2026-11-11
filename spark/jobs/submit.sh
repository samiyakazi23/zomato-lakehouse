#!/usr/bin/env bash
# Submits a PySpark job to the standalone cluster in client mode.
# Used by the streaming container and by Airflow tasks:  bash /opt/jobs/submit.sh <job.py> [args]
set -euo pipefail
JOB="$1"; shift
DRIVER_IP="$(hostname -i | awk '{print $1}')"
SUBMIT="spark-submit"
[ -n "${SPARK_HOME:-}" ] && [ -x "${SPARK_HOME}/bin/spark-submit" ] && SUBMIT="${SPARK_HOME}/bin/spark-submit"

echo "Submitting ${JOB} to ${SPARK_MASTER:-spark://spark-master:7077} (driver ${DRIVER_IP})"
exec "${SUBMIT}" \
  --master "${SPARK_MASTER:-spark://spark-master:7077}" \
  --deploy-mode client \
  --name "zomato-${JOB%.py}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --driver-class-path "/opt/extra-jars/*" \
  --conf spark.driver.host="${DRIVER_IP}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.cores.max="${SPARK_CORES_MAX:-2}" \
  --conf spark.executor.cores="${SPARK_EXECUTOR_CORES:-${SPARK_CORES_MAX:-2}}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-1g}" \
  --conf spark.pyspark.python=python3 \
  --conf spark.ui.showConsoleProgress=false \
  "/opt/jobs/${JOB}" "$@"
