# Airflow 2.9.3 that can act as a Spark driver (Java + PySpark + the same jars as
# the cluster) and run dbt from an isolated virtualenv.
FROM apache/airflow:2.9.3-python3.10

USER root
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java TZ=UTC

ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar /opt/extra-jars/
ADD ${MAVEN}/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar /opt/extra-jars/
ADD ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/extra-jars/
ADD ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/extra-jars/
ADD ${MAVEN}/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/extra-jars/
RUN chmod 755 /opt/extra-jars && chmod 644 /opt/extra-jars/*.jar

# dbt lives in its own venv so its dependencies never clash with Airflow's
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-postgres==1.8.2

USER airflow
RUN pip install --no-cache-dir psycopg2-binary==2.9.9 pyspark==3.5.1 numpy==1.26.4 pandas==2.1.4   # numpy + pandas: Spark MLlib (ml_models.py)
