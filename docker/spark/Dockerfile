# Spark 3.5.1 (Java 17, Python 3.10) + every connector jar baked in, so nothing
# is downloaded at runtime: Kafka source, Delta Lake, S3A (MinIO), Postgres JDBC.
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root
RUN pip3 install --no-cache-dir psycopg2-binary==2.9.9 requests==2.32.3 numpy==1.26.4 pandas==2.2.2   # numpy + pandas: Spark MLlib

ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar /opt/spark/jars/
ADD ${MAVEN}/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD ${MAVEN}/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar \
 && mkdir -p /tmp/spark-work /tmp/spark-events \
 && chown -R spark:spark /tmp/spark-work /tmp/spark-events

ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    TZ=UTC

USER spark
WORKDIR /opt/spark/work-dir
