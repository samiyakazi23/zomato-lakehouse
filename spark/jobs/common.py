"""Shared Spark / storage / Postgres helpers for every job."""
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")

# Lake root. In Docker this is s3a://lakehouse on the S3 store (SeaweedFS). Override LAKE_PATH (and LAKE_FORMAT=parquet)
# to run the jobs locally against a plain folder for testing.
LAKE = os.getenv("LAKE_PATH", f"s3a://{os.getenv('LAKEHOUSE_BUCKET', 'lakehouse')}")
FMT = os.getenv("LAKE_FORMAT", "delta")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

PG = {
    "host": os.getenv("PG_HOST", "postgres"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DB", "zomato"),
    "user": os.getenv("PG_USER", "zomato"),
    "password": os.getenv("PG_PASSWORD", "zomato"),
}
JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"
JDBC_PROPS = {"user": PG["user"], "password": PG["password"], "driver": "org.postgresql.Driver"}


def lake(*parts):
    return "/".join([LAKE.rstrip("/"), *parts])


def get_spark(app_name, streaming=False):
    b = (SparkSession.builder.appName(app_name)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8" if streaming else "16")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.parquet.compression.codec", "zstd"))   # ~30% smaller than snappy
    if FMT == "delta":
        b = (b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.databricks.delta.autoCompact.enabled", "true")
             .config("spark.databricks.delta.retentionDurationCheck.enabled", "false"))
    if LAKE.startswith("s3a://"):
        b = (b.config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://s3:9000"))
             .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
             .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .config("spark.hadoop.fs.s3a.fast.upload", "true"))
    if streaming:
        b = (b.config("spark.sql.streaming.stateStore.providerClass",
                      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
             .config("spark.scheduler.mode", "FAIR")
             .config("spark.sql.streaming.metricsEnabled", "true"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_lake(spark, *parts) -> DataFrame | None:
    """Read a lake table, or None if it has not been created yet."""
    try:
        return spark.read.format(FMT).load(lake(*parts))
    except AnalysisException:
        return None


def read_pg(spark, table) -> DataFrame:
    return spark.read.jdbc(JDBC_URL, table, properties=JDBC_PROPS)


def write_pg(df, table):
    """Replace a Postgres serving table. TRUNCATE keeps dependent dbt views intact; if the table's
    columns changed (after an upgrade) it is dropped and recreated, and dbt rebuilds its views next."""
    schema, name = table.split(".")
    try:
        conn = pg_connect()
        with conn, conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
                        (schema, name))
            existing = {r[0] for r in cur.fetchall()}
            if existing and existing != {c.lower() for c in df.columns}:
                logging.getLogger("common").info("schema of %s changed - recreating it", table)
                cur.execute(f"DROP TABLE {table} CASCADE")
        conn.close()
    except Exception as exc:   # never block the write on this check
        logging.getLogger("common").warning("schema check for %s skipped: %s", table, exc)
    (df.write.format("jdbc")
       .option("url", JDBC_URL).option("dbtable", table)
       .option("user", PG["user"]).option("password", PG["password"])
       .option("driver", "org.postgresql.Driver")
       .option("truncate", "true").option("batchsize", "5000")
       .mode("overwrite").save())


def pg_connect():
    import psycopg2
    return psycopg2.connect(**PG)


def pg_upsert(table, rows, keys):
    """Idempotent upsert of a small list of dicts (used for streaming window aggregates)."""
    if not rows:
        return
    from psycopg2.extras import execute_values
    cols = list(rows[0].keys())
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in keys)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}, updated_at = now()")
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, sql, [tuple(r[c] for c in cols) for r in rows], page_size=1000)
    finally:
        conn.close()
