"""Spark Structured Streaming: Kafka -> Delta Lake (bronze/silver) + real-time aggregates -> Postgres.

Queries started (all run concurrently in one application, FAIR scheduled):
  bronze_events          raw payload of every topic, partitioned by topic/date (replayable source of truth)
  silver_orders          parsed, validated, de-duplicated orders (10 min watermark)
  quarantine_orders      rejected orders with a reason (data-quality dead-letter table)
  silver_order_status    parsed, de-duplicated status changes
  silver_rider_locations GPS pings
  silver_reviews         ratings
  rt_city_orders         1-minute tumbling windows per city      -> realtime.city_orders_1m
  rt_cuisine_orders      1-minute tumbling windows per cuisine   -> realtime.cuisine_orders_1m
  rt_order_status        1-minute windows per city & status     -> realtime.order_status_1m
  rt_rider_activity      1-minute windows of active riders (HLL) -> realtime.rider_activity_1m
  rt_late_risk           every new order scored by the MLlib late-delivery model -> realtime.order_risk
A StreamingQueryListener records the throughput of every micro-batch in realtime.stream_progress.
"""
import logging
import os
import threading
import time

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from common import KAFKA_BOOTSTRAP, FMT, get_spark, lake, pg_connect, pg_upsert, read_pg
from schemas import ALL_TOPICS, ORDER_DDL, REVIEW_DDL, RIDER_DDL, STATUS_DDL, T_ORDERS, T_REVIEWS, T_RIDERS, T_STATUS

log = logging.getLogger("streaming")
TRIGGER = os.getenv("STREAM_TRIGGER", "15 seconds")
AGG_TRIGGER = os.getenv("AGG_TRIGGER", "10 seconds")
MAX_OFFSETS = os.getenv("MAX_OFFSETS_PER_TRIGGER", "300000")
RISK_TRIGGER = os.getenv("RISK_TRIGGER", "15 seconds")
RISK_MAX_OFFSETS = os.getenv("RISK_MAX_OFFSETS_PER_TRIGGER", "5000")
# Rolling retention for the two firehose tables (days; 0 = keep forever). Everything else is kept.
RETENTION = {
    "bronze/events": ("ingest_date", int(os.getenv("BRONZE_RETENTION_DAYS", "3"))),
    "silver/rider_locations": ("event_date_ist", int(os.getenv("RIDER_PINGS_RETENTION_DAYS", "3"))),
}


def ckpt(name):
    return lake("_checkpoints", name)


def kafka(spark, topics, starting="earliest", max_offsets=MAX_OFFSETS):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", ",".join(topics))
            .option("startingOffsets", starting)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", max_offsets)
            .load())


def parsed(spark, topic, ddl, starting="earliest", max_offsets=MAX_OFFSETS):
    return (kafka(spark, [topic], starting, max_offsets)
            .select(F.col("value").cast("string").alias("payload"), F.col("timestamp").alias("kafka_ts"))
            .withColumn("j", F.from_json("payload", ddl))
            .select("payload", "kafka_ts", "j.*")
            .withColumn("event_ts", F.to_timestamp("event_time"))
            .withColumn("event_date_ist", F.to_date(F.from_utc_timestamp("event_ts", "Asia/Kolkata"))))


def sink_lake(df, name, path, partitions, trigger=TRIGGER):
    w = (df.writeStream.format(FMT).outputMode("append")
         .option("checkpointLocation", ckpt(name))
         .queryName(name).trigger(processingTime=trigger))
    if partitions:
        w = w.partitionBy(*partitions)
    return w.start(lake(*path.split("/")))


def sink_postgres(df, name, table, keys):
    def upsert(batch_df, batch_id):
        rows = [r.asDict() for r in batch_df.collect()]   # windowed aggregates: small
        pg_upsert(table, rows, keys)
    return (df.writeStream.outputMode("update").foreachBatch(upsert)
            .option("checkpointLocation", ckpt(name))
            .queryName(name).trigger(processingTime=AGG_TRIGGER).start())


class LateRiskScorer:
    """foreachBatch sink: scores each micro-batch of new orders with the late-delivery model trained by
    ml_models.py (Spark MLlib) and upserts the probability into realtime.order_risk. Until the first model
    has been trained it does nothing; a retrained model is picked up automatically."""
    RELOAD_S, RETRY_S, KEEP_HOURS = 1800, 120, 6

    def __init__(self, spark):
        self.spark, self.model, self.next_load, self.rest, self.rest_at, self.batches = spark, None, 0, None, 0, 0
        self.disabled_until, self.failures = 0, 0
        self.high, self.medium = 0.40, 0.23
        try:
            conn = pg_connect()
            with conn, conn.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS realtime.order_risk (order_id TEXT PRIMARY KEY, event_ts TIMESTAMPTZ,
                    city TEXT, restaurant_name TEXT, cuisine TEXT, order_hour_ist INT, promised_minutes INT,
                    late_probability DOUBLE PRECISION, risk_level TEXT, updated_at TIMESTAMPTZ DEFAULT now())""")
                cur.execute("CREATE INDEX IF NOT EXISTS order_risk_ts ON realtime.order_risk (event_ts DESC)")
            conn.close()
        except Exception as exc:
            log.warning("could not prepare realtime.order_risk: %s", exc)

    def _load(self):
        if time.time() < self.next_load:
            return self.model
        try:
            from pyspark.ml import PipelineModel          # needs numpy (in the rebuilt Spark image)
            from ml_models import LATE_MODEL_PATH
            self.model = PipelineModel.load(LATE_MODEL_PATH)
            self.next_load = time.time() + self.RELOAD_S
            try:   # risk bands: Medium = flagged by the model (its F1-optimal threshold), High = 1.6x the average late rate
                conn = pg_connect()
                with conn, conn.cursor() as cur:
                    cur.execute("SELECT threshold, base_late_rate FROM ml.late_model_metrics WHERE selected LIMIT 1")
                    row = cur.fetchone()
                conn.close()
                if row:
                    self.medium = float(row[0])
                    self.high = max(self.medium + 0.05, 1.6 * float(row[1]))
            except Exception:
                pass
            log.info("late-delivery model loaded (high risk >= %.2f, medium >= %.2f)", self.high, self.medium)
        except Exception as exc:
            if self.model is None:
                log.info("late-delivery model not trained yet (%s); live scoring waits", type(exc).__name__)
            self.next_load = time.time() + self.RETRY_S
        return self.model

    def _restaurants(self):
        if self.rest is None or time.time() - self.rest_at > 3600:
            self.rest = read_pg(self.spark, "dims.restaurants").select("restaurant_id", "prep_time_min", "rating").cache()
            self.rest_at = time.time()
        return self.rest

    def __call__(self, batch_df, batch_id):
        # ML scoring is an add-on: any failure here (no numpy in an old image, memory, Postgres) is logged and the
        # query keeps running, so it can never stop the live windows and the rest of the streaming application.
        if time.time() < self.disabled_until:
            return
        try:
            self._score(batch_df)
        except Exception as exc:
            self.failures += 1
            pause = min(600, 60 * self.failures)
            log.warning("late-risk scoring skipped for %ds (%s: %s)", pause, type(exc).__name__, str(exc)[:200])
            self.disabled_until = time.time() + pause

    def _score(self, batch_df):
        model = self._load()
        if model is None or batch_df.isEmpty():
            return
        from pyspark.ml.functions import vector_to_array
        from ml_models import order_features
        df = (batch_df.dropDuplicates(["order_id"])
              .withColumn("order_hour_ist", F.hour(F.from_utc_timestamp("event_ts", "Asia/Kolkata")))
              .withColumn("order_date", F.col("event_date_ist")))
        scored = (model.transform(order_features(df, self._restaurants()))
                  .withColumn("p", F.round(vector_to_array("probability")[1], 4))
                  .select("order_id", "event_ts", "city", "restaurant_name", "cuisine", "order_hour_ist",
                          "promised_minutes", "p"))
        rows = [{"order_id": r["order_id"], "event_ts": r["event_ts"], "city": r["city"], "restaurant_name": r["restaurant_name"],
                 "cuisine": r["cuisine"], "order_hour_ist": int(r["order_hour_ist"]), "promised_minutes": int(r["promised_minutes"] or 0),
                 "late_probability": float(r["p"]),
                 "risk_level": "High" if r["p"] >= self.high else "Medium" if r["p"] >= self.medium else "Low"}
                for r in scored.collect()]
        pg_upsert("realtime.order_risk", rows, ["order_id"])
        self.batches += 1
        if self.batches % 60 == 0:
            conn = pg_connect()
            with conn, conn.cursor() as cur:
                cur.execute(f"DELETE FROM realtime.order_risk WHERE event_ts < now() - INTERVAL '{self.KEEP_HOURS} hours'")
            conn.close()


def window_cols(df):
    return df.withColumn("window_start", F.col("window.start")).withColumn("window_end", F.col("window.end")).drop("window")


class ProgressToPostgres(StreamingQueryListener):
    """Writes per-micro-batch throughput of every query to Postgres (powers the pipeline-health panel)."""

    def __init__(self):
        super().__init__()
        self.conn = None

    def _cursor(self):
        if self.conn is None or self.conn.closed:
            self.conn = pg_connect()
            self.conn.autocommit = True
        return self.conn.cursor()

    def onQueryStarted(self, event):
        log.info("Query started: %s", event.name)

    def onQueryProgress(self, event):
        p = event.progress
        try:
            with self._cursor() as cur:
                cur.execute(
                    "INSERT INTO realtime.stream_progress (query_name, batch_id, input_rows, input_rps, "
                    "processed_rps, batch_duration_ms) VALUES (%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (query_name, batch_id) DO UPDATE SET input_rows = EXCLUDED.input_rows, "
                    "input_rps = EXCLUDED.input_rps, processed_rps = EXCLUDED.processed_rps, "
                    "batch_duration_ms = EXCLUDED.batch_duration_ms, ts = now()",   # batch ids restart if checkpoints reset
                    (p.name, p.batchId, p.numInputRows, float(p.inputRowsPerSecond or 0),
                     float(p.processedRowsPerSecond or 0), int((p.durationMs or {}).get("triggerExecution", 0))))
        except Exception as exc:  # never let monitoring kill the pipeline
            log.warning("progress write failed: %s", exc)
            self.conn = None

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.error("Query terminated: %s %s", event.id, event.exception)


def retention_loop(spark):
    """Runs inside the streaming driver: Delta on S3 supports one writer process per table, and this
    keeps DELETE/VACUUM in the same JVM as the streaming writes. Partition DELETEs are metadata-only."""
    time.sleep(600)
    while True:
        for path, (col, days) in RETENTION.items():
            if days <= 0:
                continue
            try:
                spark.sql(f"DELETE FROM delta.`{lake(*path.split('/'))}` WHERE {col} < date_sub(current_date(), {days})")
                spark.sql(f"VACUUM delta.`{lake(*path.split('/'))}` RETAIN 24 HOURS")
                log.info("retention: %s keeps %d days", path, days)
            except Exception as exc:
                log.warning("retention on %s failed: %s", path, exc)
        time.sleep(3600)


def main():
    spark = get_spark("zomato-streaming", streaming=True)
    spark.streams.addListener(ProgressToPostgres())

    # ---------------------------------------------------------------- BRONZE: raw, replayable
    bronze = (kafka(spark, ALL_TOPICS)
              .select("topic", "partition", "offset", F.col("timestamp").alias("kafka_ts"),
                      F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("payload"))
              .withColumn("ingested_at", F.current_timestamp())
              .withColumn("ingest_date", F.to_date("kafka_ts")))
    sink_lake(bronze, "bronze_events", "bronze/events", ["topic", "ingest_date"])

    # ---------------------------------------------------------------- SILVER: orders + quarantine
    orders = parsed(spark, T_ORDERS, ORDER_DDL)
    reason = (F.when(F.col("order_id").isNull() & F.col("event_type").isNull(), "corrupt_payload")
               .when(F.col("order_id").isNull(), "missing_order_id")
               .when(F.col("city").isNull(), "missing_city")
               .when(F.col("total_amount").isNull() | (F.col("total_amount") <= 0), "non_positive_amount")
               .when(F.col("event_ts").isNull(), "bad_timestamp")
               .when(F.col("event_ts") > F.current_timestamp() + F.expr("INTERVAL 5 MINUTES"), "future_timestamp"))
    orders = orders.withColumn("dq_reason", reason)
    valid_orders = orders.where(F.col("dq_reason").isNull()).drop("dq_reason")

    silver_orders = (valid_orders
                     .withWatermark("event_ts", "10 minutes")
                     .dropDuplicatesWithinWatermark(["order_id"])
                     .withColumn("order_hour_ist", F.hour(F.from_utc_timestamp("event_ts", "Asia/Kolkata")))
                     .drop("payload"))
    sink_lake(silver_orders, "silver_orders", "silver/orders", ["event_date_ist"])

    quarantine = (orders.where(F.col("dq_reason").isNotNull())
                  .select("dq_reason", "order_id", "city", "total_amount", "event_time", "payload", "kafka_ts")
                  .withColumn("quarantine_date", F.to_date("kafka_ts")))
    sink_lake(quarantine, "quarantine_orders", "silver/quarantine_orders", ["quarantine_date"])

    # ---------------------------------------------------------------- SILVER: status, riders, reviews
    status = parsed(spark, T_STATUS, STATUS_DDL).where("order_id IS NOT NULL AND status IS NOT NULL AND event_ts IS NOT NULL")
    silver_status = (status.withWatermark("event_ts", "15 minutes")
                     .dropDuplicatesWithinWatermark(["order_id", "status"]).drop("payload"))
    sink_lake(silver_status, "silver_order_status", "silver/order_status", ["event_date_ist"])

    riders = parsed(spark, T_RIDERS, RIDER_DDL).where("rider_id IS NOT NULL AND event_ts IS NOT NULL")
    sink_lake(riders.drop("payload"), "silver_rider_locations", "silver/rider_locations", ["event_date_ist"])

    reviews = parsed(spark, T_REVIEWS, REVIEW_DDL).where("order_id IS NOT NULL AND rating BETWEEN 1 AND 5")
    sink_lake(reviews.drop("payload"), "silver_reviews", "silver/reviews", ["event_date_ist"])

    # ---------------------------------------------------------------- REAL-TIME aggregates -> Postgres
    # (approximate "speed layer": exact numbers come from the batch gold layer)
    w_orders = valid_orders.withWatermark("event_ts", "2 minutes")
    city_agg = window_cols(
        w_orders.groupBy(F.window("event_ts", "1 minute"), "city")
        .agg(F.count(F.lit(1)).alias("orders"),
             F.round(F.sum("total_amount"), 2).alias("gmv"),
             F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
             F.sum(F.col("is_gold").cast("int")).alias("gold_orders"),
             F.sum(F.when(F.col("coupon_code").isNotNull(), 1).otherwise(0)).alias("coupon_orders")))
    sink_postgres(city_agg, "rt_city_orders", "realtime.city_orders_1m", ["window_start", "city"])

    cuisine_agg = window_cols(
        w_orders.groupBy(F.window("event_ts", "1 minute"), "cuisine")
        .agg(F.count(F.lit(1)).alias("orders"), F.round(F.sum("total_amount"), 2).alias("gmv")))
    sink_postgres(cuisine_agg, "rt_cuisine_orders", "realtime.cuisine_orders_1m", ["window_start", "cuisine"])

    status_agg = window_cols(
        status.where("city IS NOT NULL").withWatermark("event_ts", "2 minutes")
        .groupBy(F.window("event_ts", "1 minute"), "city", "status")
        .agg(F.count(F.lit(1)).alias("events"),
             F.round(F.avg("delivery_minutes"), 2).alias("avg_delivery_minutes")))
    sink_postgres(status_agg, "rt_order_status", "realtime.order_status_1m", ["window_start", "city", "status"])

    rider_agg = window_cols(
        riders.where("city IS NOT NULL").withWatermark("event_ts", "2 minutes")
        .groupBy(F.window("event_ts", "1 minute"), "city")
        .agg(F.approx_count_distinct("rider_id").alias("active_riders"),
             F.count(F.lit(1)).alias("pings"),
             F.round(F.avg("speed_kmph"), 1).alias("avg_speed_kmph")))
    sink_postgres(rider_agg, "rt_rider_activity", "realtime.rider_activity_1m", ["window_start", "city"])

    # ---------------------------------------------------------------- ML: late-delivery risk for every new order
    # Its own Kafka source starting at the *latest* offset with small batches: only orders arriving from now on are
    # scored, so the ML query never replays the topic's history and never competes with the live windows for CPU.
    risk_orders = (parsed(spark, T_ORDERS, ORDER_DDL, starting="latest", max_offsets=RISK_MAX_OFFSETS)
                   .where("order_id IS NOT NULL AND city IS NOT NULL AND total_amount > 0 AND event_ts IS NOT NULL")
                   .where(F.col("event_ts") >= F.current_timestamp() - F.expr("INTERVAL 15 MINUTES")))
    (risk_orders.writeStream.foreachBatch(LateRiskScorer(spark))
     .option("checkpointLocation", ckpt("rt_late_risk_v2"))   # fresh checkpoint so existing installs start at 'latest'
     .queryName("rt_late_risk").trigger(processingTime=RISK_TRIGGER).start())

    log.info("Started %d streaming queries", len(spark.streams.active))
    if FMT == "delta":
        threading.Thread(target=retention_loop, args=(spark,), daemon=True, name="retention").start()
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
