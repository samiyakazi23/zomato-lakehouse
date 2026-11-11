"""Incremental GOLD layer (scheduled by Airflow every 30 minutes). Cost stays flat as data grows.
Produces the serving tables behind every tab of the Zomato Analytics website and the dbt marts.

1. Streaming side: rebuilds additive daily aggregates for only the last STREAM_RECOMPUTE_DAYS days
   of streamed orders (order + final status + delivery time + rating, de-duplicated on order_id)
   and replaces just those date partitions in gold/stream_agg_*.
2. History side: reads the pre-computed gold/hist_agg_* written by the backfill (never the raw
   billion-row table).
3. Combines both, derives the serving metrics, and writes them to Delta (gold/*) and Postgres
   (gold.*), where dbt builds the marts and Grafana reads them.
"""
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from aggregates import combine, daily_aggregates, ratio
from common import FMT, get_spark, lake, read_lake, write_pg
from schemas import FACT_COLUMNS

log = logging.getLogger("gold")
RECOMPUTE_DAYS = int(os.getenv("STREAM_RECOMPUTE_DAYS", "2"))
IST = timezone(timedelta(hours=5, minutes=30))


def streaming_fact(spark, since):
    orders = read_lake(spark, "silver", "orders")
    if orders is None:
        return None
    status, reviews = read_lake(spark, "silver", "order_status"), read_lake(spark, "silver", "reviews")
    if since is not None:   # partition pruning on event_date_ist keeps this a few days of data
        orders = orders.where(F.col("event_date_ist") >= F.lit(since))
        status = status.where(F.col("event_date_ist") >= F.lit(since)) if status is not None else None
        reviews = reviews.where(F.col("event_date_ist") >= F.lit(since)) if reviews is not None else None

    o = (orders.dropDuplicates(["order_id"])
         .withColumn("order_ts", F.col("event_ts")).withColumn("order_date", F.col("event_date_ist")))
    if status is not None:
        s = status.groupBy("order_id").agg(
            F.max(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("is_delivered"),
            F.max(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("is_cancelled"),
            F.max("delivery_minutes").alias("delivery_minutes"),
            F.max("distance_km").alias("distance_km"),
            F.max("rider_id").alias("rider_id"),
            F.max("cancel_reason").alias("cancel_reason"))
        o = o.join(s, "order_id", "left")
    else:
        for c, t in [("is_delivered", "int"), ("is_cancelled", "int"), ("delivery_minutes", "double"),
                     ("distance_km", "double"), ("rider_id", "string"), ("cancel_reason", "string")]:
            o = o.withColumn(c, F.lit(None).cast(t))
    if reviews is not None:
        o = o.join(reviews.groupBy("order_id").agg(F.max("rating").alias("rating")), "order_id", "left")
    else:
        o = o.withColumn("rating", F.lit(None).cast("int"))
    o = (o.withColumn("final_status", F.when(F.col("is_cancelled") == 1, "CANCELLED")
                      .when(F.col("is_delivered") == 1, "DELIVERED").otherwise("IN_PROGRESS"))
         .withColumn("source", F.lit("stream")))
    return o.select(*FACT_COLUMNS)


def refresh_stream_aggregates(spark):
    first_run = read_lake(spark, "gold", "stream_agg_city_day") is None
    since = None if first_run else (datetime.now(IST).date() - timedelta(days=RECOMPUTE_DAYS))
    fact = streaming_fact(spark, since)
    if fact is None:
        log.info("No streaming data yet")
        return
    log.info("Recomputing streaming aggregates %s", "for all dates" if first_run else f"since {since}")
    for name, agg in daily_aggregates(fact, customers="day").items():
        w = (agg.repartition(1).write.format(FMT).partitionBy("order_date").mode("overwrite"))
        w = w.option("overwriteSchema", "true") if first_run else w.option("partitionOverwriteMode", "dynamic")
        w.save(lake("gold", f"stream_agg_{name}"))


def load_combined(spark, name, since=None):
    frames = []
    for src in ("hist", "stream"):
        df = read_lake(spark, "gold", f"{src}_agg_{name}")
        if df is None:
            continue
        if since is not None:
            if "order_month" in df.columns:  # prune history partitions first
                df = df.where(F.col("order_month") >= F.lit(since.strftime("%Y-%m")))
            df = df.where(F.col("order_date") >= F.lit(since))
        frames.append(df)
    return combine(name, frames)


def pct(num, den, digits=4):
    return F.round(F.col(num) / F.when(F.col(den) > 0, F.col(den)), digits)


def sum_all(df, keys, measures):
    return df.groupBy(*keys).agg(*[F.sum(m).alias(m) for m in measures])


def customer_tables(spark, as_of):
    """Cohorts, RFM segments, Gold split and top customers from monthly customer activity."""
    hist = read_lake(spark, "gold", "hist_agg_customer_month")
    stream = read_lake(spark, "gold", "stream_agg_customer_day")
    if stream is not None:
        stream = (stream.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
                  .groupBy("order_month", "customer_id", "city", "is_gold")
                  .agg(F.sum("orders").alias("orders"), F.sum("amount_sum").alias("amount_sum"),
                       F.min("first_order_date").alias("first_order_date"), F.max("last_order_date").alias("last_order_date")))
    cm = combine("customer_month", [hist, stream])
    if cm is None:
        return {}
    cm = (cm.groupBy("order_month", "customer_id")
          .agg(F.sum("orders").alias("orders"), F.sum("amount_sum").alias("amount_sum"), F.max("city").alias("city"),
               F.max(F.col("is_gold").cast("int")).alias("is_gold"), F.min("first_order_date").alias("first_order_date"),
               F.max("last_order_date").alias("last_order_date"))).cache()
    cust = (cm.groupBy("customer_id")
            .agg(F.min("first_order_date").alias("first_order_date"), F.max("last_order_date").alias("last_order_date"),
                 F.sum("orders").alias("orders"), F.sum("amount_sum").alias("gmv"), F.count("*").alias("months_active"),
                 F.max("city").alias("city"), F.max("is_gold").cast("boolean").alias("is_gold"))
            .withColumn("cohort", F.date_format("first_order_date", "yyyy-MM"))
            .withColumn("recency_days", F.datediff(F.lit(as_of), F.col("last_order_date")))
            .withColumn("tenure_days", F.datediff(F.lit(as_of), F.col("first_order_date")))).cache()

    f50, f75 = cust.approxQuantile("orders", [0.5, 0.75], 0.01)
    seg = (F.when(F.col("tenure_days") <= 30, "New")
            .when((F.col("recency_days") <= 7) & (F.col("orders") >= f75), "Champions")
            .when((F.col("recency_days") <= 30) & (F.col("orders") >= f50), "Loyal")
            .when((F.col("recency_days") <= 90) & (F.col("orders") >= f50), "At risk")
            .when(F.col("recency_days") > 90, "Hibernating")
            .otherwise("Occasional"))
    cust = cust.withColumn("segment", seg)

    new_by_month = cust.groupBy(F.col("cohort").alias("order_month")).agg(F.count("*").alias("new_customers"))
    monthly = (cm.groupBy("order_month").agg(F.count("*").alias("active_customers"), F.sum("orders").alias("orders"),
                                             F.round(F.sum("amount_sum"), 2).alias("gmv"))
               .join(new_by_month, "order_month", "left").fillna(0, ["new_customers"])
               .withColumn("orders_per_customer", F.round(F.col("orders") / F.col("active_customers"), 2)))

    cohorts = (cm.join(cust.select("customer_id", "cohort"), "customer_id")
               .withColumn("month_number", F.months_between(F.to_date(F.concat("order_month", F.lit("-01"))),
                                                            F.to_date(F.concat("cohort", F.lit("-01")))).cast("int"))
               .groupBy("cohort", "month_number").agg(F.count("*").alias("customers")))
    sizes = cohorts.where("month_number = 0").select("cohort", F.col("customers").alias("cohort_size"))
    cohorts = cohorts.join(sizes, "cohort").withColumn("retention", F.round(F.col("customers") / F.col("cohort_size"), 4))

    band = (F.when(F.col("orders") == 1, "1").when(F.col("orders") <= 3, "2-3").when(F.col("orders") <= 6, "4-6")
             .when(F.col("orders") <= 12, "7-12").when(F.col("orders") <= 24, "13-24").when(F.col("orders") <= 50, "25-50")
             .otherwise("51+"))
    out = {
        "customer_monthly": monthly,
        "customer_cohorts": cohorts,
        "customer_segments": cust.groupBy("city", "segment").agg(
            F.count("*").alias("customers"), F.sum("orders").alias("orders"), F.round(F.sum("gmv"), 2).alias("gmv"),
            F.round(F.avg("recency_days"), 1).alias("avg_recency_days"), F.round(F.avg("orders"), 2).alias("avg_orders"),
            F.round(F.avg("gmv"), 2).alias("avg_spend")),
        "customer_gold_split": cust.groupBy("city", "is_gold").agg(
            F.count("*").alias("customers"), F.sum("orders").alias("orders"), F.round(F.sum("gmv"), 2).alias("gmv")),
        "customer_frequency": cust.withColumn("orders_band", band).groupBy("orders_band").agg(F.count("*").alias("customers")),
        "top_customers": cust.orderBy(F.desc("gmv")).limit(50).select(
            "customer_id", "city", "is_gold", "orders", F.round("gmv", 2).alias("gmv"), "first_order_date",
            "last_order_date", "segment"),
    }
    log.info(f"customers: {cust.count():,} (orders p50={f50:.0f}, p75={f75:.0f})")
    return out


def main():
    started = time.time()
    spark = get_spark("zomato-gold")
    refresh_stream_aggregates(spark)

    city_day = load_combined(spark, "city_day")
    if city_day is None:
        log.warning("No data yet - nothing to serve")
        return
    city_day = city_day.cache()
    max_date = city_day.agg(F.max("order_date")).first()[0]
    total = city_day.agg(F.sum("orders")).first()[0]
    log.info(f"Serving {total:,} orders up to {max_date}")
    since90, since30 = max_date - timedelta(days=90), max_date - timedelta(days=30)
    closed = F.greatest(F.col("delivered_orders") + F.col("cancelled_orders"), F.lit(1))

    outputs = {
        "daily_city_metrics": city_day.select(
            "order_date", "city", "orders", "delivered_orders", "cancelled_orders",
            F.round("gmv", 2).alias("gmv"),
            F.round(F.col("amount_sum") / F.col("orders"), 2).alias("avg_order_value"),
            F.round(F.col("cancelled_orders") / closed, 4).alias("cancel_rate"),
            pct("delivery_sum", "delivery_cnt", 2).alias("avg_delivery_minutes"),
            "p90_delivery_minutes",
            pct("rating_sum", "rating_cnt", 2).alias("avg_rating"),
            "unique_customers",
            pct("gold_orders", "orders").alias("gold_share"),
            F.round("discount_sum", 2).alias("total_discount"),
            F.col("rating_cnt").alias("ratings_count"),
            pct("rating_cnt", "delivered_orders").alias("review_rate"),
            pct("late_orders", "delivered_orders").alias("late_rate"),
            pct("coupon_orders", "orders").alias("coupon_share"),
            pct("cod_orders", "orders").alias("cod_share"),
            pct("items_sum", "orders", 2).alias("avg_items"),
            F.round(F.col("gold_amount_sum") / F.when(F.col("gold_orders") > 0, F.col("gold_orders")), 2).alias("gold_aov"),
            F.round((F.col("amount_sum") - F.col("gold_amount_sum"))
                    / F.when(F.col("orders") > F.col("gold_orders"), F.col("orders") - F.col("gold_orders")), 2).alias("non_gold_aov")),
    }

    rest = load_combined(spark, "restaurant_day", since90)
    outputs["restaurant_performance"] = (
        sum_all(rest, ["restaurant_id", "restaurant_name", "city", "cuisine"],
                ["orders", "delivered_orders", "cancelled_orders", "amount_sum", "gmv", "delivery_sum", "delivery_cnt",
                 "rating_sum", "rating_cnt"])
        .select("restaurant_id", "restaurant_name", "city", "cuisine", "orders", F.round("gmv", 2).alias("gmv"),
                F.round(F.col("amount_sum") / F.col("orders"), 2).alias("avg_order_value"),
                pct("rating_sum", "rating_cnt", 2).alias("avg_rating"), F.col("rating_cnt").alias("ratings_count"),
                F.round(F.col("cancelled_orders") / closed, 4).alias("cancel_rate"),
                pct("delivery_sum", "delivery_cnt", 2).alias("avg_delivery_minutes")))

    hours = load_combined(spark, "city_hour", since90)
    outputs["hourly_demand"] = (
        hours.withColumn("day_of_week", F.date_format("order_date", "E"))
        .groupBy("city", "day_of_week", "hour_of_day")
        .agg(F.sum("orders").alias("orders"), F.sum("amount_sum").alias("amount_sum"))
        .select("city", "day_of_week", "hour_of_day", "orders",
                F.round(F.col("amount_sum") / F.col("orders"), 2).alias("avg_order_value")))

    riders = load_combined(spark, "rider_day", since30)
    outputs["rider_daily"] = riders.select(
        "order_date", "rider_id", "city", "deliveries",
        pct("delivery_sum", "delivery_cnt", 2).alias("avg_delivery_minutes"),
        F.round("distance_sum", 2).alias("distance_km"),
        pct("rating_sum", "rating_cnt", 2).alias("avg_rating"))

    payments = load_combined(spark, "payment_day")
    outputs["payment_mix"] = payments.select("order_date", "payment_method", "platform", "orders",
                                             F.round("amount_sum", 2).alias("gmv"))

    basket = load_combined(spark, "basket_day", since90)
    outputs["basket_mix"] = sum_all(basket, ["city", "items_bucket", "value_band"], ["orders", "amount_sum"]) \
        .withColumn("amount_sum", F.round("amount_sum", 2))
    delivery = load_combined(spark, "delivery_day", since90)
    outputs["delivery_distribution"] = sum_all(delivery, ["city", "delivery_bucket"], ["orders"])
    ratings = load_combined(spark, "rating_day", since90)
    outputs["rating_matrix"] = sum_all(ratings, ["city", "cuisine", "late_bucket", "rating"], ["reviews"])
    coupons = load_combined(spark, "coupon_day", since90)
    outputs["coupon_performance"] = (sum_all(coupons, ["coupon", "is_gold"],
                                             ["orders", "amount_sum", "discount_sum", "cancelled_orders"])
                                     .select("coupon", "is_gold", "orders", F.round("amount_sum", 2).alias("gmv"),
                                             F.round("discount_sum", 2).alias("discount"), "cancelled_orders"))
    cancels = load_combined(spark, "cancel_day", since90)
    outputs["cancellation_reasons"] = (sum_all(cancels, ["city", "cancel_reason"], ["orders"])
                                       if cancels is not None else
                                       spark.createDataFrame([], "city STRING, cancel_reason STRING, orders BIGINT"))

    outputs.update(customer_tables(spark, max_date))

    quarantine = read_lake(spark, "silver", "quarantine_orders")
    outputs["dq_quarantine_daily"] = (
        quarantine.groupBy("quarantine_date", "dq_reason").agg(F.count("*").alias("rejected_rows"))
        if quarantine is not None else   # keep the table (and its dbt model) valid before any bad data
        spark.createDataFrame([], "quarantine_date DATE, dq_reason STRING, rejected_rows BIGINT"))

    for name, df in outputs.items():
        path = lake("gold", name)
        df.coalesce(4).write.format(FMT).mode("overwrite").option("overwriteSchema", "true").save(path)
        write_pg(spark.read.format(FMT).load(path), f"gold.{name}")
        log.info("gold.%s refreshed", name)

    refresh = spark.createDataFrame(
        [(datetime.now(timezone.utc).replace(tzinfo=None), int(total), max_date, round(time.time() - started, 1))],
        "refreshed_at TIMESTAMP, orders_served BIGINT, data_through DATE, duration_s DOUBLE")
    write_pg(refresh, "gold.refresh_log")
    city_day.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
