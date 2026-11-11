"""Additive daily aggregates: the trick that makes billion-row history cheap to serve.

Every aggregate stores sums and counts (never averages), so aggregates computed separately -
one month of history at a time, or the last few days of streaming data - can be combined later
with a plain SUM. The 30-minute batch job then reads a few million aggregate rows instead of
rescanning billions of orders. Each one feeds a tab of the Zomato Analytics website.
"""
from pyspark.sql import functions as F

KEYS = {
    "city_day":       ["order_date", "city"],
    "restaurant_day": ["order_date", "restaurant_id", "restaurant_name", "city", "cuisine"],
    "city_hour":      ["order_date", "city", "hour_of_day"],
    "rider_day":      ["order_date", "rider_id", "city"],
    "payment_day":    ["order_date", "payment_method", "platform"],
    "basket_day":     ["order_date", "city", "items_bucket", "value_band"],
    "delivery_day":   ["order_date", "city", "delivery_bucket"],
    "rating_day":     ["order_date", "city", "cuisine", "late_bucket", "rating"],
    "coupon_day":     ["order_date", "coupon", "is_gold"],
    "cancel_day":     ["order_date", "city", "cancel_reason"],
    "customer_day":   ["order_date", "customer_id", "city", "is_gold"],      # streaming side
    "customer_month": ["order_month", "customer_id", "city", "is_gold"],     # history side
}
MAX_MEASURES = {"p90_delivery_minutes", "last_order_date"}   # p90: one source owns each day, so MAX is exact
MIN_MEASURES = {"first_order_date"}



def _delivered():
    return F.col("final_status") == "DELIVERED"


def _cancelled():
    return F.col("final_status") == "CANCELLED"


def _late_min():
    return F.col("delivery_minutes") - F.col("promised_minutes")


def _i(cond):
    return F.when(cond, 1).otherwise(0)


def _with_derived(fact):
    LATE_MIN = _late_min()
    return (fact
            .withColumn("items_bucket", F.least(F.col("items_count"), F.lit(5)).cast("int"))   # 5 = "5+"
            .withColumn("value_band", (F.least(F.floor(F.col("total_amount") / 100), F.lit(15)) * 100).cast("int"))
            .withColumn("delivery_bucket", (F.least(F.floor(F.col("delivery_minutes") / 5), F.lit(18)) * 5).cast("int"))
            .withColumn("late_bucket", F.when(LATE_MIN <= 0, "On time").when(LATE_MIN <= 10, "Up to 10 min late")
                        .otherwise("10+ min late")))


def daily_aggregates(fact, rider_since=None, customers="day"):
    """fact: rows with the FACT_COLUMNS schema -> dict of aggregate DataFrames (keys above)."""
    fact = _with_derived(fact)
    DELIVERED, CANCELLED, LATE_MIN = _delivered(), _cancelled(), _late_min()
    common = [
        F.count(F.lit(1)).alias("orders"),
        F.sum(_i(DELIVERED)).alias("delivered_orders"),
        F.sum(_i(CANCELLED)).alias("cancelled_orders"),
        F.sum("total_amount").alias("amount_sum"),
        F.sum(F.when(DELIVERED, F.col("total_amount")).otherwise(0.0)).alias("gmv"),
        F.sum("delivery_minutes").alias("delivery_sum"),
        F.count("delivery_minutes").alias("delivery_cnt"),
        F.sum("rating").alias("rating_sum"),
        F.count("rating").alias("rating_cnt"),
    ]
    out = {
        "city_day": fact.groupBy(*KEYS["city_day"]).agg(
            *common,
            F.percentile_approx("delivery_minutes", 0.9).cast("double").alias("p90_delivery_minutes"),
            F.approx_count_distinct("customer_id").alias("unique_customers"),
            F.sum(F.col("is_gold").cast("int")).alias("gold_orders"),
            F.sum(F.when(F.col("is_gold"), F.col("total_amount")).otherwise(0.0)).alias("gold_amount_sum"),
            F.sum("discount").alias("discount_sum"),
            F.sum(_i(F.col("coupon_code").isNotNull())).alias("coupon_orders"),
            F.sum("items_count").alias("items_sum"),
            F.sum(_i(DELIVERED & (LATE_MIN > 0))).alias("late_orders"),
            F.sum(_i(F.col("payment_method") == "Cash on Delivery")).alias("cod_orders")),
        "restaurant_day": fact.groupBy(*KEYS["restaurant_day"]).agg(*common),
        "city_hour": fact.withColumnRenamed("order_hour_ist", "hour_of_day").groupBy(*KEYS["city_hour"]).agg(
            F.count(F.lit(1)).alias("orders"), F.sum("total_amount").alias("amount_sum")),
        "payment_day": fact.groupBy(*KEYS["payment_day"]).agg(
            F.count(F.lit(1)).alias("orders"), F.sum("total_amount").alias("amount_sum")),
        "basket_day": fact.groupBy(*KEYS["basket_day"]).agg(
            F.count(F.lit(1)).alias("orders"), F.sum("total_amount").alias("amount_sum")),
        "delivery_day": fact.where(DELIVERED & F.col("delivery_minutes").isNotNull())
            .groupBy(*KEYS["delivery_day"]).agg(F.count(F.lit(1)).alias("orders")),
        "rating_day": fact.where(F.col("rating").isNotNull())
            .groupBy(*KEYS["rating_day"]).agg(F.count(F.lit(1)).alias("reviews")),
        "coupon_day": fact.withColumn("coupon", F.coalesce("coupon_code", F.lit("No coupon")))
            .groupBy(*KEYS["coupon_day"]).agg(
                F.count(F.lit(1)).alias("orders"), F.sum("total_amount").alias("amount_sum"),
                F.sum("discount").alias("discount_sum"), F.sum(_i(CANCELLED)).alias("cancelled_orders")),
        "cancel_day": fact.where(CANCELLED).withColumn("cancel_reason", F.coalesce("cancel_reason", F.lit("Unknown")))
            .groupBy(*KEYS["cancel_day"]).agg(F.count(F.lit(1)).alias("orders")),
    }
    customer_measures = [F.count(F.lit(1)).alias("orders"), F.sum("total_amount").alias("amount_sum"),
                         F.min("order_date").alias("first_order_date"), F.max("order_date").alias("last_order_date")]
    if customers == "month":
        out["customer_month"] = (fact.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
                                 .groupBy(*KEYS["customer_month"]).agg(*customer_measures))
    else:
        out["customer_day"] = fact.groupBy(*KEYS["customer_day"]).agg(*customer_measures)

    riders = fact.where(DELIVERED & F.col("rider_id").isNotNull())
    if rider_since is not None:
        riders = riders.where(F.col("order_date") >= F.lit(rider_since))
    out["rider_day"] = riders.groupBy(*KEYS["rider_day"]).agg(
        F.count(F.lit(1)).alias("deliveries"),
        F.sum("delivery_minutes").alias("delivery_sum"),
        F.count("delivery_minutes").alias("delivery_cnt"),
        F.sum("distance_km").alias("distance_sum"),
        F.sum("rating").alias("rating_sum"),
        F.count("rating").alias("rating_cnt"))
    return out


def combine(name, frames):
    """Union aggregate frames from several sources and re-aggregate by key."""
    frames = [f for f in frames if f is not None]
    if not frames:
        return None
    keys = KEYS[name]
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f, allowMissingColumns=True)
    skip = set(keys) | ({"order_month"} if "order_month" not in keys else set())
    measures = [c for c in df.columns if c not in skip]

    def agg(c):
        if c in MAX_MEASURES:
            return F.max(c).alias(c)
        if c in MIN_MEASURES:
            return F.min(c).alias(c)
        return F.sum(c).alias(c)
    return df.groupBy(*keys).agg(*[agg(c) for c in measures])


def ratio(num, den, digits=4):
    return F.round(F.col(num) / F.when(F.col(den) > 0, F.col(den)), digits)
