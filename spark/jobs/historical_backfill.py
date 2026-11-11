"""Billion-scale historical order generator (distributed, chunked, resumable).

    --orders 1000000000 --days 365      # ~1B orders, ~2.7M per day

Design for scale:
  * One Spark partition per day, built from a tiny "plan" of (day, slice) rows exploded into
    order rows. Generation is a pure map: no shuffle, bounded memory, linear in cores.
  * History is written one calendar month at a time ("chunk") with dynamic partition overwrite,
    so a chunk can be re-run safely and an interrupted job resumes where it stopped.
  * Every chunk also writes compact additive daily aggregates (gold/hist_agg_*), so the
    30-minute batch job never has to rescan the billion-row table.
Realistic signals: lunch & dinner peaks (IST), weekend uplift, business growth over the year,
power-law restaurant popularity, monsoon & peak-hour delays, Gold free delivery, coupons,
cancellations, and ratings that drop when food is late.
"""
import argparse
import logging
import math
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import Row, Window
from pyspark.sql import functions as F

from aggregates import daily_aggregates
from common import FMT, get_spark, lake, read_lake, read_pg
from schemas import RAIN_MONTHS, CITY_GOLD_PTS, CITY_GROWTH, CITY_HOUR_SHIFT, CITY_TRAFFIC_MIN, FACT_COLUMNS

log = logging.getLogger("backfill")
IST = timezone(timedelta(hours=5, minutes=30))

HOURLY = [3, 2, 1, 1, 1, 1, 2, 3, 5, 5, 6, 8, 13, 15, 11, 7, 6, 8, 10, 14, 16, 15, 11, 6]
HOUR_BAG = [h for h, w in enumerate(HOURLY) for _ in range(w)]
ROWS_PER_SLICE = 1_000_000
ORDERS_PER_CUSTOMER_YEAR = 30    # sizes the customer base to the history (generated IDs beyond the named ones)
CORE_SHARE = 0.25                                    # loyal customers who order all year
LIFETIMES = (20, 90, 300)                            # active life (days) of short / medium / long-lived customers       # rows exploded from one plan row (bounds per-row memory)
MAX_ROWS_PER_FILE = 4_000_000    # ~150-250 MB parquet files


def city_map(d):
    return F.create_map(*[x for k, v in d.items() for x in (F.lit(k), F.lit(v))])


def pick(values, seed):
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.floor(F.rand(seed) * len(values)) + 1).cast("int"))


def day_weights(days, anchor):
    """Relative order volume for each history day: growth over time + weekend uplift."""
    weights = {}
    for back in range(1, days + 1):
        d = anchor - timedelta(days=back)
        growth = 1.0 + (1.0 - back / days)       # steady growth: ~2x busier than a year ago
        weekend = 1.25 if d.weekday() >= 5 else 1.0
        festive = 1.12 if d.month in (10, 11) and 15 <= d.day <= 28 else 1.0   # Diwali / festive season
        weights[d] = growth * weekend * festive
    return weights


def month_chunks(dates, months_per_chunk):
    months = sorted({d.replace(day=1) for d in dates})
    for i in range(0, len(months), months_per_chunk):
        group = set(months[i:i + months_per_chunk])
        yield [d for d in sorted(dates) if d.replace(day=1) in group]


def load_dimensions(spark, n_orders, days):
    restaurants = read_pg(spark, "dims.restaurants").select(
        "restaurant_id", "restaurant_name", "city", "locality", "cuisine", "avg_cost_for_two", "prep_time_min",
        F.col("rating").cast("double").alias("listed_rating"))
    n_rest = restaurants.count()
    if n_rest == 0:
        raise SystemExit("dims.restaurants is empty - start the generator first")
    restaurants = (restaurants
                   .withColumn("r_idx", (F.row_number().over(Window.orderBy("restaurant_id")) - 1).cast("long"))
                   .withColumn("avg_cost_for_two", F.col("avg_cost_for_two").cast("double")).cache())

    def indexed(df, keys):
        keys = [keys] if isinstance(keys, str) else keys
        return df.withColumn("idx", (F.row_number().over(Window.partitionBy("city").orderBy(*keys)) - 1).cast("int"))

    # customers are indexed in sign-up order, so "the first k customers of a city" = those who had joined by then
    customers = indexed(read_pg(spark, "dims.customers").select("customer_id", "city", "is_gold", "signup_date"),
                        ["signup_date", "customer_id"]).drop("signup_date").cache()
    riders = indexed(read_pg(spark, "dims.riders").select("rider_id", "city", "vehicle_type"), "rider_id").cache()
    counts = (customers.groupBy("city").agg(F.count("*").alias("n_cust"))
              .join(riders.groupBy("city").agg(F.count("*").alias("n_riders")), "city"))
    total_named = counts.agg(F.sum("n_cust")).first()[0]
    total_riders = counts.agg(F.sum("n_riders")).first()[0]
    counts = counts.withColumn("rider_share", F.col("n_riders") / F.lit(float(total_riders)))
    target = max(total_named, int(n_orders * 365 / days / ORDERS_PER_CUSTOMER_YEAR))
    counts = counts.withColumn("n_virt", F.greatest(F.col("n_cust"), F.ceil(F.col("n_cust") * F.lit(target / total_named)))).cache()
    log.info(f"customer base: {target:,} ({total_named:,} named in dims.customers)")
    return restaurants, n_rest, customers, riders, counts


def customer_index(days, anchor):
    """Which customer places an order, as a pure column expression (no shuffle, scales to billions).

    Customers are a sequence ordered by sign-up. A loyal core (the first 25%) orders all year and
    places most orders. Everyone else signs up at an accelerating rate (the business is growing)
    and stays active for a limited life: 60% for about 20 days (two or three orders, then gone),
    30% for about 3 months, 10% for about 10 months. This produces realistic cohort retention:
    roughly half order again in month 1, a steep drop by month 3, a long thin tail after.
    """
    short, medium, long = LIFETIMES
    n = F.col("n_virt")
    core = F.floor(n * CORE_SHARE)
    m_total = n - core
    span = float(days + long)
    t = F.lit(float(days)) - F.datediff(F.lit(anchor), F.col("order_date")) + F.lit(float(long))
    hi = F.least(m_total - 1, F.floor(m_total * F.pow(t / span, 1.6)))   # customers signed up so far
    rate = m_total * 1.6 * F.pow(t, 0.6) / F.lit(span ** 1.6)            # sign-ups per day right now
    skew = F.pow(F.col("u_cust"), 1.2)

    def window(life, first_digit, digits):
        m = hi - F.floor(rate * life * skew)
        # early in the history the window is wider than the customers who exist yet:
        # spread those orders evenly over everyone signed up so far (never pile them on one ID)
        m = F.when(m < 0, F.floor(F.col("u_wrap") * (hi + 1))).otherwise(m)
        return m - (m % 10) + first_digit + (m % 10) % digits            # land on this class's digits
    u = F.col("u_seg")
    m = (F.when(u < 0.86, window(short, 0, 6))       # digits 0-5: short-lived (60% of sign-ups)
          .when(u < 0.93, window(medium, 6, 3))      # digits 6-8: medium (30%)
          .otherwise(window(long, 9, 1)))            # digit 9: long-lived (10%)
    m = F.greatest(F.lit(0), F.least(m_total - 1, m))
    return F.when(u < 0.82, F.floor(F.col("u_cust") * core)).otherwise(core + m).cast("int")


def element(d, col, default):
    return F.coalesce(F.element_at(city_map(d), F.col(col)), F.lit(default))


DELIVERIES_PER_SHIFT = 14       # a rider on shift does ~14 deliveries a day
VEHICLE_MIN = {"Bicycle": 7.0, "Scooter": 0.0, "Bike": 0.0, "EV Scooter": -1.0}


def build_chunk(spark, chunk_dates, per_day, dims, seed, anchor, days):
    restaurants, n_rest, customers, riders, counts = dims
    slices = max(1, math.ceil(max(per_day[d] for d in chunk_dates) / ROWS_PER_SLICE))
    day_orders = F.create_map(*[x for d in chunk_dates for x in (F.lit(d.isoformat()), F.lit(per_day[d]))])
    plan = []
    for d in chunk_dates:                       # same #slices per day => exactly one day per partition
        n, size = per_day[d], math.ceil(per_day[d] / slices)
        for s in range(slices):
            plan.append(Row(day=d.isoformat(), slice=s, n=max(0, min(size, n - s * size))))
    rdd = spark.sparkContext.parallelize(plan, numSlices=len(chunk_dates))
    base = (spark.createDataFrame(rdd, "day STRING, slice INT, n LONG")
            .where("n > 0")
            .withColumn("i", F.explode(F.sequence(F.lit(0).cast("long"), F.col("n") - 1)))
            .withColumn("order_date", F.to_date("day")))

    s = seed
    base = base.select(
        "order_date", "slice", "i",
        F.floor(F.pow(F.rand(s), 1.7) * n_rest).cast("long").alias("r_idx"),   # power-law popularity
        F.rand(s + 1).alias("u_cust"), F.rand(s + 2).alias("u_rider"), F.rand(s + 21).alias("u_seg"),
        F.rand(s + 24).alias("u_keep"), F.rand(s + 25).alias("u_hour"), F.rand(s + 26).alias("u_wrap"),
        pick(HOUR_BAG, s + 4).alias("hour_ist"),
        F.floor(F.rand(s + 5) * 3600).cast("int").alias("sec_in_hour"),
        pick([1, 1, 1, 1, 2, 2, 2, 3, 3, 4, 5], s + 6).alias("items_count"),
        F.rand(s + 7).alias("u_price"), F.rand(s + 8).alias("u_coupon"),
        pick([0.0, 19.0, 29.0, 39.0, 49.0], s + 9).alias("fee_draw"),
        pick(["UPI"] * 55 + ["Card"] * 18 + ["Cash on Delivery"] * 12 + ["Wallet"] * 10 + ["Net Banking"] * 5,
             s + 10).alias("payment_method"),
        pick(["android"] * 68 + ["ios"] * 24 + ["web"] * 8, s + 11).alias("platform"),
        pick(["ZOMATO50"] * 30 + ["SUPERSAVER"] * 25 + ["TRYNEW"] * 20 + ["GOLD20"] * 15 + ["WELCOME60"] * 10, s + 12).alias("coupon_draw"),
        F.randn(s + 22).alias("z_rating"),
        F.rand(s + 13).alias("u_status"), F.randn(s + 14).alias("z_delay"),
        F.rand(s + 15).alias("u_review"), F.rand(s + 16).alias("u_rating"), F.rand(s + 17).alias("u_dist"),
        F.floor(F.rand(s + 18) * 16).cast("int").alias("promise_extra"),
        pick(["Item out of stock"] * 26 + ["Delivery taking too long"] * 22 + ["Changed mind"] * 18 + ["Restaurant closed"] * 10
             + ["Wrong address"] * 10 + ["Ordered by mistake"] * 8 + ["Kitchen overloaded"] * 6, s + 20).alias("cancel_draw"),
        pick(["Delivery taking too long"] * 40 + ["Kitchen overloaded"] * 20 + ["Item out of stock"] * 20 + ["Changed mind"] * 12
             + ["Wrong address"] * 8, s + 23).alias("cancel_draw_peak"),
    )

    # every join is a broadcast (dimensions are small) => the whole plan stays shuffle-free
    df = (base.join(F.broadcast(restaurants), "r_idx")
          # city personality: growth (older days thinned per city) and meal timing
          .where(F.col("u_keep") >= element(CITY_GROWTH, "city", 0.3)
                 * F.datediff(F.lit(anchor), F.col("order_date")) / F.lit(float(days)))
          .withColumn("hour_ist", F.when(F.col("u_hour") < 0.45, (F.col("hour_ist") + element(CITY_HOUR_SHIFT, "city", 0) + 24) % 24)
                      .otherwise(F.col("hour_ist")))
          .join(F.broadcast(counts), "city")
          .withColumn("c_idx", customer_index(days, anchor))
          # only part of the fleet is on shift each day (sized to ~14 deliveries each), rotating daily
          .withColumn("day_orders", F.element_at(day_orders, F.col("order_date").cast("string")))
          .withColumn("shift_size", F.greatest(F.lit(20), F.least(F.col("n_riders"),
                      F.ceil(F.col("day_orders") * F.col("rider_share") / DELIVERIES_PER_SHIFT))))
          .withColumn("d_idx", ((F.floor(F.col("u_rider") * F.col("shift_size"))
                                 + F.dayofyear("order_date") * 7919) % F.col("n_riders")).cast("int"))
          .join(F.broadcast(customers.withColumnRenamed("idx", "c_idx")), ["city", "c_idx"], "left")
          # customers beyond the named ones get a generated ID; ~18% of them are Gold members
          .withColumn("customer_id", F.coalesce("customer_id", F.concat(
              F.lit("V"), F.upper(F.substring(F.regexp_replace("city", "[^A-Za-z]", ""), 1, 3)),
              F.lpad(F.col("c_idx").cast("string"), 8, "0"))))
          .withColumn("is_gold", F.when(F.col("c_idx") < F.floor(F.col("n_virt") * CORE_SHARE),
                                        (F.abs(F.hash("customer_id")) % 100) < 40 + element(CITY_GOLD_PTS, "city", 0))
                      .otherwise((F.abs(F.hash("customer_id")) % 100) < 10 + element(CITY_GOLD_PTS, "city", 0)))
          .join(F.broadcast(riders.withColumnRenamed("idx", "d_idx")), ["city", "d_idx"]))

    local_ts = F.expr("to_timestamp(order_date) + make_interval(0, 0, 0, 0, hour_ist, 0, sec_in_hour)")
    month = F.month("order_date")
    peak = F.col("hour_ist").isin(12, 13, 19, 20, 21)
    rain_key = F.concat(F.col("city"), F.lit("|"), month.cast("string"))
    monsoon = F.coalesce(F.element_at(F.create_map(*[x for c, ms in RAIN_MONTHS.items() for m in ms
                                                     for x in (F.lit(f"{c}|{m}"), F.lit(True))]), rain_key), F.lit(False))
    delivered = F.col("final_status") == "DELIVERED"
    subtotal = F.round(F.col("avg_cost_for_two") / 2 * F.pow(F.col("items_count"), 0.8)
                       * (0.7 + F.col("u_price") * 0.6), 0)
    df = (df.withColumn("subtotal", subtotal)
          # Gold members use coupons more; GOLD20 is members-only; each code has its own % and cap
          .withColumn("coupon_code", F.when(F.col("u_coupon") < F.when(F.col("is_gold"), 0.36).otherwise(0.25),
                                            F.when((F.col("coupon_draw") == "GOLD20") & ~F.col("is_gold"), F.lit("SUPERSAVER"))
                                             .otherwise(F.col("coupon_draw"))))
          .withColumn("discount", F.round(
              F.when(F.col("coupon_code") == "WELCOME60", F.least(F.col("subtotal") * 0.60, F.lit(120.0)))
               .when(F.col("coupon_code") == "ZOMATO50", F.least(F.col("subtotal") * 0.50, F.lit(100.0)))
               .when(F.col("coupon_code") == "TRYNEW", F.least(F.col("subtotal") * 0.40, F.lit(80.0)))
               .when(F.col("coupon_code") == "SUPERSAVER", F.least(F.col("subtotal") * 0.30, F.lit(60.0)))
               .when(F.col("coupon_code") == "GOLD20", F.least(F.col("subtotal") * 0.20, F.lit(75.0)))
               .otherwise(0.0), 2))
          .withColumn("delivery_fee", F.when(F.col("is_gold") & (F.col("subtotal") >= 199), 0.0)
                      .otherwise(F.col("fee_draw")))
          .withColumn("platform_fee", F.lit(10.0))
          .withColumn("taxes", F.round(F.col("subtotal") * 0.05, 2))
          .withColumn("total_amount", F.round(F.col("subtotal") + F.col("delivery_fee") + F.col("platform_fee")
                                              + F.col("taxes") - F.col("discount"), 2))
          .withColumn("promised_minutes", (F.col("prep_time_min") + 15 + F.col("promise_extra")).cast("int"))
          # each restaurant has its own reliability: cancellation odds from 0.6x to 2x the base
          .withColumn("rest_cancel_x", F.lit(0.6) + (F.abs(F.hash("restaurant_id")) % 100) / F.lit(70.0))
          .withColumn("final_status", F.when(F.col("u_status") < (F.when(peak, 0.035).otherwise(0.025)
                                             + F.when(monsoon, 0.01).otherwise(0.0)) * F.col("rest_cancel_x"), "CANCELLED")
                      .otherwise("DELIVERED"))
          .withColumn("cancel_reason", F.when(~delivered, F.when(peak, F.col("cancel_draw_peak")).otherwise(F.col("cancel_draw"))))
          # most orders beat the promise; peaks, traffic and monsoon push some past it (~20% late overall)
          .withColumn("delivery_minutes", F.when(delivered, F.round(F.greatest(
              F.lit(12.0), F.col("promised_minutes") - 9 + F.col("z_delay") * 6
              + F.when(peak, 3.0).otherwise(0.0) + F.when(monsoon, 4.0).otherwise(0.0)
              + element(CITY_TRAFFIC_MIN, "city", 0) + element(VEHICLE_MIN, "vehicle_type", 0.0)), 1)))
          .withColumn("distance_km", F.when(delivered, F.round((F.lit(0.8) + F.col("u_dist") * 6.5)
                                                               * F.when(F.col("vehicle_type") == "Bicycle", 0.55).otherwise(1.0), 2)))
          # ratings follow the restaurant's quality, minus a penalty for late food; angry customers review more
          .withColumn("late_by", F.col("delivery_minutes") - F.col("promised_minutes"))
          .withColumn("rating", F.when(delivered & (F.col("u_review") < F.when(F.col("late_by") > 10, 0.5).otherwise(0.3)),
                                       F.least(F.lit(5), F.greatest(F.lit(1), F.round(
                                           F.col("listed_rating") + 0.25 + F.col("z_rating") * 0.7
                                           - F.when(F.col("late_by") > 10, 1.8).when(F.col("late_by") > 0, 0.7).otherwise(0.0)))))
                      .cast("int"))
          .withColumn("rider_id", F.when(delivered, F.col("rider_id")))
          .withColumn("order_ts", F.to_utc_timestamp(local_ts, "Asia/Kolkata"))
          .withColumn("order_hour_ist", F.col("hour_ist"))
          .withColumn("order_id", F.format_string("H%s%02d%07d", F.date_format("order_date", "yyyyMMdd"),
                                                  F.col("slice"), F.col("i")))
          .withColumn("source", F.lit("historical")))
    return df.select(*FACT_COLUMNS)


def write(df, path, partition_col, fresh):
    w = df.write.format(FMT).partitionBy(partition_col).option("maxRecordsPerFile", MAX_ROWS_PER_FILE).mode("overwrite")
    w = w.option("overwriteSchema", "true") if fresh else w.option("partitionOverwriteMode", "dynamic")
    w.save(path)


def main(n_orders, days, seed, months_per_chunk, resume):
    spark = get_spark("zomato-historical-backfill")
    spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
    hist_path, log_path = lake("silver", "orders_historical"), lake("silver", "_backfill_log")

    # resume: continue an unfinished run with the same parameters (same anchor date => same days)
    anchor, done = datetime.now(IST).date(), set()
    run_log = read_lake(spark, "silver", "_backfill_log")
    if resume and run_log is not None:
        prev = (run_log.where((F.col("orders") == n_orders) & (F.col("days") == days) & (F.col("seed") == seed))
                .orderBy(F.desc("finished_at")).limit(1).collect())
        if prev:
            anchor = date.fromisoformat(prev[0]["anchor"])
            done = {r["chunk_start"] for r in run_log.where(F.col("anchor") == prev[0]["anchor"]).collect()}
            log.info(f"Resuming run anchored at {anchor}: {len(done)} chunks already complete")

    weights = day_weights(days, anchor)
    total_w = sum(weights.values())
    g_avg = sum(CITY_GROWTH.values()) / len(CITY_GROWTH)
    per_day = {d: int(round(n_orders * w / total_w)) for d, w in weights.items()}
    # cities grow at different rates: older rows are thinned per city, so plan a few more of them
    plan_day = {d: int(round(n / (1 - g_avg * (anchor - d).days / days))) for d, n in per_day.items()}
    chunks = list(month_chunks(list(per_day), months_per_chunk))
    rider_since = anchor - timedelta(days=31)   # rider-level detail is only served for recent days
    dims = load_dimensions(spark, n_orders, days)
    log.info(f"Plan: {sum(per_day.values()):,} orders over {days} days in {len(chunks)} chunks, "
             f"{max(per_day.values()):,} orders on the busiest day")

    fresh = not done
    for n, chunk in enumerate(chunks, 1):
        key = chunk[0].isoformat()
        if key in done:
            continue
        rows = sum(per_day[d] for d in chunk)
        started = datetime.now()
        write(build_chunk(spark, chunk, plan_day, dims, seed + n * 1000, anchor, days), hist_path, "order_date", fresh)
        gen_secs = (datetime.now() - started).total_seconds()

        # aggregates from what was just written (partition-pruned + column-pruned re-read)
        written = spark.read.format(FMT).load(hist_path).where(F.col("order_date").between(chunk[0], chunk[-1]))
        for name, agg in daily_aggregates(written, rider_since=rider_since, customers="month").items():
            if "order_month" not in agg.columns:
                agg = agg.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
            agg = agg.coalesce(1)
            write(agg, lake("gold", f"hist_agg_{name}"), "order_month", fresh)

        secs = (datetime.now() - started).total_seconds()
        (spark.createDataFrame([(n_orders, days, seed, anchor.isoformat(), key, rows, datetime.now())],
                               "orders LONG, days INT, seed INT, anchor STRING, chunk_start STRING, rows LONG, "
                               "finished_at TIMESTAMP")
         .write.format(FMT).mode("overwrite" if fresh else "append").save(log_path))
        fresh = False
        log.info(f"chunk {n}/{len(chunks)} {chunk[0]}..{chunk[-1]}: {rows:,} orders in {secs:.0f}s "
                 f"({rows / max(secs, 1):,.0f} orders/s; generate {gen_secs:.0f}s, aggregates {secs - gen_secs:.0f}s)")

    log.info(f"Backfill complete: {sum(per_day.values()):,} historical orders in {hist_path}")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=5_000_000)
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--months-per-chunk", type=int, default=1)
    ap.add_argument("--no-resume", dest="resume", action="store_false")
    a = ap.parse_args()
    main(a.orders, a.days, a.seed, a.months_per_chunk, a.resume)
