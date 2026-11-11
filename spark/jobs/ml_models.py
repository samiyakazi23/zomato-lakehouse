"""Machine learning on the lakehouse (Spark MLlib). Run by the Airflow DAG `zomato_ml_training`.

Model 1 - late-delivery prediction (supervised, binary classification)
    Predicts, at the moment an order is placed, whether it will arrive after the promised time.
    Trained on delivered orders from silver/orders_historical using only order-time features
    (city, cuisine, hour, weekday, month, rainy season, restaurant prep time, basket, Gold, ...).
    Three algorithms are trained on the same split and compared: Logistic Regression (baseline),
    Random Forest and Gradient-Boosted Trees. The best by ROC-AUC is saved and used by the
    streaming job to score every live order (realtime.order_risk).

Model 2 - customer clustering (unsupervised, K-means)
    Groups customers by behaviour (recency, tenure, frequency, spend, order value, Gold) built from the
    additive gold aggregate gold/hist_agg_customer_month, so it scales with customers, not orders.
    k is chosen with the silhouette score; clusters are named from their centroids and compared with
    the rule-based RFM segments.

Outputs: Spark models in <lake>/models/, result tables in Postgres schema `ml`.
"""
import argparse
import logging
import math
import time
from datetime import datetime, timezone

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, ClusteringEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from common import get_spark, lake, pg_connect, read_lake, read_pg, write_pg
from schemas import RAIN_MONTHS

log = logging.getLogger("ml")

LATE_MODEL_PATH = lake("models", "late_delivery")
KMEANS_MODEL_PATH = lake("models", "customer_kmeans")

CAT_FEATURES = ["city", "cuisine", "platform", "payment_method"]
NUM_FEATURES = ["order_hour_ist", "day_of_week", "month", "is_weekend", "is_peak_hour", "is_rain_season",
                "is_gold_i", "has_coupon", "items_count", "subtotal", "promised_minutes",
                "prep_time_min", "restaurant_rating"]
FEATURE_LABELS = {
    "city": "City", "cuisine": "Cuisine", "platform": "App platform", "payment_method": "Payment method",
    "order_hour_ist": "Hour of day", "day_of_week": "Day of week", "month": "Month", "is_weekend": "Weekend",
    "is_peak_hour": "Peak hour (lunch/dinner)", "is_rain_season": "Rainy season in city", "is_gold_i": "Gold member",
    "has_coupon": "Coupon used", "items_count": "Items in basket", "subtotal": "Basket value",
    "promised_minutes": "Promised delivery time", "prep_time_min": "Restaurant prep time",
    "restaurant_rating": "Restaurant rating",
}
PEAK_HOURS = [12, 13, 19, 20, 21]
CLUSTER_FEATURES = ["recency_days", "tenure_days", "log_orders", "log_spend", "avg_order_value", "is_gold_i"]


# ----------------------------------------------------------------------------- shared feature logic
def rain_season(city_col, month_col):
    """True when the order falls in the city's rainy season (same table the generator uses)."""
    m = F.create_map(*[x for c, ms in RAIN_MONTHS.items() for mo in ms for x in (F.lit(f"{c}|{mo}"), F.lit(1))])
    return F.coalesce(F.element_at(m, F.concat(city_col, F.lit("|"), month_col.cast("string"))), F.lit(0))


def order_features(orders, restaurants):
    """Order-time features. `orders` needs: city, cuisine, platform, payment_method, order_hour_ist,
    order_date (IST), is_gold, coupon_code, items_count, subtotal, promised_minutes, restaurant_id.
    Used for training here and for live scoring in streaming_pipeline.py, so both see identical features."""
    r = restaurants.select("restaurant_id", F.col("prep_time_min").cast("double").alias("prep_time_min"),
                           F.col("rating").cast("double").alias("restaurant_rating"))
    dow = F.dayofweek("order_date")                      # 1 = Sunday ... 7 = Saturday
    return (orders.join(F.broadcast(r), "restaurant_id", "left")
            .withColumn("day_of_week", dow.cast("double"))
            .withColumn("month", F.month("order_date").cast("double"))
            .withColumn("is_weekend", dow.isin(1, 7).cast("double"))
            .withColumn("is_peak_hour", F.col("order_hour_ist").isin(*PEAK_HOURS).cast("double"))
            .withColumn("is_rain_season", rain_season(F.col("city"), F.month("order_date")).cast("double"))
            .withColumn("is_gold_i", F.coalesce(F.col("is_gold").cast("double"), F.lit(0.0)))
            .withColumn("has_coupon", F.col("coupon_code").isNotNull().cast("double"))
            .withColumn("order_hour_ist", F.col("order_hour_ist").cast("double"))
            .withColumn("items_count", F.col("items_count").cast("double"))
            .withColumn("promised_minutes", F.col("promised_minutes").cast("double"))
            .fillna({"prep_time_min": 15.0, "restaurant_rating": 4.0, "platform": "android", "payment_method": "UPI"}))


def indexers():
    return [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CAT_FEATURES]


def tree_assembler():
    return VectorAssembler(inputCols=[f"{c}_idx" for c in CAT_FEATURES] + NUM_FEATURES, outputCol="features",
                           handleInvalid="keep")


# ----------------------------------------------------------------------------- model 1: late delivery
def threshold_stats(scored):
    """Precision / recall / F1 for every threshold in 0.01 steps, and ROC points, from one small aggregate."""
    bins = (scored.withColumn("b", F.least(F.floor(F.col("p_late") * 100), F.lit(99)).cast("int"))
            .groupBy("b").agg(F.sum("label").alias("pos"), F.sum(1 - F.col("label")).alias("neg")).collect())
    pos = {r["b"]: float(r["pos"]) for r in bins}
    neg = {r["b"]: float(r["neg"]) for r in bins}
    P, N = sum(pos.values()), sum(neg.values())
    rows, roc = [], [(0.0, 0.0)]
    tp = fp = 0.0
    for b in range(99, -1, -1):                     # threshold t = b/100, predict late if p >= t
        tp += pos.get(b, 0.0); fp += neg.get(b, 0.0)
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / P if P else 0.0
        f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        acc = (tp + (N - fp)) / (P + N) if P + N else 0.0
        rows.append({"threshold": b / 100, "tp": tp, "fp": fp, "fn": P - tp, "tn": N - fp,
                     "precision": prec, "recall": rec, "f1": f1, "accuracy": acc})
        roc.append((fp / N if N else 0.0, rec))
    best = max(rows, key=lambda r: r["f1"])
    return best, roc


def train_late_model(spark, sample_rows, seed):
    hist = read_lake(spark, "silver", "orders_historical")
    if hist is None:
        log.warning("silver/orders_historical not found - run the backfill first; skipping the late-delivery model")
        return None
    delivered = hist.where((F.col("final_status") == "DELIVERED") & F.col("delivery_minutes").isNotNull())
    total = delivered.count()
    frac = min(1.0, sample_rows / max(total, 1))
    data = (order_features(delivered.sample(False, frac, seed) if frac < 1 else delivered, read_pg(spark, "dims.restaurants"))
            .withColumn("label", (F.col("delivery_minutes") > F.col("promised_minutes")).cast("double"))
            .select(*CAT_FEATURES, *NUM_FEATURES, "label")
            .repartition(8).cache())
    train, test = data.randomSplit([0.8, 0.2], seed=seed)
    train, test = train.cache(), test.cache()
    n_train, n_test = train.count(), test.count()
    late_rate = test.agg(F.avg("label")).first()[0]
    log.info("late-delivery data: %d delivered orders, sampled %d (train %d / test %d), late rate %.3f",
             total, n_train + n_test, n_train, n_test, late_rate)

    lr = Pipeline(stages=indexers() + [
        OneHotEncoder(inputCols=[f"{c}_idx" for c in CAT_FEATURES], outputCols=[f"{c}_oh" for c in CAT_FEATURES],
                      handleInvalid="keep"),
        VectorAssembler(inputCols=[f"{c}_oh" for c in CAT_FEATURES] + NUM_FEATURES, outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=False),
        LogisticRegression(featuresCol="features", labelCol="label", maxIter=50, regParam=0.001)])
    rf = Pipeline(stages=indexers() + [tree_assembler(),
        RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=60, maxDepth=8, maxBins=64,
                               subsamplingRate=0.7, seed=seed)])
    gbt = Pipeline(stages=indexers() + [tree_assembler(),
        GBTClassifier(featuresCol="features", labelCol="label", maxIter=60, maxDepth=5, stepSize=0.1, maxBins=64,
                      subsamplingRate=0.8, seed=seed)])

    auc_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    pr_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR")
    results, models, rocs = [], {}, {}
    for name, pipe in [("Logistic Regression", lr), ("Random Forest", rf), ("Gradient-Boosted Trees", gbt)]:
        t0 = time.time()
        model = pipe.fit(train)
        secs = time.time() - t0
        scored = model.transform(test).withColumn("p_late", vector_to_array("probability")[1]).cache()
        auc, pr = auc_eval.evaluate(scored), pr_eval.evaluate(scored)
        best, roc = threshold_stats(scored)
        scored.unpersist()
        log.info("%-24s AUC %.4f  PR-AUC %.4f  F1 %.4f @ %.2f  (%.0f s)", name, auc, pr, best["f1"], best["threshold"], secs)
        results.append({"model": name, "auc": auc, "pr_auc": pr, "threshold": best["threshold"],
                        "accuracy": best["accuracy"], "precision": best["precision"], "recall": best["recall"],
                        "f1": best["f1"], "tp": best["tp"], "fp": best["fp"], "fn": best["fn"], "tn": best["tn"],
                        "train_seconds": secs})
        models[name], rocs[name] = model, roc

    best = max(results, key=lambda r: r["auc"])
    model = models[best["model"]]
    model.write().overwrite().save(LATE_MODEL_PATH)
    log.info("selected %s, saved to %s", best["model"], LATE_MODEL_PATH)

    # feature importance of the best tree model (one slot per input feature, categories kept whole)
    tree_name = max((r for r in results if r["model"] != "Logistic Regression"), key=lambda r: r["auc"])["model"]
    imp = models[tree_name].stages[-1].featureImportances.toArray()
    names = [*CAT_FEATURES, *NUM_FEATURES]
    importance = sorted(({"feature": FEATURE_LABELS[n], "importance": float(v)} for n, v in zip(names, imp)),
                        key=lambda r: -r["importance"])

    # predicted vs actual late rate per city on the test set (does the model see the city differences?)
    by_city = (model.transform(test).withColumn("p_late", vector_to_array("probability")[1])
               .groupBy("city").agg(F.count("*").alias("orders"), F.avg("label").alias("actual_late_rate"),
                                    F.avg("p_late").alias("predicted_late_rate"))
               .orderBy(F.desc("actual_late_rate")))
    trained_at = datetime.now(timezone.utc)
    metrics = spark.createDataFrame([{
        **{k: float(v) for k, v in r.items() if k != "model"}, "model": r["model"],
        "selected": r["model"] == best["model"], "train_rows": n_train, "test_rows": n_test,
        "base_late_rate": float(late_rate), "trained_at": trained_at} for r in results])
    roc_rows = [{"model": m, "point": i, "fpr": float(x), "tpr": float(y)}
                for m, pts in rocs.items() for i, (x, y) in enumerate(pts) if i % 2 == 0 or i == len(pts) - 1]
    write_pg(metrics.select("model", "selected", "auc", "pr_auc", "accuracy", "precision", "recall", "f1", "threshold",
                            "tp", "fp", "fn", "tn", "train_rows", "test_rows", "base_late_rate", "train_seconds",
                            "trained_at"), "ml.late_model_metrics")
    write_pg(spark.createDataFrame(importance), "ml.late_feature_importance")
    write_pg(spark.createDataFrame(roc_rows), "ml.late_roc_curve")
    write_pg(by_city.select("city", "orders", F.round("actual_late_rate", 4).alias("actual_late_rate"),
                            F.round("predicted_late_rate", 4).alias("predicted_late_rate")), "ml.late_by_city")
    train.unpersist(); test.unpersist(); data.unpersist()
    return best


# ----------------------------------------------------------------------------- model 2: customer clusters
def name_clusters(profiles):
    """Readable names from centroids (compared with the customer-weighted average of each feature)."""
    left = {p["cluster"]: p for p in profiles}
    total = sum(p["customers"] for p in profiles)
    avg = {k: sum(p[k] * p["customers"] for p in profiles) / total for k in ("gold_share", "avg_order_value")}
    names = {}

    def take(key, label, reverse=True, cond=lambda p: True):
        cands = [p for p in left.values() if cond(p)]
        if cands:
            p = sorted(cands, key=lambda q: q[key], reverse=reverse)[0]
            names[p["cluster"]] = label(p) if callable(label) else label
            left.pop(p["cluster"])

    take("avg_spend", "High-value regulars")
    take("avg_recency_days", "Lapsed customers")
    take("gold_share", lambda p: "Gold loyalists" if p["avg_recency_days"] <= 45 else "Dormant Gold members",
         cond=lambda p: p["gold_share"] > avg["gold_share"] * 1.5)
    take("avg_order_value", "Big-basket occasionals", cond=lambda p: p["avg_order_value"] > avg["avg_order_value"] * 1.3)
    take("avg_tenure_days", "Recent joiners", reverse=False)
    for i, p in enumerate(sorted(left.values(), key=lambda q: -q["avg_orders"])):
        names[p["cluster"]] = ["Steady regulars", "Occasional diners", "Light users", "Drifting customers"][min(i, 3)]
    return names


def train_clusters(spark, k_min, k_max, seed):
    hist = read_lake(spark, "gold", "hist_agg_customer_month")
    if hist is None:
        log.warning("gold/hist_agg_customer_month not found - skipping customer clustering")
        return None
    as_of = hist.agg(F.max("last_order_date")).first()[0]
    cust = (hist.groupBy("customer_id")
            .agg(F.min("first_order_date").alias("first_order_date"), F.max("last_order_date").alias("last_order_date"),
                 F.sum("orders").alias("orders"), F.sum("amount_sum").alias("spend"),
                 F.max(F.col("is_gold").cast("int")).alias("is_gold_i"))
            .withColumn("recency_days", F.datediff(F.lit(as_of), "last_order_date").cast("double"))
            .withColumn("tenure_days", F.datediff(F.lit(as_of), "first_order_date").cast("double"))
            .withColumn("avg_order_value", F.col("spend") / F.col("orders"))
            .withColumn("log_orders", F.log1p("orders"))
            .withColumn("log_spend", F.log1p("spend"))
            .withColumn("is_gold_i", F.col("is_gold_i").cast("double"))
            .repartition(8).cache())
    n = cust.count()
    # rule-based RFM segment (same rules as gold_aggregates.py) for comparison
    f50, f75 = cust.approxQuantile("orders", [0.5, 0.75], 0.01)
    cust = cust.withColumn("rfm_segment",
                           F.when(F.col("tenure_days") <= 30, "New")
                           .when((F.col("recency_days") <= 7) & (F.col("orders") >= f75), "Champions")
                           .when((F.col("recency_days") <= 30) & (F.col("orders") >= f50), "Loyal")
                           .when((F.col("recency_days") <= 90) & (F.col("orders") >= f50), "At risk")
                           .when(F.col("recency_days") > 90, "Hibernating").otherwise("Occasional"))

    prep = Pipeline(stages=[VectorAssembler(inputCols=CLUSTER_FEATURES, outputCol="raw"),
                            StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True)]).fit(cust)
    feats = prep.transform(cust).cache()
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette",
                                  distanceMeasure="squaredEuclidean")
    k_rows = []
    sample = feats.sample(False, min(1.0, 60000 / max(n, 1)), seed).cache()   # silhouette is O(n) per k on a sample
    for k in range(k_min, k_max + 1):
        km = KMeans(k=k, seed=seed, maxIter=40, featuresCol="features", predictionCol="cluster").fit(feats)
        sil = evaluator.evaluate(km.transform(sample))
        k_rows.append({"k": k, "silhouette": float(sil), "wssse": float(km.summary.trainingCost)})
        log.info("k=%d silhouette %.4f  WSSSE %.0f", k, sil, km.summary.trainingCost)
    best_k = max(k_rows, key=lambda r: r["silhouette"])["k"]
    km = KMeans(k=best_k, seed=seed, maxIter=60, featuresCol="features", predictionCol="cluster").fit(feats)
    PipelineModel(stages=[*prep.stages, km]).write().overwrite().save(KMEANS_MODEL_PATH)
    labelled = km.transform(feats).cache()

    total_spend = labelled.agg(F.sum("spend")).first()[0]
    prof = (labelled.groupBy("cluster")
            .agg(F.count("*").alias("customers"), F.avg("recency_days").alias("avg_recency_days"),
                 F.avg("tenure_days").alias("avg_tenure_days"), F.avg("orders").alias("avg_orders"),
                 F.avg("spend").alias("avg_spend"), F.avg("avg_order_value").alias("avg_order_value"),
                 F.avg("is_gold_i").alias("gold_share"), F.sum("spend").alias("spend")).collect())
    profiles = [r.asDict() for r in prof]
    names = name_clusters(profiles)
    rows = [{"cluster": int(p["cluster"]), "cluster_name": names[p["cluster"]], "customers": int(p["customers"]),
             "customer_share": p["customers"] / n, "revenue_share": float(p["spend"] / total_spend),
             "avg_recency_days": float(p["avg_recency_days"]), "avg_tenure_days": float(p["avg_tenure_days"]),
             "avg_orders": float(p["avg_orders"]), "avg_spend": float(p["avg_spend"]),
             "avg_order_value": float(p["avg_order_value"]), "gold_share": float(p["gold_share"])} for p in profiles]
    rows.sort(key=lambda r: -r["avg_spend"])
    name_map = F.create_map(*[x for c, nm in names.items() for x in (F.lit(int(c)), F.lit(nm))])
    cross = (labelled.withColumn("cluster_name", F.element_at(name_map, F.col("cluster")))
             .groupBy("cluster_name", "rfm_segment").agg(F.count("*").alias("customers")))
    k_df = spark.createDataFrame([{**r, "selected": r["k"] == best_k} for r in k_rows])
    write_pg(spark.createDataFrame(rows).withColumn("trained_at", F.lit(datetime.now(timezone.utc))), "ml.customer_clusters")
    write_pg(k_df.select("k", "silhouette", "wssse", "selected"), "ml.cluster_k_selection")
    write_pg(cross, "ml.cluster_vs_rfm")
    log.info("K-means: %d customers, k=%d, clusters %s", n, best_k, {r["cluster_name"]: r["customers"] for r in rows})
    for df in (labelled, feats, sample, cust):
        df.unpersist()
    return {"customers": n, "k": best_k}


def ensure_schema():
    try:
        conn = pg_connect()
        with conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS ml")
            cur.execute("GRANT USAGE ON SCHEMA ml TO grafana")
            cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA ml GRANT SELECT ON TABLES TO grafana")
        conn.close()
    except Exception as exc:   # tables can still be written; grants are re-applied below
        log.warning("could not prepare schema ml: %s", exc)


def grant_read():
    try:
        conn = pg_connect()
        with conn, conn.cursor() as cur:
            cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA ml TO grafana")
        conn.close()
    except Exception as exc:
        log.warning("could not grant read access on schema ml: %s", exc)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample-rows", type=int, default=1_000_000, help="delivered orders used to train model 1")
    ap.add_argument("--k-min", type=int, default=3)
    ap.add_argument("--k-max", type=int, default=8)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--only", choices=["late", "clusters"], default=None)
    a = ap.parse_args()
    spark = get_spark("zomato-ml")
    ensure_schema()
    t0 = time.time()
    if a.only in (None, "late"):
        train_late_model(spark, a.sample_rows, a.seed)
    if a.only in (None, "clusters"):
        train_clusters(spark, a.k_min, a.k_max, a.seed)
    grant_read()
    log.info("ML training finished in %.0f s", time.time() - t0)


if __name__ == "__main__":
    main()
