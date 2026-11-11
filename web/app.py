"""Zomato Analytics - the project's web dashboard.

Serves one page (index.html) plus a small JSON API over the warehouse:
  GET /api/live       real-time KPIs, 30-minute series, city board, order funnel, pipeline health
  GET /api/feed       the latest orders and reviews, tailed straight from Kafka
  GET /api/history    everything behind the analytics tabs (orders, customers, restaurants, reviews,
                      riders, cities, data quality), at city grain so the page filters instantly
  GET /api/analytics  compact lakehouse summary (kept for Grafana-style embeds)
  GET /api/ml         Spark MLlib results: late-delivery model comparison, ROC curve, feature importance,
                      live order risk, and K-means customer clusters
Run:  uvicorn app:app --host 0.0.0.0 --port 8000
"""
import json
import logging
import os
import threading
import time
from collections import deque
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import psycopg2.extras
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

import ai

log = logging.getLogger("web")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")

PG = dict(host=os.getenv("PG_HOST", "postgres"), port=os.getenv("PG_PORT", "5432"), dbname=os.getenv("PG_DB", "zomato"),
          user=os.getenv("WEB_PG_USER", "grafana"), password=os.getenv("WEB_PG_PASSWORD", "grafana"))
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SURGE_MINUTES = float(os.getenv("SURGE_DELIVERY_MINUTES", "46"))
HERE = Path(__file__).parent

app = FastAPI(title="Zomato Analytics")
FEED = deque(maxlen=60)
REVIEWS = deque(maxlen=40)
CACHE = {}
FEED_STATE = {"connected": False}


@contextmanager
def cursor():
    conn = psycopg2.connect(**PG, connect_timeout=3)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()


def rows(cur, sql):
    try:
        cur.execute(sql)
        return [dict(r) for r in cur.fetchall()]
    except psycopg2.Error as exc:          # table not built yet (e.g. dbt has not run)
        cur.connection.rollback()
        log.debug("query failed: %s", exc)
        return []


def num(v, digits=2):
    return None if v is None else round(float(v), digits)


# ----------------------------------------------------------------------------- live
@app.get("/api/live")
def live():
    with cursor() as cur:
        k = rows(cur, """
            SELECT
              (SELECT COALESCE(SUM(orders),0) FROM realtime.city_orders_1m WHERE window_start >= now() - interval '5 minutes') AS orders_5m,
              (SELECT COALESCE(SUM(gmv),0)    FROM realtime.city_orders_1m WHERE window_start >= now() - interval '5 minutes') AS gmv_5m,
              (SELECT SUM(gmv)/NULLIF(SUM(orders),0) FROM realtime.city_orders_1m WHERE window_start >= now() - interval '15 minutes') AS aov_15m,
              (SELECT SUM(avg_delivery_minutes*events)/NULLIF(SUM(events),0) FROM realtime.order_status_1m
                 WHERE status='DELIVERED' AND window_start >= now() - interval '15 minutes') AS delivery_15m,
              (SELECT SUM(events) FILTER (WHERE status='CANCELLED')::float
                      / NULLIF(SUM(events) FILTER (WHERE status IN ('CANCELLED','DELIVERED')),0)
                 FROM realtime.order_status_1m WHERE window_start >= now() - interval '15 minutes') AS cancel_15m,
              (SELECT COALESCE(SUM(active_riders),0) FROM realtime.rider_activity_1m
                 WHERE window_start = (SELECT max(window_start) - interval '1 minute' FROM realtime.rider_activity_1m)) AS riders
        """)
        k = k[0] if k else {"orders_5m": 0, "gmv_5m": 0, "aov_15m": None, "delivery_15m": None, "cancel_15m": None, "riders": 0}
        series = rows(cur, """
            SELECT window_start AS t, city, orders, gmv FROM realtime.city_orders_1m
            WHERE window_start >= date_trunc('minute', now()) - interval '30 minutes' ORDER BY 1""")
        cities = rows(cur, """
            WITH o AS (SELECT city, SUM(orders) orders, SUM(gmv) gmv FROM realtime.city_orders_1m
                       WHERE window_start >= now() - interval '15 minutes' GROUP BY 1),
                 s AS (SELECT city,
                         SUM(avg_delivery_minutes*events) FILTER (WHERE status='DELIVERED')
                           / NULLIF(SUM(events) FILTER (WHERE status='DELIVERED'),0) AS delivery,
                         SUM(events) FILTER (WHERE status='CANCELLED')::float
                           / NULLIF(SUM(events) FILTER (WHERE status IN ('CANCELLED','DELIVERED')),0) AS cancel_rate
                       FROM realtime.order_status_1m WHERE window_start >= now() - interval '10 minutes' GROUP BY 1)
            SELECT o.city, o.orders, o.gmv, s.delivery, s.cancel_rate FROM o LEFT JOIN s USING (city) ORDER BY o.gmv DESC""")
        funnel = rows(cur, """SELECT status, SUM(events) AS events FROM realtime.order_status_1m
                              WHERE window_start >= now() - interval '15 minutes' GROUP BY 1""")
        cuisines = rows(cur, """SELECT cuisine, SUM(orders) AS orders FROM realtime.cuisine_orders_1m
                                WHERE window_start >= now() - interval '15 minutes' GROUP BY 1 ORDER BY 2 DESC""")
        events = {e["city"]: e["kind"] for e in rows(cur, """
            SELECT DISTINCT ON (city) city, kind FROM realtime.city_events
            WHERE now() BETWEEN started_at AND ends_at ORDER BY city, started_at DESC""")}
        pipeline = rows(cur, """
            SELECT DISTINCT ON (query_name) query_name, input_rps, batch_duration_ms,
                   EXTRACT(EPOCH FROM now() - ts)::int AS age_s
            FROM realtime.stream_progress ORDER BY query_name, ts DESC""")
    return {
        "mode": "live",
        "kpis": {"orders_5m": int(k["orders_5m"] or 0), "gmv_5m": num(k["gmv_5m"]), "aov_15m": num(k["aov_15m"]),
                 "delivery_15m": num(k["delivery_15m"], 1), "cancel_15m": num(k["cancel_15m"], 4),
                 "riders": int(k["riders"] or 0)},
        "series": [{"t": r["t"].isoformat(), "city": r["city"], "orders": int(r["orders"]), "gmv": num(r["gmv"])}
                   for r in series],
        "cities": [{"city": c["city"], "orders": int(c["orders"]), "gmv": num(c["gmv"]),
                    "delivery": num(c["delivery"], 1), "cancel_rate": num(c["cancel_rate"], 4),
                    "event": events.get(c["city"]),
                    "surge": c["city"] in events or (c["delivery"] is not None and float(c["delivery"]) >= SURGE_MINUTES)}
                   for c in cities],
        "funnel": {f["status"]: int(f["events"]) for f in funnel},
        "cuisines": [{"cuisine": c["cuisine"], "orders": int(c["orders"])} for c in cuisines],
        "pipeline": [{"query": p["query_name"], "rps": num(p["input_rps"], 1), "batch_ms": p["batch_duration_ms"],
                      "age_s": p["age_s"]} for p in pipeline],
    }


# ----------------------------------------------------------------------------- analytics
@app.get("/api/analytics")
def analytics():
    with cursor() as cur:
        totals = rows(cur, """SELECT SUM(orders) AS orders, SUM(gmv) AS gmv, COUNT(DISTINCT order_date) AS days,
                                     COUNT(DISTINCT city) AS cities FROM analytics.mart_city_kpis""")
        daily = rows(cur, """SELECT order_date AS d, SUM(orders) AS orders, SUM(gmv) AS gmv
                             FROM analytics.mart_city_kpis GROUP BY 1 ORDER BY 1""")
        heat = rows(cur, "SELECT city, hour_of_day, demand_index FROM analytics.mart_peak_hours")
        top = rows(cur, """SELECT restaurant_name, city, cuisine, orders, gmv, avg_rating, health_segment
                           FROM analytics.mart_restaurant_rankings ORDER BY national_gmv_rank LIMIT 10""")
        cuisines = rows(cur, """SELECT cuisine, SUM(gmv) AS gmv FROM analytics.mart_cuisine_city
                                GROUP BY 1 ORDER BY 2 DESC""")
        quality = rows(cur, "SELECT dq_reason, SUM(rejected_rows) AS n FROM analytics.mart_data_quality GROUP BY 1 ORDER BY 2 DESC")
    t = totals[0] if totals else {}
    return {
        "totals": {"orders": int(t.get("orders") or 0), "gmv": num(t.get("gmv")), "days": int(t.get("days") or 0),
                   "cities": int(t.get("cities") or 0)},
        "daily": [{"d": r["d"].isoformat(), "orders": int(r["orders"]), "gmv": num(r["gmv"])} for r in daily],
        "heatmap": [{"city": h["city"], "hour": int(h["hour_of_day"]), "index": num(h["demand_index"])} for h in heat],
        "top_restaurants": [{**r, "orders": int(r["orders"]), "gmv": num(r["gmv"]), "avg_rating": num(r["avg_rating"])}
                            for r in top],
        "cuisines": [{"cuisine": c["cuisine"], "gmv": num(c["gmv"])} for c in cuisines],
        "quality": [{"reason": q["dq_reason"], "n": int(q["n"])} for q in quality],
    }


# ----------------------------------------------------------------------------- history (all tabs)
def table(cur, sql):
    """Columnar result {cols, rows}: ~3x smaller JSON than a list of objects."""
    try:
        cur.execute(sql)
    except psycopg2.Error as exc:
        cur.connection.rollback()
        log.info("history query skipped (%s)", str(exc).splitlines()[0])
        return {"cols": [], "rows": []}
    cols = [d[0] for d in cur.description]
    def clean(v):
        if hasattr(v, "isoformat"):
            return v.isoformat()
        if v is not None and not isinstance(v, (int, float, str, bool)):
            return float(v)
        return v
    return {"cols": cols, "rows": [[clean(v) for v in r.values()] for r in cur.fetchall()]}


HISTORY_SQL = {
    "refresh": "SELECT refreshed_at, orders_served, data_through, duration_s FROM gold.refresh_log",
    "daily": """SELECT order_date AS d, city, orders, delivered_orders AS delivered, cancelled_orders AS cancelled, gmv,
                       avg_order_value AS aov, cancel_rate, avg_delivery_minutes AS delivery, p90_delivery_minutes AS p90,
                       avg_rating AS rating, ratings_count, unique_customers AS customers, gold_share, total_discount AS discount,
                       review_rate, late_rate, coupon_share, cod_share, avg_items, gold_aov, non_gold_aov
                FROM gold.daily_city_metrics ORDER BY 1, 2""",
    "hourly": "SELECT city, day_of_week AS dow, hour_of_day AS hour, orders, avg_order_value AS aov FROM gold.hourly_demand",
    "basket": "SELECT city, items_bucket AS items, value_band AS band, orders, amount_sum AS amount FROM gold.basket_mix",
    "delivery_dist": "SELECT city, delivery_bucket AS bucket, orders FROM gold.delivery_distribution",
    "ratings": "SELECT city, cuisine, late_bucket AS late, rating, reviews AS n FROM gold.rating_matrix",
    "coupons": "SELECT coupon, is_gold, orders, gmv, discount, cancelled_orders AS cancelled FROM gold.coupon_performance",
    "cancels": "SELECT city, cancel_reason AS reason, orders AS n FROM gold.cancellation_reasons",
    "payments": """SELECT payment_method AS method, platform, SUM(orders) AS orders, SUM(gmv) AS gmv FROM gold.payment_mix
                   WHERE order_date >= (SELECT max(order_date) - 90 FROM gold.payment_mix) GROUP BY 1, 2""",
    "cust_monthly": """SELECT order_month AS month, active_customers AS active, new_customers AS new, orders, gmv,
                              orders_per_customer AS opc FROM gold.customer_monthly ORDER BY 1""",
    "cohorts": "SELECT cohort, month_number AS k, customers, cohort_size AS size, retention FROM gold.customer_cohorts",
    "segments": """SELECT city, segment, customers, orders, gmv, avg_recency_days AS recency, avg_orders AS frequency,
                          avg_spend AS spend FROM gold.customer_segments""",
    "gold_split": "SELECT city, is_gold, customers, orders, gmv FROM gold.customer_gold_split",
    "frequency": "SELECT orders_band AS band, customers FROM gold.customer_frequency",
    "top_customers": """SELECT COALESCE(c.customer_name,
                                        (ARRAY['Aarav','Vivaan','Aditya','Ishaan','Rohan','Ananya','Diya','Saanvi','Kavya','Sneha',
                                               'Rahul','Priya','Amit','Neha','Karan','Pooja','Meera','Farhan','Zara','Arnav'])
                                          [1 + abs(hashtext(t.customer_id)) % 20] || ' ' ||
                                        (ARRAY['Sharma','Verma','Patel','Iyer','Reddy','Nair','Gupta','Singh','Khan','Das',
                                               'Joshi','Mehta','Shah','Rao','Kapoor','Bose','Menon','Desai','Ghosh','Yadav'])
                                          [1 + abs(hashtext(t.customer_id || '#')) % 20]) AS name, t.city, t.is_gold, t.orders, t.gmv, t.last_order_date AS last_order,
                               t.segment, c.signup_date AS member_since
                        FROM gold.top_customers t LEFT JOIN dims.customers c USING (customer_id) ORDER BY t.gmv DESC LIMIT 25""",
    "restaurants": """SELECT restaurant_id AS id, restaurant_name AS name, city, cuisine, locality, orders, gmv, avg_order_value AS aov,
                             avg_rating AS rating, ratings_count AS ratings, cancel_rate, avg_delivery_minutes AS delivery,
                             avg_cost_for_two AS cost_for_two, is_pure_veg AS veg, listed_rating, health_segment AS health,
                             city_gmv_rank AS city_rank, national_gmv_rank AS rank
                      FROM analytics.mart_restaurant_rankings WHERE city_gmv_rank <= 40""",
    "restaurant_profile": """SELECT city,
                                    CASE WHEN avg_cost_for_two < 300 THEN 'Under ₹300' WHEN avg_cost_for_two < 500 THEN '₹300–499'
                                         WHEN avg_cost_for_two < 800 THEN '₹500–799' ELSE '₹800+' END AS band,
                                    is_pure_veg AS veg, health_segment AS health,
                                    COUNT(*) AS restaurants, SUM(orders) AS orders, SUM(gmv) AS gmv,
                                    SUM(avg_rating * ratings_count) / NULLIF(SUM(ratings_count), 0) AS rating,
                                    SUM(avg_delivery_minutes * orders) / NULLIF(SUM(orders), 0) AS delivery
                             FROM analytics.mart_restaurant_rankings GROUP BY 1, 2, 3, 4""",
    "cuisines": "SELECT city, cuisine, restaurants, orders, gmv, avg_rating AS rating FROM analytics.mart_cuisine_city",
    "riders_top": """WITH r AS (SELECT rider_id, city, SUM(deliveries) AS deliveries,
                                       SUM(avg_delivery_minutes * deliveries) / SUM(deliveries) AS avg_minutes,
                                       SUM(distance_km) AS distance_km, AVG(avg_rating) AS rating,
                                       COUNT(*) AS days_active
                                FROM gold.rider_daily GROUP BY 1, 2),
                          ranked AS (SELECT *, rank() OVER (PARTITION BY city ORDER BY deliveries DESC, avg_minutes) AS city_rank FROM r)
                     SELECT d.rider_name AS name, ranked.city, d.vehicle_type AS vehicle, deliveries, avg_minutes, distance_km,
                            rating, days_active, city_rank
                     FROM ranked LEFT JOIN dims.riders d USING (rider_id) WHERE city_rank <= 10""",
    "riders_vehicle": """SELECT r.city, d.vehicle_type AS vehicle, COUNT(DISTINCT r.rider_id) AS riders, SUM(r.deliveries) AS deliveries,
                                SUM(r.avg_delivery_minutes * r.deliveries) / SUM(r.deliveries) AS avg_minutes,
                                SUM(r.distance_km) / NULLIF(SUM(r.deliveries), 0) AS km_per_delivery
                         FROM gold.rider_daily r LEFT JOIN dims.riders d USING (rider_id) GROUP BY 1, 2""",
    "riders_daily": """SELECT order_date AS d, city, COUNT(*) AS active_riders, SUM(deliveries) AS deliveries
                       FROM gold.rider_daily GROUP BY 1, 2 ORDER BY 1""",
    "riders_load": """SELECT city, LEAST(deliveries, 12) AS per_day, COUNT(*) AS rider_days FROM gold.rider_daily GROUP BY 1, 2""",
    "fleet": "SELECT city, COUNT(*) AS riders FROM dims.riders GROUP BY 1",
    "restaurant_changes": """SELECT restaurant_name AS name, city, cuisine, changed_at, previous_cost, new_cost,
                                    previous_rating, new_rating, change_type
                             FROM analytics.mart_restaurant_changes ORDER BY changed_at DESC LIMIT 40""",
    "change_summary": """SELECT city, change_type, COUNT(*) AS changes,
                                AVG(new_cost - previous_cost) FILTER (WHERE change_type <> 'Rating') AS avg_price_change
                         FROM analytics.mart_restaurant_changes GROUP BY 1, 2""",
    "quality": "SELECT quarantine_date AS d, dq_reason AS reason, rejected_rows AS n FROM gold.dq_quarantine_daily ORDER BY 1",
}


@app.get("/api/history")
def history():
    hit = CACHE.get("history")
    if hit and time.time() - hit[0] < 60:
        return hit[1]
    with cursor() as cur:
        data = {k: table(cur, sql) for k, sql in HISTORY_SQL.items()}
    data["mode"] = "live"
    CACHE["history"] = (time.time(), data)
    return data


# ----------------------------------------------------------------------------- Kafka feed
def tail_kafka():
    """Keeps the most recent orders in memory. Its own consumer group, latest offsets only."""
    try:
        from confluent_kafka import Consumer
    except ImportError:
        return
    while True:
        try:
            c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"web-feed-{os.getpid()}",
                          "auto.offset.reset": "latest", "enable.auto.commit": False})
            c.subscribe(["zomato.orders", "zomato.reviews"])
            FEED_STATE["connected"] = True
            while True:
                msg = c.poll(1.0)
                if msg is None or msg.error():
                    continue
                try:
                    o = json.loads(msg.value())
                except ValueError:
                    continue
                if msg.topic() == "zomato.reviews":
                    REVIEWS.appendleft({k: o.get(k) for k in ("review_id", "event_time", "city", "rating", "review_text",
                                                              "delivery_minutes", "restaurant_id")})
                    continue
                if not o.get("order_id") or not o.get("city") or (o.get("total_amount") or 0) <= 0:
                    continue   # the feed shows clean orders; rejects are counted on the data-quality panel
                FEED.appendleft({k: o.get(k) for k in ("order_id", "event_time", "city", "locality", "restaurant_name",
                                                       "cuisine", "items_count", "total_amount", "payment_method",
                                                       "platform", "coupon_code", "is_gold", "items")})
        except Exception as exc:
            FEED_STATE["connected"] = False
            log.warning("Kafka feed unavailable (%s), retrying in 10s", exc)
            time.sleep(10)


# ----------------------------------------------------------------------------- machine learning
def _plain(rs):
    return [{k: (v.isoformat() if hasattr(v, "isoformat") else float(v) if hasattr(v, "is_integer") or type(v).__name__ == "Decimal" else v)
             for k, v in r.items()} for r in rs]


@app.get("/api/ml")
def ml():
    hit = CACHE.get("ml")
    if hit and time.time() - hit[0] < 10:
        return hit[1]
    with cursor() as cur:
        out = {
            "models": _plain(rows(cur, "SELECT * FROM ml.late_model_metrics ORDER BY auc DESC")),
            "importance": _plain(rows(cur, "SELECT feature, importance FROM ml.late_feature_importance ORDER BY importance DESC")),
            "roc": _plain(rows(cur, "SELECT model, point, fpr, tpr FROM ml.late_roc_curve ORDER BY model, point")),
            "by_city": _plain(rows(cur, "SELECT * FROM ml.late_by_city ORDER BY actual_late_rate DESC")),
            "clusters": _plain(rows(cur, "SELECT * FROM ml.customer_clusters ORDER BY avg_spend DESC")),
            "k_selection": _plain(rows(cur, "SELECT * FROM ml.cluster_k_selection ORDER BY k")),
            "cluster_vs_rfm": _plain(rows(cur, "SELECT * FROM ml.cluster_vs_rfm")),
            "risk_recent": _plain(rows(cur, """
                SELECT order_id, event_ts, city, restaurant_name, cuisine, promised_minutes, late_probability, risk_level
                FROM realtime.order_risk WHERE event_ts >= now() - interval '15 minutes'
                ORDER BY late_probability DESC, event_ts DESC LIMIT 12""")),
            "risk_city": _plain(rows(cur, """
                SELECT city, count(*) AS orders, avg(late_probability) AS avg_probability,
                       avg((risk_level = 'High')::int) AS high_share
                FROM realtime.order_risk WHERE event_ts >= now() - interval '15 minutes'
                GROUP BY city ORDER BY avg_probability DESC""")),
        }
    CACHE["ml"] = (time.time(), out)
    return out


@app.get("/api/feed")
def feed():
    return {"connected": FEED_STATE["connected"], "orders": list(FEED)[:25], "reviews": list(REVIEWS)[:20]}


# ----------------------------------------------------------------------------- AI
class Question(BaseModel):
    question: str


ASK_LOCK = threading.Lock()   # one question at a time keeps within the API rate limit


@app.get("/api/ai/status")
def ai_status():
    return ai.status()


@app.post("/api/ask")
def ask(q: Question):
    if not ASK_LOCK.acquire(timeout=60):
        return JSONResponse({"error": "Another question is still being answered. Try again in a moment."}, status_code=429)
    try:
        return ai.ask(PG, q.question)
    except ai.AIError as exc:
        return JSONResponse({"error": str(exc)}, status_code=422)
    finally:
        ASK_LOCK.release()


@app.get("/api/reviews/digest")
def reviews_digest():
    hit = CACHE.get("digest")
    if hit and time.time() - hit[0] < 120:
        return hit[1]
    out = ai.review_digest(list(REVIEWS))
    CACHE["digest"] = (time.time(), out)
    return out


@app.get("/api/health")
def health():
    return {"ok": True}


@app.on_event("startup")
def start_feed():
    if os.getenv("FEED_DISABLED", "false").lower() != "true":
        threading.Thread(target=tail_kafka, daemon=True, name="kafka-feed").start()


@app.get("/demo/{name}")
def demo_file(name: str):
    path = HERE / "demo" / name
    if path.suffix != ".json" or not path.is_file():
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(path)


@app.get("/")
def index():
    return FileResponse(HERE / "index.html", headers={"Cache-Control": "no-store"})


@app.exception_handler(psycopg2.OperationalError)
def db_down(_, exc):
    return JSONResponse({"error": "warehouse unavailable", "detail": str(exc).splitlines()[0]}, status_code=503)
