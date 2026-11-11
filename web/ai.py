"""AI layer for Zomato Analytics: plain-English questions -> SQL -> answer, and a review digest.

Uses the Groq API (OpenAI-compatible, very fast inference) with Llama 3.3 70B by default.
Put your Groq API key in .env as AI_API_KEY (free key from https://console.groq.com/keys).
AI_BASE_URL / AI_MODEL can point to any other OpenAI-compatible service (OpenAI, Ollama, ...).

Safety: generated SQL must be a single SELECT/WITH statement, may only touch the analytics, gold,
dims and ml schemas (plus realtime.order_risk), runs as the read-only `grafana` login with a 5 s timeout, and returns at most
200 rows. The model never sees credentials and nothing it writes can change data.
"""
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from collections import Counter

import psycopg2

log = logging.getLogger("ai")

AI_BASE_URL = os.getenv("AI_BASE_URL", "https://api.groq.com/openai/v1").rstrip("/")
AI_MODEL = os.getenv("AI_MODEL", "llama-3.3-70b-versatile")
AI_API_KEY = os.getenv("AI_API_KEY", "").strip()
KEY_MISSING = (not AI_API_KEY or AI_API_KEY.startswith("paste-")) and "groq.com" in AI_BASE_URL
MAX_ROWS = 200
# Groq sits behind Cloudflare, which rejects Python's default "Python-urllib" user agent (HTTP 403, error 1010)
USER_AGENT = "ZomatoAnalytics/1.0 (+https://github.com; python)"

TABLES = {
    "analytics.mart_city_kpis": "one row per city per day: orders, delivered_orders, cancelled_orders, gmv (net revenue, INR), "
                                "avg_order_value, cancel_rate (0-1), avg_delivery_minutes, p90_delivery_minutes, avg_rating, "
                                "unique_customers, gold_share, gmv_7d, wow_growth_pct",
    "analytics.mart_kpi_history": "incremental history of daily city KPIs incl. late_rate (share of deliveries after the promised time)",
    "analytics.mart_restaurant_rankings": "one row per restaurant, last 90 days: orders, gmv, avg_rating, cancel_rate, "
                                          "avg_delivery_minutes, avg_cost_for_two, is_pure_veg, locality, city_gmv_rank, "
                                          "national_gmv_rank, health_segment ('Star','Steady','At risk')",
    "analytics.mart_cuisine_city": "per city and cuisine: restaurants, orders, gmv, avg_rating, gmv_share_pct, cuisine_rank",
    "analytics.mart_peak_hours": "per city and hour_of_day (0-23, IST): orders, avg_order_value, demand_index (1 = average hour)",
    "analytics.mart_customer_segments": "RFM segments per city: segment ('Champions','Loyal','New','Occasional','At risk','Hibernating'), "
                                        "customers, orders, gmv, avg_recency_days, avg_orders, avg_spend, recommended_action",
    "analytics.mart_customer_retention": "cohort (YYYY-MM of first order), month_number, customers, cohort_size, retention (0-1)",
    "analytics.mart_rating_drivers": "late_bucket ('On time','Up to 10 min late','10+ min late'), cuisine, reviews, avg_rating, low_rating_share",
    "analytics.mart_restaurant_changes": "SCD2 history: restaurant price/rating changes with previous_cost, new_cost, previous_rating, "
                                         "new_rating, change_type, changed_at",
    "analytics.mart_rider_leaderboard": "riders, last 7 days: rider_name, city, vehicle_type, deliveries, avg_delivery_minutes, distance_km",
    "gold.payment_mix": "per order_date, payment_method ('UPI','Card','Cash on Delivery','Wallet','Net Banking'), platform: orders, gmv",
    "gold.coupon_performance": "last 90 days per coupon (or 'No coupon') and is_gold: orders, gmv, discount, cancelled_orders",
    "gold.cancellation_reasons": "last 90 days per city and cancel_reason: orders",
    "gold.customer_monthly": "per order_month (YYYY-MM): active_customers, new_customers, orders, gmv, orders_per_customer",
    "dims.restaurants": "restaurant_id, restaurant_name, city, locality, cuisine, rating (listed), avg_cost_for_two, is_pure_veg",
    "ml.late_model_metrics": "late-delivery classifiers compared: model, selected (bool), auc, pr_auc, accuracy, precision, recall, f1, "
                             "threshold, train_rows, test_rows, base_late_rate, train_seconds, trained_at",
    "ml.late_feature_importance": "feature, importance (0-1): what drives late deliveries in the selected model",
    "ml.late_by_city": "city, orders, actual_late_rate, predicted_late_rate (test set)",
    "ml.customer_clusters": "K-means customer clusters: cluster_name, customers, customer_share, revenue_share, avg_recency_days, "
                            "avg_tenure_days, avg_orders, avg_spend, avg_order_value, gold_share",
    "realtime.order_risk": "live orders scored by the ML model: order_id, event_ts, city, restaurant_name, cuisine, promised_minutes, "
                           "late_probability (0-1), risk_level ('High','Medium','Low')",
}
CITIES = "Mumbai, Delhi NCR, Bengaluru, Hyderabad, Chennai, Kolkata, Pune, Ahmedabad, Jaipur, Lucknow"

SYSTEM = f"""You are a data analyst for an Indian food delivery marketplace. Write ONE PostgreSQL query that answers
the user's question using only these tables:
{chr(10).join(f"- {t}: {d}" for t, d in TABLES.items())}
Cities are: {CITIES}. Money is in INR. Dates are type date; use the latest data with
(SELECT max(order_date) FROM analytics.mart_city_kpis) rather than current_date.
Rules: a single SELECT (CTEs allowed), no data changes, alias columns readably, ORDER BY something meaningful,
LIMIT 50 or less, round numbers with ROUND(x::numeric, 2). Reply with the SQL only inside ```sql ... ```."""

EXAMPLES = [
    ("Which city had the highest revenue last week?",
     "SELECT city, ROUND(SUM(gmv)::numeric, 0) AS revenue_inr FROM analytics.mart_city_kpis\n"
     "WHERE order_date > (SELECT max(order_date) - 7 FROM analytics.mart_city_kpis)\nGROUP BY city ORDER BY revenue_inr DESC LIMIT 10"),
    ("How much do late deliveries hurt ratings?",
     "SELECT late_bucket, ROUND(SUM(avg_rating * reviews)::numeric / SUM(reviews), 2) AS avg_rating, SUM(reviews) AS reviews\n"
     "FROM analytics.mart_rating_drivers GROUP BY late_bucket ORDER BY avg_rating DESC"),
]

FORBIDDEN = re.compile(r"\b(insert|update|delete|merge|drop|alter|create|grant|revoke|truncate|copy|call|do|vacuum|analyze|"
                       r"refresh|lock|listen|notify|set|reset|pg_sleep|pg_read_file|pg_ls_dir|lo_import|dblink)\b", re.I)


class AIError(Exception):
    pass


def chat(messages, temperature=0.0, timeout=90):
    if KEY_MISSING:
        raise AIError("No Groq API key yet. Create a free key at console.groq.com/keys, put it in .env as "
                      "AI_API_KEY, then restart the website container.")
    body = json.dumps({"model": AI_MODEL, "messages": messages, "temperature": temperature}).encode()
    req = urllib.request.Request(f"{AI_BASE_URL}/chat/completions", data=body, method="POST",
                                 headers={"Content-Type": "application/json", "Authorization": f"Bearer {AI_API_KEY}",
                                          "User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)["choices"][0]["message"]["content"]
    except urllib.error.HTTPError as exc:
        detail = exc.read()[:200].decode(errors='ignore')
        if exc.code == 401:
            raise AIError("Groq rejected the API key (401). Check AI_API_KEY in .env.") from exc
        if exc.code == 403:
            raise AIError(f"Groq refused the request (403): {detail}. Check the API key and that your network allows api.groq.com.") from exc
        if exc.code == 429:
            raise AIError("Groq rate limit reached (429). Wait a few seconds and ask again.") from exc
        raise AIError(f"The AI service returned {exc.code}: {detail}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise AIError(f"Can't reach the AI service at {AI_BASE_URL} ({exc}). "
                      "Check your internet connection and AI_BASE_URL in .env.") from exc


def status():
    if KEY_MISSING:
        return {"available": False, "key_missing": True, "base_url": AI_BASE_URL, "model": AI_MODEL, "reason": "no API key"}
    try:
        req = urllib.request.Request(f"{AI_BASE_URL}/models", headers={"Authorization": f"Bearer {AI_API_KEY}",
                                                                      "User-Agent": USER_AGENT, "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as r:
            models = [m.get("id") for m in json.load(r).get("data", [])]
        return {"available": True, "base_url": AI_BASE_URL, "model": AI_MODEL,
                "model_installed": (AI_MODEL in models) if models else None, "models": models[:20]}
    except Exception as exc:
        return {"available": False, "base_url": AI_BASE_URL, "model": AI_MODEL, "reason": str(exc)[:160]}


def extract_sql(text):
    m = re.search(r"```(?:sql)?\s*(.+?)```", text, re.S | re.I)
    sql = (m.group(1) if m else text).strip().rstrip(";").strip()
    return sql


def validate(sql):
    if not re.match(r"^\s*(select|with)\b", sql, re.I):
        raise AIError("The model didn't produce a SELECT query.")
    if ";" in sql:
        raise AIError("Only a single statement is allowed.")
    stripped = re.sub(r"'[^']*'", "''", sql)           # ignore words inside string literals
    if FORBIDDEN.search(stripped):
        raise AIError("The query contained a command that isn't allowed (only read-only SELECTs run).")
    # The read-only login can only read the project's schemas; also refuse system catalogs outright.
    if re.search(r"\b(pg_catalog|information_schema|pg_\w+)\b", stripped, re.I):
        raise AIError("System tables aren't available to questions.")
    return sql


def run_sql(pg, sql):
    conn = psycopg2.connect(**pg, connect_timeout=3, options="-c statement_timeout=5000 -c default_transaction_read_only=on "
                                                                "-c search_path=analytics,gold,dims")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM ({sql}) AS q LIMIT {MAX_ROWS}")
            cols = [d[0] for d in cur.description]
            rows = [[(v.isoformat() if hasattr(v, "isoformat") else float(v) if v is not None and not isinstance(v, (int, float, str, bool)) else v)
                     for v in r] for r in cur.fetchall()]
        return cols, rows
    finally:
        conn.close()


def ask(pg, question):
    question = (question or "").strip()[:400]
    if len(question) < 4:
        raise AIError("Ask a question about orders, customers, restaurants, reviews or cities.")
    started = time.time()
    messages = [{"role": "system", "content": SYSTEM}]
    for q, a in EXAMPLES:
        messages += [{"role": "user", "content": q}, {"role": "assistant", "content": f"```sql\n{a}\n```"}]
    messages.append({"role": "user", "content": question})

    sql, error = None, None
    for attempt in range(2):                      # one retry with the database error
        sql = validate(extract_sql(chat(messages)))
        try:
            cols, rows = run_sql(pg, sql)
            break
        except psycopg2.Error as exc:
            error = str(exc).splitlines()[0]
            log.info("generated SQL failed (%s), attempt %d", error, attempt + 1)
            messages += [{"role": "assistant", "content": f"```sql\n{sql}\n```"},
                         {"role": "user", "content": f"That query failed with: {error}. Fix it and reply with SQL only."}]
    else:
        raise AIError(f"The generated query didn't run: {error}")

    preview = json.dumps({"columns": cols, "rows": rows[:25]}, default=str)
    answer = chat([{"role": "system", "content": "You explain query results to a business user in 2-3 short sentences. "
                                                 "Use the numbers given; INR amounts with ₹ and lakh/crore where natural. "
                                                 "Don't mention SQL or tables."},
                   {"role": "user", "content": f"Question: {question}\nResult: {preview}"}], temperature=0.2)
    return {"question": question, "sql": sql, "cols": cols, "rows": rows, "answer": answer.strip(),
            "model": AI_MODEL, "seconds": round(time.time() - started, 1)}


THEMES = {
    "Late delivery": ("late", "longer than promised", "took long", "delay"),
    "Cold food": ("cold",),
    "Wrong order": ("wrong order", "wrong item", "missing"),
    "Small portions": ("portion", "small", "quantity"),
    "Great taste": ("loved", "best", "tasty", "delicious", "fresh", "absolutely"),
    "On time": ("on time", "quick", "fast"),
    "Packaging": ("packaging", "spilled", "leak"),
}


def themes(reviews):
    counts = Counter()
    for r in reviews:
        text = (r.get("review_text") or "").lower()
        for theme, words in THEMES.items():
            if any(w in text for w in words):
                counts[theme] += 1
    return [{"theme": k, "reviews": v} for k, v in counts.most_common()]


def review_digest(reviews):
    """Keyword themes always; an AI-written summary when a model is reachable."""
    out = {"reviews": len(reviews), "themes": themes(reviews), "summary": None}
    if not reviews:
        return out
    sample = "\n".join(f"- {r.get('rating')}★ ({r.get('city')}, {round(r.get('delivery_minutes') or 0)} min): {r.get('review_text')}"
                       for r in reviews[:40])
    try:
        out["summary"] = chat([{"role": "system", "content": "You summarise customer reviews for a food delivery operations team. "
                                                             "Write 3 short bullet points: what customers like, what they complain about, "
                                                             "and one concrete action. Be specific and use the cities mentioned."},
                               {"role": "user", "content": sample}], temperature=0.3).strip()
        out["model"] = AI_MODEL
    except AIError as exc:
        out["summary_error"] = str(exc)
    return out
