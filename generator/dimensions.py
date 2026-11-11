"""Deterministic reference data (restaurants, customers, riders).

The same seed always produces the same entities, so the streaming generator, the Spark
historical backfill (which reads these from Postgres) and dbt all agree on IDs.
"""
import csv
import hashlib
import io
import logging
import random
from datetime import date, timedelta

from reference_data import (CITIES, CITY_CUISINE_BIAS, CITY_PRICE, CUISINES, FIRST_NAMES, LAST_NAMES, NAME_PREFIXES, NAME_SUFFIXES,
                            VEG_CUISINES, VEHICLES)

log = logging.getLogger("dimensions")

CITY_NAMES = list(CITIES)
CITY_WEIGHTS = [CITIES[c]["weight"] for c in CITY_NAMES]
CUISINE_NAMES = list(CUISINES)
CUISINE_WEIGHTS = [CUISINES[c][0] for c in CUISINE_NAMES]

RESTAURANT_COLS = ["restaurant_id", "restaurant_name", "city", "locality", "cuisine", "rating",
                   "avg_cost_for_two", "is_pure_veg", "prep_time_min", "lat", "lon", "onboarded_on"]
CUSTOMER_COLS = ["customer_id", "customer_name", "city", "locality", "signup_date", "is_gold"]
RIDER_COLS = ["rider_id", "rider_name", "city", "vehicle_type", "joined_on"]


def _jitter(rng, lat, lon, spread=0.08):
    return round(lat + rng.uniform(-spread, spread), 6), round(lon + rng.uniform(-spread, spread), 6)


def _person(rng):
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def build_dimensions(n_restaurants, n_customers, n_riders, seed=42):
    rng = random.Random(seed)
    today = date(2026, 1, 1)  # fixed anchor keeps output deterministic

    restaurants = []
    for i in range(1, n_restaurants + 1):
        city = rng.choices(CITY_NAMES, CITY_WEIGHTS)[0]
        bias = CITY_CUISINE_BIAS.get(city, {})
        cuisine = rng.choices(CUISINE_NAMES, [w * bias.get(c, 1.0) for c, w in zip(CUISINE_NAMES, CUISINE_WEIGHTS)])[0]
        lat, lon = _jitter(rng, CITIES[city]["lat"], CITIES[city]["lon"])
        cost_base = CITY_PRICE.get(city, 1.0) * {"Healthy": 650, "Cafe": 700, "Mughlai": 800, "Pizza": 600, "Street Food": 250,
                     "South Indian": 300, "Desserts": 350}.get(cuisine, 500)
        restaurants.append({
            "restaurant_id": f"R{i:06d}",
            "restaurant_name": f"{rng.choice(NAME_PREFIXES)} {rng.choice(NAME_SUFFIXES[cuisine])}",
            "city": city,
            "locality": rng.choice(CITIES[city]["localities"]),
            "cuisine": cuisine,
            "rating": round(min(4.9, max(2.5, rng.gauss(3.9, 0.35))), 1),
            "avg_cost_for_two": int(max(150, rng.gauss(cost_base, cost_base * 0.3)) // 50 * 50),
            "is_pure_veg": cuisine in VEG_CUISINES or rng.random() < 0.15,
            "prep_time_min": rng.randint(10, 30),
            "lat": lat,
            "lon": lon,
            "onboarded_on": (today - timedelta(days=rng.randint(30, 3000))).isoformat(),
        })

    rng = random.Random(seed + 1)   # independent stream: sizes of one table never shift another
    customers = []
    for i in range(1, n_customers + 1):
        city = rng.choices(CITY_NAMES, CITY_WEIGHTS)[0]
        customers.append({
            "customer_id": f"C{i:07d}",
            "customer_name": _person(rng),
            "city": city,
            "locality": rng.choice(CITIES[city]["localities"]),
            "signup_date": (today - timedelta(days=rng.randint(1, 2200))).isoformat(),
            "is_gold": rng.random() < 0.18,
        })

    rng = random.Random(seed + 2)
    riders = []
    for i in range(1, n_riders + 1):
        city = rng.choices(CITY_NAMES, CITY_WEIGHTS)[0]
        riders.append({
            "rider_id": f"D{i:06d}",
            "rider_name": _person(rng),
            "city": city,
            "vehicle_type": rng.choices([v for v, _ in VEHICLES], [w for _, w in VEHICLES])[0],
            "joined_on": (today - timedelta(days=rng.randint(1, 1500))).isoformat(),
        })

    return restaurants, customers, riders


def _copy(cur, table, cols, rows):
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in rows:
        writer.writerow([r[c] for c in cols])
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv)", buf)


def fingerprint(restaurants, customers, riders):
    sample = [restaurants[i] for i in range(0, len(restaurants), max(1, len(restaurants) // 500))]
    blob = repr((len(restaurants), len(customers), len(riders), sample, customers[:200], riders[:200]))
    return hashlib.sha1(blob.encode()).hexdigest()


def load_dimensions(conn, restaurants, customers, riders):
    """Idempotently load dimensions into Postgres (skips when the same reference data is already there)."""
    fp = fingerprint(restaurants, customers, riders)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(424242)")  # safe when the generator is scaled out
        try:
            cur.execute("CREATE TABLE IF NOT EXISTS dims.meta (key TEXT PRIMARY KEY, value TEXT)")
            cur.execute("SELECT value FROM dims.meta WHERE key = 'fingerprint'")
            row = cur.fetchone()
            if row and row[0] == fp:
                log.info("Dimensions already loaded - skipping")
                conn.commit()
                return
            log.info("Loading dimensions: %d restaurants, %d customers, %d riders",
                     len(restaurants), len(customers), len(riders))
            cur.execute("TRUNCATE dims.restaurants, dims.customers, dims.riders")
            _copy(cur, "dims.restaurants", RESTAURANT_COLS, restaurants)
            _copy(cur, "dims.customers", CUSTOMER_COLS, customers)
            _copy(cur, "dims.riders", RIDER_COLS, riders)
            cur.execute("INSERT INTO dims.meta VALUES ('fingerprint', %s) "
                        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (fp,))
            conn.commit()
            log.info("Dimensions loaded")
        finally:
            cur.execute("SELECT pg_advisory_unlock(424242)")
            conn.commit()
