"""Zomato order event generator.

Produces a continuous, realistic event stream to Kafka:
  zomato.orders           order placed (cart, pricing, coupon, payment, platform)
  zomato.order_status     ACCEPTED -> PREPARING -> RIDER_ASSIGNED -> PICKED_UP -> DELIVERED | CANCELLED
  zomato.rider_locations  GPS pings every few seconds for every rider on a delivery
  zomato.reviews          ratings + text after delivery

Realism: Indian lunch/dinner demand curve, city weights, popular restaurants, Gold members,
seasonal events (heavy rain only when the city is actually in its rainy season,
otherwise match nights, festival evenings) that spike demand and delivery times in one city, and ~1% dirty data
(duplicates, nulls, negative amounts, late events, corrupt JSON) for the pipeline to clean.

Order lifecycles are time-compressed (a delivery takes ~1 real minute) but carry realistic
`delivery_minutes`, so dashboards update quickly and still show believable numbers.
"""
import heapq
import itertools
import json
import logging
import math
import os
import random
import signal
import sys
import time
import uuid
import zlib
from collections import Counter
from datetime import datetime, timedelta, timezone

from dimensions import build_dimensions, load_dimensions
from reference_data import (CITIES, CITY_TRAFFIC_MIN, COUPONS, NON_RAIN_EVENTS, RAIN_CHANCE, RAIN_EVENT, CUISINES, CUSTOMER_CANCEL_REASONS, HOURLY_DEMAND_IST,
                            PAYMENT_METHODS, PLATFORMS, RESTAURANT_CANCEL_REASONS, REVIEW_TEXT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
log = logging.getLogger("generator")

ORDERS_PER_SEC = float(os.getenv("ORDERS_PER_SEC", "50"))
NUM_RESTAURANTS = int(os.getenv("NUM_RESTAURANTS", "20000"))
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", "200000"))
NUM_RIDERS = int(os.getenv("NUM_RIDERS", "30000"))
DIRTY_RATE = float(os.getenv("DIRTY_DATA_RATE", "0.01"))
DEMAND_CURVE = os.getenv("DEMAND_CURVE", "true").lower() == "true"
PING_INTERVAL = float(os.getenv("RIDER_PING_INTERVAL_SEC", "3"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
PROCESSES = max(1, int(os.getenv("GENERATOR_PROCESSES", "1")))  # fan out past one CPU core
CATALOG_CHANGE_EVERY = float(os.getenv("CATALOG_CHANGE_EVERY_SEC", "600"))   # restaurants revise prices / ratings  # print instead of producing (local testing)

T_ORDERS, T_STATUS, T_RIDERS, T_REVIEWS = ("zomato.orders", "zomato.order_status",
                                           "zomato.rider_locations", "zomato.reviews")
TOPIC_PARTITIONS = {T_ORDERS: 6, T_STATUS: 6, T_RIDERS: 12, T_REVIEWS: 3}
IST = timezone(timedelta(hours=5, minutes=30))
TICK = 0.1

rng = random.Random()


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def weighted(pairs):
    return rng.choices([p[0] for p in pairs], [p[1] for p in pairs])[0]


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(a))


# ----------------------------------------------------------------------------- sinks
class KafkaSink:
    def __init__(self):
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        futures = admin.create_topics([NewTopic(t, num_partitions=p, replication_factor=1)
                                       for t, p in TOPIC_PARTITIONS.items()])
        for topic, fut in futures.items():
            try:
                fut.result()
                log.info("Created topic %s", topic)
            except Exception as exc:  # already exists
                log.info("Topic %s: %s", topic, str(exc).split(",")[0])
        self.p = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 50,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 500000,
            "compression.type": "lz4",
            "acks": "1",
        })

    def send(self, topic, key, value):
        payload = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
        while True:
            try:
                self.p.produce(topic, key=key, value=payload)
                return
            except BufferError:
                self.p.poll(0.2)

    def poll(self):
        self.p.poll(0)

    def close(self):
        self.p.flush(15)


class PrintSink:
    def __init__(self):
        self.n = 0

    def send(self, topic, key, value):
        self.n += 1
        if self.n <= 25:
            print(topic, value if isinstance(value, str) else json.dumps(value)[:300])

    def poll(self):
        pass

    def close(self):
        pass


# ----------------------------------------------------------------------------- marketplace
class Marketplace:
    def __init__(self, restaurants, customers, riders, sink, catalog=None):
        self.sink = sink
        self.catalog = catalog            # updates dims.restaurants (feeds the dbt SCD2 snapshot)
        self.next_catalog = time.time() + 60
        self.events_sink = catalog.record_event if catalog else None
        self.rest_by_city, self.cust_by_city, self.riders_by_city = {}, {}, {}
        for r in restaurants:
            self.rest_by_city.setdefault(r["city"], []).append(r)
        for c in customers:
            self.cust_by_city.setdefault(c["city"], []).append(c)
        for d in riders:
            self.riders_by_city.setdefault(d["city"], []).append(d)
        self.cities = [c for c in CITIES if c in self.rest_by_city]
        self.city_weights = [CITIES[c]["weight"] for c in self.cities]
        self.heap, self.seq = [], itertools.count()
        self.surges = {}            # city -> (end time, (label, demand x, extra minutes, extra cancel))
        self.next_surge_check = 0.0
        self.stats = Counter()

    # -- scheduling helpers
    def schedule(self, due, action, order):
        heapq.heappush(self.heap, (due, next(self.seq), action, order))

    def emit(self, topic, key, value):
        self.sink.send(topic, key, value)
        self.stats[topic] += 1

    def event(self, city, now):
        ev = self.surges.get(city)
        return ev[1] if ev and ev[0] > now else None

    def surge(self, city, now):
        return self.event(city, now) is not None

    def demand_multiplier(self, now):
        if not DEMAND_CURVE:
            return 1.0
        return max(0.35, HOURLY_DEMAND_IST[datetime.fromtimestamp(now, IST).hour])

    def maybe_start_surge(self, now):
        if now < self.next_surge_check:
            return
        self.next_surge_check = now + 120
        month = datetime.fromtimestamp(now, IST).month
        for city in rng.sample(self.cities, len(self.cities)):  # rain only where it's the season
            if not self.surge(city, now) and rng.random() < RAIN_CHANCE.get(city, {}).get(month, 0) * 0.5:
                self.start_event(city, RAIN_EVENT, now, rng.uniform(90, 300))
                return
        if rng.random() < 0.35:                                 # otherwise an occasional dry-day spike
            city = rng.choices(self.cities, self.city_weights)[0]
            self.start_event(city, rng.choice(NON_RAIN_EVENTS), now, rng.uniform(90, 240))

    def start_event(self, city, ev, now, duration):
        self.surges[city] = (now + duration, ev)
        log.info("%s in %s -> demand and delivery times surging", ev[0], city)
        if self.events_sink:
            self.events_sink(city, ev[0], now, now + duration)

    # -- order lifecycle
    def place_order(self, now):
        weights = [w * ((self.event(c, now) or (0, 1.0))[1]) for c, w in zip(self.cities, self.city_weights)]
        city = rng.choices(self.cities, weights)[0]
        rests = self.rest_by_city[city]
        rest = rests[int(len(rests) * rng.random() ** 1.8)]   # popularity skew: some restaurants are hits
        cust = rng.choice(self.cust_by_city.get(city) or rng.choice(list(self.cust_by_city.values())))
        menu = CUISINES[rest["cuisine"]][1]
        price_factor = (rest["avg_cost_for_two"] / 500) ** 0.5

        items = []
        for name, price in rng.sample(menu, k=min(len(menu), rng.choices([1, 2, 3, 4], [40, 32, 18, 10])[0])):
            items.append({"item_name": name, "quantity": rng.choices([1, 2, 3], [75, 20, 5])[0],
                          "unit_price": round(price * price_factor)})
        subtotal = float(sum(i["quantity"] * i["unit_price"] for i in items))

        coupon, discount = None, 0.0
        if rng.random() < (0.36 if cust["is_gold"] else 0.25):
            coupon, pct, cap = rng.choices(COUPONS, [10, 30, 15, 20, 25])[0]
            if coupon == "GOLD20" and not cust["is_gold"]:     # members-only code
                coupon, pct, cap = "SUPERSAVER", 0.30, 60
            discount = round(min(subtotal * pct, cap), 2)
        delivery_fee = 0.0 if (cust["is_gold"] and subtotal >= 199) else float(rng.choice([0, 19, 29, 39, 49]))
        platform_fee, taxes = 10.0, round(subtotal * 0.05, 2)
        total = round(subtotal + delivery_fee + platform_fee + taxes - discount, 2)

        loc = CITIES[city]
        c_lat = round(loc["lat"] + rng.uniform(-0.08, 0.08), 6)
        c_lon = round(loc["lon"] + rng.uniform(-0.08, 0.08), 6)
        order = {
            "event_type": "order_placed",
            "order_id": uuid.uuid4().hex,
            "event_time": iso(now),
            "customer_id": cust["customer_id"],
            "restaurant_id": rest["restaurant_id"],
            "restaurant_name": rest["restaurant_name"],
            "city": city,
            "locality": rest["locality"],
            "cuisine": rest["cuisine"],
            "items": items,
            "items_count": sum(i["quantity"] for i in items),
            "subtotal": subtotal,
            "delivery_fee": delivery_fee,
            "platform_fee": platform_fee,
            "taxes": taxes,
            "discount": discount,
            "total_amount": total,
            "coupon_code": coupon,
            "payment_method": weighted(PAYMENT_METHODS),
            "platform": weighted(PLATFORMS),
            "is_gold": cust["is_gold"],
            "promised_minutes": rest["prep_time_min"] + rng.randint(15, 25),
            "restaurant_lat": rest["lat"], "restaurant_lon": rest["lon"],
            "customer_lat": c_lat, "customer_lon": c_lon,
        }
        self.publish_order(order, now)
        state = {"order": order, "rider": None, "pickup_ts": None, "deliver_ts": None, "quality": rest["rating"],
                 "cancel_x": 0.6 + (zlib.crc32(rest["restaurant_id"].encode()) % 100) / 70}
        self.schedule(now + rng.uniform(1, 4), "ACCEPTED", state)

    def publish_order(self, order, now):
        """Publish, occasionally corrupting the event to exercise the pipeline's data-quality layer."""
        if rng.random() >= DIRTY_RATE:
            self.emit(T_ORDERS, order["order_id"], order)
            return
        kind = rng.choice(["duplicate", "null_city", "negative_amount", "late", "corrupt"])
        self.stats[f"dirty_{kind}"] += 1
        if kind == "duplicate":
            self.emit(T_ORDERS, order["order_id"], order)
            self.emit(T_ORDERS, order["order_id"], order)
        elif kind == "null_city":
            self.emit(T_ORDERS, order["order_id"], {**order, "city": None})
        elif kind == "negative_amount":
            self.emit(T_ORDERS, order["order_id"], {**order, "total_amount": -abs(order["total_amount"])})
        elif kind == "late":
            self.emit(T_ORDERS, order["order_id"], {**order, "event_time": iso(now - rng.uniform(180, 900))})
        else:
            self.emit(T_ORDERS, order["order_id"], '{"event_type":"order_placed","order_id":' + order["order_id"][:8])

    def status_event(self, state, status, now, **extra):
        o = state["order"]
        evt = {"event_type": "order_status", "order_id": o["order_id"], "status": status,
               "event_time": iso(now), "city": o["city"], "restaurant_id": o["restaurant_id"],
               "rider_id": state["rider"]["rider_id"] if state["rider"] else None}
        evt.update(extra)
        self.emit(T_STATUS, o["order_id"], evt)

    def handle(self, action, state, now):
        o = state["order"]
        city = o["city"]
        if action == "ACCEPTED":
            if rng.random() < (0.015 + (self.event(city, now) or (0, 0, 0, 0.0))[3]) * state["cancel_x"]:
                self.status_event(state, "CANCELLED", now, cancelled_by="restaurant",
                                  cancel_reason=rng.choice(RESTAURANT_CANCEL_REASONS))
                return
            self.status_event(state, "ACCEPTED", now)
            self.schedule(now + rng.uniform(1, 3), "PREPARING", state)
        elif action == "PREPARING":
            self.status_event(state, "PREPARING", now)
            self.schedule(now + rng.uniform(3, 10), "RIDER_ASSIGNED", state)
        elif action == "RIDER_ASSIGNED":
            if rng.random() < 0.025 + (0.03 if (self.event(city, now) or ("",))[0] == "Heavy rain" else 0):
                self.status_event(state, "CANCELLED", now, cancelled_by="customer",
                                  cancel_reason=rng.choice(CUSTOMER_CANCEL_REASONS))
                return
            state["rider"] = rng.choice(self.riders_by_city.get(city) or rng.choice(list(self.riders_by_city.values())))
            self.status_event(state, "RIDER_ASSIGNED", now)
            self.schedule(now + rng.uniform(5, 20), "PICKED_UP", state)
        elif action == "PICKED_UP":
            state["pickup_ts"] = now
            state["deliver_ts"] = now + rng.uniform(15, 60)
            self.status_event(state, "PICKED_UP", now)
            self.schedule(now + PING_INTERVAL * rng.random(), "PING", state)
            self.schedule(state["deliver_ts"], "DELIVERED", state)
        elif action == "PING":
            if now >= state["deliver_ts"]:
                return
            frac = (now - state["pickup_ts"]) / (state["deliver_ts"] - state["pickup_ts"])
            lat = o["restaurant_lat"] + (o["customer_lat"] - o["restaurant_lat"]) * frac
            lon = o["restaurant_lon"] + (o["customer_lon"] - o["restaurant_lon"]) * frac
            self.emit(T_RIDERS, state["rider"]["rider_id"], {
                "event_type": "rider_location", "rider_id": state["rider"]["rider_id"],
                "order_id": o["order_id"], "city": city,
                "lat": round(lat + rng.uniform(-0.0005, 0.0005), 6),
                "lon": round(lon + rng.uniform(-0.0005, 0.0005), 6),
                "speed_kmph": round(max(0.0, rng.gauss(24, 7)), 1),
                "battery_pct": rng.randint(15, 100), "event_time": iso(now)})
            self.schedule(now + PING_INTERVAL, "PING", state)
        elif action == "DELIVERED":
            hour = datetime.fromtimestamp(now, IST).hour
            peak = 3 if hour in (12, 13, 19, 20, 21) else 0
            rain = (self.event(city, now) or (0, 0, 0))[2]   # extra minutes from rain / event traffic
            traffic = CITY_TRAFFIC_MIN.get(city, 0)
            minutes = max(12.0, round(o["promised_minutes"] - 9 + rng.gauss(0, 6) + peak + rain + traffic, 1))
            dist = round(haversine_km(o["restaurant_lat"], o["restaurant_lon"], o["customer_lat"], o["customer_lon"]), 2)
            self.status_event(state, "DELIVERED", now, delivery_minutes=minutes, distance_km=dist)
            if rng.random() < (0.5 if minutes - o["promised_minutes"] > 10 else 0.3):   # angry customers review more
                state["delivery_minutes"] = minutes
                self.schedule(now + rng.uniform(5, 20), "REVIEW", state)
        elif action == "REVIEW":
            late = state["delivery_minutes"] - o["promised_minutes"]
            penalty = 1.8 if late > 10 else 0.7 if late > 0 else 0.0
            rating = int(min(5, max(1, round(state["quality"] + 0.25 + rng.gauss(0, 0.7) - penalty))))
            self.emit(T_REVIEWS, o["order_id"], {
                "event_type": "review", "review_id": uuid.uuid4().hex, "order_id": o["order_id"],
                "customer_id": o["customer_id"], "restaurant_id": o["restaurant_id"], "city": city,
                "rating": rating, "review_text": rng.choice(REVIEW_TEXT[rating]),
                "delivery_minutes": state["delivery_minutes"], "event_time": iso(now)})

    def process_due(self, now):
        while self.heap and self.heap[0][0] <= now:
            _, _, action, state = heapq.heappop(self.heap)
            self.handle(action, state, now)

    # -- main loop
    def run(self, max_seconds=None):
        running = {"on": True}

        def stop(*_):
            running["on"] = False
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)

        start = next_tick = last_log = time.time()
        carry = 0.0
        log.info("Streaming at base %.0f orders/sec (demand curve %s)", ORDERS_PER_SEC, DEMAND_CURVE)
        while running["on"]:
            now = time.time()
            carry += ORDERS_PER_SEC * self.demand_multiplier(now) * TICK
            n, carry = int(carry), carry - int(carry)
            for _ in range(n):
                self.place_order(now)
            self.process_due(now)
            self.maybe_start_surge(now)
            if self.catalog and now >= self.next_catalog:
                self.next_catalog = now + CATALOG_CHANGE_EVERY
                self.catalog.revise(rng)
            self.sink.poll()

            if now - last_log >= 10:
                elapsed = now - last_log
                total = sum(v for k, v in self.stats.items() if k.startswith("zomato."))
                log.info("%.0f events/sec | %s | in-flight orders: %d", total / elapsed,
                         ", ".join(f"{k.replace('zomato.', '')}={v}" for k, v in sorted(self.stats.items())), len(self.heap))
                self.stats.clear()
                last_log = now
            if max_seconds and now - start > max_seconds:
                break
            next_tick += TICK
            sleep = next_tick - time.time()
            if sleep > 0:
                time.sleep(sleep)
            else:
                next_tick = time.time()
        log.info("Shutting down, flushing producer...")
        self.sink.close()


def connect_postgres(retries=60):
    import psycopg2
    for attempt in range(retries):
        try:
            return psycopg2.connect(host=os.getenv("PG_HOST", "postgres"), port=os.getenv("PG_PORT", "5432"),
                                    dbname=os.getenv("PG_DB", "zomato"), user=os.getenv("PG_USER", "zomato"),
                                    password=os.getenv("PG_PASSWORD", "zomato"))
        except Exception as exc:
            log.warning("Postgres not ready (%s), retry %d", exc, attempt + 1)
            time.sleep(3)
    sys.exit("Could not connect to Postgres")


class CatalogUpdater:
    """Every few minutes a handful of restaurants change price or see their rating drift,
    like a real catalogue. Writes to dims.restaurants; dbt's snapshot keeps the history (SCD2)."""

    def __init__(self, restaurants, share=0.003):
        self.restaurants, self.share = restaurants, share
        self.conn = connect_postgres()

    def record_event(self, city, kind, start, end):
        """Publishes the active event so the website can label it (rain vs match night)."""
        try:
            with self.conn, self.conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS realtime.city_events (city TEXT, kind TEXT, "
                            "started_at TIMESTAMPTZ, ends_at TIMESTAMPTZ)")
                cur.execute("GRANT SELECT ON realtime.city_events TO grafana")
                cur.execute("DELETE FROM realtime.city_events WHERE ends_at < now() - interval '1 day'")
                cur.execute("INSERT INTO realtime.city_events VALUES (%s, %s, to_timestamp(%s), to_timestamp(%s))",
                            (city, kind, start, end))
        except Exception as exc:
            log.warning("event write failed: %s", exc)
            self.conn = connect_postgres()

    def revise(self, r):
        picks = r.sample(self.restaurants, max(1, int(len(self.restaurants) * self.share)))
        rows = []
        for rest in picks:
            if r.random() < 0.6:   # price revision, usually up
                rest["avg_cost_for_two"] = max(150, rest["avg_cost_for_two"] + r.choice([50, 50, 100, -50]))
            else:                  # rating drift
                rest["rating"] = round(min(4.9, max(2.5, rest["rating"] + r.choice([-0.2, -0.1, 0.1, 0.1, 0.2]))), 1)
            rows.append((rest["avg_cost_for_two"], rest["rating"], rest["restaurant_id"]))
        try:
            with self.conn, self.conn.cursor() as cur:
                cur.executemany("UPDATE dims.restaurants SET avg_cost_for_two = %s, rating = %s WHERE restaurant_id = %s", rows)
            log.info("Catalogue update: %d restaurants changed price or rating", len(rows))
        except Exception as exc:
            log.warning("catalogue update failed: %s", exc)
            self.conn = connect_postgres()


def run_worker(worker_id, restaurants, customers, riders):
    global ORDERS_PER_SEC
    ORDERS_PER_SEC = ORDERS_PER_SEC / PROCESSES
    rng.seed(os.getpid() * 7919 + worker_id)
    log.name = f"generator-{worker_id}"
    sink = PrintSink() if DRY_RUN else KafkaSink()
    catalog = CatalogUpdater(restaurants) if (worker_id == 0 and not DRY_RUN) else None
    Marketplace(restaurants, customers, riders, sink, catalog).run(
        max_seconds=float(os.getenv("RUN_SECONDS", "0")) or None)


def main():
    log.info("Building reference data...")
    restaurants, customers, riders = build_dimensions(NUM_RESTAURANTS, NUM_CUSTOMERS, NUM_RIDERS)
    if not DRY_RUN:
        conn = connect_postgres()
        load_dimensions(conn, restaurants, customers, riders)
        conn.close()
    if PROCESSES == 1:
        run_worker(0, restaurants, customers, riders)
        return
    import multiprocessing as mp
    ctx = mp.get_context("fork")   # children share the reference data copy-on-write
    log.info("Starting %d generator processes", PROCESSES)
    procs = [ctx.Process(target=run_worker, args=(i, restaurants, customers, riders), daemon=False)
             for i in range(PROCESSES)]
    for p in procs:
        p.start()

    def forward(sig, _):
        for p in procs:
            if p.is_alive():
                p.terminate()
    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)
    for p in procs:
        p.join()
    if any(p.exitcode not in (0, -signal.SIGTERM) for p in procs):
        sys.exit(1)


if __name__ == "__main__":
    main()
