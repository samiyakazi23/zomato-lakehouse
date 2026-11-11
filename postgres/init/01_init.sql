-- Runs once, the first time the Postgres volume is created.
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER zomato WITH PASSWORD 'zomato';
CREATE DATABASE zomato OWNER zomato;

\connect zomato
SET ROLE zomato;

CREATE SCHEMA IF NOT EXISTS dims;       -- reference data written by the generator
CREATE SCHEMA IF NOT EXISTS realtime;   -- 1-minute windows upserted by Spark Streaming
CREATE SCHEMA IF NOT EXISTS gold;       -- batch aggregates written by Spark (Airflow)
CREATE SCHEMA IF NOT EXISTS analytics;  -- dbt marts
CREATE SCHEMA IF NOT EXISTS ml;         -- model metrics and clusters written by the Spark MLlib job

-- ----------------------------------------------------------------- dimensions
CREATE TABLE dims.restaurants (
    restaurant_id     TEXT PRIMARY KEY,
    restaurant_name   TEXT NOT NULL,
    city              TEXT NOT NULL,
    locality          TEXT NOT NULL,
    cuisine           TEXT NOT NULL,
    rating            NUMERIC(2,1),
    avg_cost_for_two  INT,
    is_pure_veg       BOOLEAN,
    prep_time_min     INT,
    lat               DOUBLE PRECISION,
    lon               DOUBLE PRECISION,
    onboarded_on      DATE
);
CREATE INDEX ON dims.restaurants (city);

CREATE TABLE dims.customers (
    customer_id    TEXT PRIMARY KEY,
    customer_name  TEXT NOT NULL,
    city           TEXT NOT NULL,
    locality       TEXT NOT NULL,
    signup_date    DATE,
    is_gold        BOOLEAN
);
CREATE INDEX ON dims.customers (city);

CREATE TABLE dims.riders (
    rider_id      TEXT PRIMARY KEY,
    rider_name    TEXT NOT NULL,
    city          TEXT NOT NULL,
    vehicle_type  TEXT,
    joined_on     DATE
);
CREATE INDEX ON dims.riders (city);

CREATE TABLE dims.meta (key TEXT PRIMARY KEY, value TEXT);   -- fingerprint of the loaded reference data

-- ----------------------------------------------------------------- real-time serving tables
CREATE TABLE realtime.city_orders_1m (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    city             TEXT NOT NULL,
    orders           BIGINT,
    gmv              DOUBLE PRECISION,
    avg_order_value  DOUBLE PRECISION,
    gold_orders      BIGINT,
    coupon_orders    BIGINT,
    updated_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (window_start, city)
);

CREATE TABLE realtime.cuisine_orders_1m (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    cuisine       TEXT NOT NULL,
    orders        BIGINT,
    gmv           DOUBLE PRECISION,
    updated_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (window_start, cuisine)
);

-- every live order scored by the late-delivery model (Spark Streaming + MLlib)
CREATE TABLE realtime.order_risk (
    order_id          TEXT PRIMARY KEY,
    event_ts          TIMESTAMPTZ,
    city              TEXT,
    restaurant_name   TEXT,
    cuisine           TEXT,
    order_hour_ist    INT,
    promised_minutes  INT,
    late_probability  DOUBLE PRECISION,
    risk_level        TEXT,
    updated_at        TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX order_risk_ts ON realtime.order_risk (event_ts DESC);

CREATE TABLE realtime.order_status_1m (
    window_start          TIMESTAMPTZ NOT NULL,
    window_end            TIMESTAMPTZ NOT NULL,
    city                  TEXT NOT NULL,
    status                TEXT NOT NULL,
    events                BIGINT,
    avg_delivery_minutes  DOUBLE PRECISION,
    updated_at            TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (window_start, city, status)
);

CREATE TABLE realtime.rider_activity_1m (
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    city           TEXT NOT NULL,
    active_riders  BIGINT,
    pings          BIGINT,
    avg_speed_kmph DOUBLE PRECISION,
    updated_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (window_start, city)
);

-- Demand events announced by the generator (heavy rain in season, match nights, festivals)
CREATE TABLE realtime.city_events (city TEXT, kind TEXT, started_at TIMESTAMPTZ, ends_at TIMESTAMPTZ);

-- One row per micro-batch of every streaming query (written by a StreamingQueryListener)
CREATE TABLE realtime.stream_progress (
    query_name        TEXT NOT NULL,
    batch_id          BIGINT NOT NULL,
    input_rows        BIGINT,
    input_rps         DOUBLE PRECISION,
    processed_rps     DOUBLE PRECISION,
    batch_duration_ms BIGINT,
    ts                TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);
CREATE INDEX ON realtime.stream_progress (ts);

-- Grafana reads everything with a read-only login
RESET ROLE;
CREATE ROLE grafana LOGIN PASSWORD 'grafana';
GRANT USAGE ON SCHEMA dims, realtime, gold, analytics, ml TO grafana;
GRANT SELECT ON ALL TABLES IN SCHEMA dims, realtime, gold, analytics, ml TO grafana;
ALTER DEFAULT PRIVILEGES FOR ROLE zomato IN SCHEMA dims, realtime, gold, analytics, ml GRANT SELECT ON TABLES TO grafana;
