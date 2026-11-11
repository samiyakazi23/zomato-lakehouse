"""Kafka topic names and JSON schemas (DDL) for every event type."""

T_ORDERS = "zomato.orders"
T_STATUS = "zomato.order_status"
T_RIDERS = "zomato.rider_locations"
T_REVIEWS = "zomato.reviews"
ALL_TOPICS = [T_ORDERS, T_STATUS, T_RIDERS, T_REVIEWS]

ORDER_DDL = """
  event_type STRING, order_id STRING, event_time STRING, customer_id STRING,
  restaurant_id STRING, restaurant_name STRING, city STRING, locality STRING, cuisine STRING,
  items ARRAY<STRUCT<item_name: STRING, quantity: INT, unit_price: DOUBLE>>, items_count INT,
  subtotal DOUBLE, delivery_fee DOUBLE, platform_fee DOUBLE, taxes DOUBLE, discount DOUBLE,
  total_amount DOUBLE, coupon_code STRING, payment_method STRING, platform STRING, is_gold BOOLEAN,
  promised_minutes INT, restaurant_lat DOUBLE, restaurant_lon DOUBLE, customer_lat DOUBLE, customer_lon DOUBLE
"""

STATUS_DDL = """
  event_type STRING, order_id STRING, status STRING, event_time STRING, city STRING,
  restaurant_id STRING, rider_id STRING, delivery_minutes DOUBLE, distance_km DOUBLE,
  cancelled_by STRING, cancel_reason STRING
"""

RIDER_DDL = """
  event_type STRING, rider_id STRING, order_id STRING, city STRING, lat DOUBLE, lon DOUBLE,
  speed_kmph DOUBLE, battery_pct INT, event_time STRING
"""

REVIEW_DDL = """
  event_type STRING, review_id STRING, order_id STRING, customer_id STRING, restaurant_id STRING,
  city STRING, rating INT, review_text STRING, delivery_minutes DOUBLE, event_time STRING
"""

# Unified order fact used by gold (historical backfill + streaming share these columns)
FACT_COLUMNS = [
    "order_id", "order_ts", "order_date", "order_hour_ist", "city", "locality", "restaurant_id",
    "restaurant_name", "cuisine", "customer_id", "is_gold", "rider_id", "items_count", "subtotal",
    "delivery_fee", "platform_fee", "taxes", "discount", "total_amount", "coupon_code", "payment_method",
    "platform", "promised_minutes", "final_status", "cancel_reason", "delivery_minutes", "distance_km",
    "rating", "source",
]


# City personalities used by the historical backfill (mirrors generator/reference_data.py)
CITY_TRAFFIC_MIN = {"Bengaluru": 4, "Mumbai": 3, "Delhi NCR": 2, "Kolkata": 1, "Pune": 1, "Chennai": 0, "Hyderabad": 0,
                    "Ahmedabad": -1, "Lucknow": -1, "Jaipur": -2}
CITY_HOUR_SHIFT = {"Bengaluru": 1, "Mumbai": 1, "Hyderabad": 1, "Delhi NCR": 0, "Pune": 0, "Kolkata": 0,
                   "Chennai": -1, "Ahmedabad": -1, "Lucknow": -1, "Jaipur": 0}
CITY_GROWTH = {"Bengaluru": 0.45, "Hyderabad": 0.42, "Pune": 0.38, "Jaipur": 0.35, "Lucknow": 0.32, "Delhi NCR": 0.30,
               "Ahmedabad": 0.30, "Mumbai": 0.27, "Chennai": 0.24, "Kolkata": 0.10}   # share of volume added over the year
CITY_GOLD_PTS = {"Mumbai": 6, "Bengaluru": 6, "Delhi NCR": 5, "Hyderabad": 3, "Pune": 3, "Chennai": 1, "Kolkata": -2,
                 "Ahmedabad": -1, "Jaipur": -3, "Lucknow": -3}

# Rainy-season months per city (mirrors generator/reference_data.RAIN_CHANCE, months with real rain)
RAIN_MONTHS = {"Mumbai": [6, 7, 8, 9], "Pune": [6, 7, 8, 9], "Kolkata": [6, 7, 8, 9], "Bengaluru": [8, 9, 10],
               "Hyderabad": [7, 8, 9, 10], "Chennai": [10, 11, 12], "Delhi NCR": [7, 8], "Lucknow": [7, 8],
               "Jaipur": [7, 8], "Ahmedabad": [7, 8]}
