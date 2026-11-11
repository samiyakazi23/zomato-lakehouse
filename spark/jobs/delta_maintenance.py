"""Daily Delta Lake housekeeping for batch-owned tables: compaction (OPTIMIZE) and VACUUM.

The billion-row history is written as one large file per day, so it is never re-compacted
(that would rewrite tens of GB nightly) - it only gets VACUUMed. Small aggregate tables are compacted.

Streaming-owned silver/bronze tables use auto-compaction inside the streaming app instead,
because Delta on S3 allows only one writer process per table without an external lock service.
"""
import logging

from aggregates import KEYS
from common import FMT, get_spark, lake, read_lake

log = logging.getLogger("maintenance")

TABLES = {   # path -> run OPTIMIZE?
    ("silver", "orders_historical"): False,
    **{("gold", f"hist_agg_{n}"): True for n in KEYS if n != "customer_day"},
    **{("gold", f"stream_agg_{n}"): True for n in KEYS if n != "customer_month"},
    ("gold", "restaurant_performance"): True,
    ("gold", "daily_city_metrics"): True,
}


def main():
    spark = get_spark("zomato-delta-maintenance")
    if FMT != "delta":
        log.info("LAKE_FORMAT=%s - maintenance only applies to Delta tables", FMT)
        return
    for parts, optimize in TABLES.items():
        if read_lake(spark, *parts) is None:
            continue
        path = lake(*parts)
        if optimize:
            spark.sql(f"OPTIMIZE delta.`{path}`").select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()
        spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")
        log.info("vacuumed %s", path)
    spark.stop()


if __name__ == "__main__":
    main()
