"""End-to-end build: raw channel extracts -> unified order lines -> Gold.

    PYTHONPATH=src python -m omni.build --raw data/raw --out output
"""
import argparse
import logging
import sys

from pyspark.sql import SparkSession

from .channels import unify
from .gold import customer_360, daily_sales_by_channel
from .identity import resolve
from .quality import reconcile, row_checks, source_totals

log = logging.getLogger("omni")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--out", default="output")
    p.add_argument("--format", default="parquet", choices=["parquet", "delta"])
    a = p.parse_args()

    spark = SparkSession.builder.appName("omnichannel").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    web = spark.read.json(f"{a.raw}/web_orders.json")
    app = spark.read.option("header", True).option("inferSchema", True).csv(f"{a.raw}/app_orders.csv")
    store = spark.read.option("header", True).option("inferSchema", True).csv(f"{a.raw}/store_pos.csv")

    lines = resolve(unify(web, app, store)).cache()

    recon = reconcile(lines, source_totals(web, app, store))
    for r in recon:
        (log.info if r.ok else log.error)("recon %-5s lines %d/%d amount %.2f/%.2f", r.channel,
                                          r.unified_lines, r.source_lines, r.unified_amount, r.source_amount)
    log.info("row checks: %s", row_checks(lines))
    if not all(r.ok for r in recon):
        log.error("reconciliation failed, not publishing Gold")
        return 1

    lines.write.mode("overwrite").format(a.format).save(f"{a.out}/silver/order_lines")
    daily_sales_by_channel(lines).write.mode("overwrite").format(a.format).save(f"{a.out}/gold/daily_sales_by_channel")
    c360 = customer_360(lines)
    c360.write.mode("overwrite").format(a.format).save(f"{a.out}/gold/customer_360")
    log.info("customers: %d, omnichannel: %d", c360.count(), c360.filter("is_omnichannel").count())
    return 0


if __name__ == "__main__":
    sys.exit(main())
