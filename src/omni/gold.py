"""Gold tables for reporting on the unified model."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def daily_sales_by_channel(lines: DataFrame) -> DataFrame:
    live = lines.filter(F.col("status") != "CANCELLED")
    return (live.groupBy(F.to_date("order_ts").alias("order_date"), "channel", "city")
            .agg(F.countDistinct("order_id").alias("orders"),
                 F.sum("quantity").alias("units"),
                 F.sum("net_amount_inr").alias("net_sales_inr")))


def customer_360(lines: DataFrame) -> DataFrame:
    live = lines.filter(F.col("customer_id").isNotNull() & (F.col("status") != "CANCELLED"))
    return (live.groupBy("customer_id")
            .agg(F.sort_array(F.collect_set("channel")).alias("channels"),
                 F.countDistinct("order_id").alias("orders"),
                 F.sum("net_amount_inr").alias("lifetime_value_inr"),
                 F.min("order_ts").alias("first_order"),
                 F.max("order_ts").alias("last_order"))
            .withColumn("is_omnichannel", F.size("channels") > 1))
