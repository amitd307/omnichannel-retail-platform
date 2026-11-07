"""Channel adapters: each source format -> one canonical order-line model.

Canonical order line:
    order_id, channel, line_no, order_ts (UTC), city, sku, quantity,
    net_amount_inr, email_norm, phone_norm, status
Adding a new channel = writing one more adapter that returns these columns.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CANONICAL = ["order_id", "channel", "line_no", "order_ts", "city", "sku", "quantity",
             "net_amount_inr", "email_norm", "phone_norm", "status"]

STORE_CITY = {"BLR": "Bengaluru", "MUM": "Mumbai", "DEL": "Delhi", "HYD": "Hyderabad"}


def norm_email(col):
    e = F.lower(F.trim(col))
    return F.when(e.rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), e)


def norm_phone(col):
    """Keep the last 10 digits of an Indian mobile number; anything shorter is unusable."""
    digits = F.regexp_replace(F.coalesce(col.cast("string"), F.lit("")), r"\D", "")
    last10 = F.substring(digits, -10, 10)
    return F.when((F.length(digits) >= 10) & last10.rlike(r"^[6-9]\d{9}$"), last10)


def from_web(df: DataFrame) -> DataFrame:
    lines = df.select("orderId", "placedAt", "customer", "status",
                      F.posexplode("items").alias("pos", "item"))
    return lines.select(
        F.concat(F.lit("WEB-"), "orderId").alias("order_id"),
        F.lit("web").alias("channel"),
        (F.col("pos") + 1).alias("line_no"),
        F.to_timestamp("placedAt").alias("order_ts"),
        F.col("customer.city").alias("city"),
        F.col("item.sku").alias("sku"),
        F.col("item.qty").cast("int").alias("quantity"),
        (F.col("item.qty") * F.col("item.price")).cast("decimal(14,2)").alias("net_amount_inr"),
        norm_email(F.col("customer.email")).alias("email_norm"),
        norm_phone(F.col("customer.phone")).alias("phone_norm"),
        F.col("status"),
    )


def from_app(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("app_order_ref").orderBy("sku_code")
    return df.select(
        F.concat(F.lit("APP-"), "app_order_ref").alias("order_id"),
        F.lit("app").alias("channel"),
        F.row_number().over(w).alias("line_no"),
        F.timestamp_millis(F.col("ts_epoch_ms").cast("long")).alias("order_ts"),
        F.col("delivery_city").alias("city"),
        F.col("sku_code").alias("sku"),
        F.col("units").cast("int").alias("quantity"),
        (F.col("line_total_paise") / 100).cast("decimal(14,2)").alias("net_amount_inr"),
        F.lit(None).cast("string").alias("email_norm"),
        norm_phone(F.col("user_mobile")).alias("phone_norm"),
        F.lit("DELIVERED").alias("status"),
    )


def from_store(df: DataFrame) -> DataFrame:
    city_map = F.create_map(*[F.lit(x) for kv in STORE_CITY.items() for x in kv])
    return df.select(
        F.concat(F.lit("POS-"), "bill_no").alias("order_id"),
        F.lit("store").alias("channel"),
        F.col("line").cast("int").alias("line_no"),
        F.to_timestamp("bill_date", "dd-MM-yyyy HH:mm").alias("order_ts"),
        city_map[F.substring("store_code", 1, 3)].alias("city"),
        F.col("item").alias("sku"),
        F.col("qty").cast("int").alias("quantity"),
        F.col("amount").cast("decimal(14,2)").alias("net_amount_inr"),
        F.lit(None).cast("string").alias("email_norm"),
        norm_phone(F.col("customer_phone")).alias("phone_norm"),
        F.lit("DELIVERED").alias("status"),
    )


def unify(web: DataFrame, app: DataFrame, store: DataFrame) -> DataFrame:
    return (from_web(web).select(CANONICAL)
            .unionByName(from_app(app).select(CANONICAL))
            .unionByName(from_store(store).select(CANONICAL)))
