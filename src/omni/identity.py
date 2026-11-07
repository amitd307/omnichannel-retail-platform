"""Customer identity resolution across channels.

Rules (deterministic and explainable to the business):
  1. Web orders carry both email and phone, which links the two identifiers.
     If one phone appears with several emails, the most frequent email wins.
  2. customer_id is derived from the email when one is known for the person,
     otherwise from the phone. A shopper who bought on the app (phone only)
     and on the web (email + phone) therefore becomes one customer.
  3. Lines with no usable identifier are anonymous walk-ins (customer_id null).
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def _cid(col):
    return F.concat(F.lit("CUST-"), F.substring(F.sha2(col, 256), 1, 12))


def phone_email_links(lines: DataFrame) -> DataFrame:
    both = lines.filter(F.col("email_norm").isNotNull() & F.col("phone_norm").isNotNull())
    w = Window.partitionBy("phone_norm").orderBy(F.col("n").desc(), "email_norm")
    return (both.groupBy("phone_norm", "email_norm").agg(F.count("*").alias("n"))
            .withColumn("rk", F.row_number().over(w)).filter("rk = 1")
            .select("phone_norm", F.col("email_norm").alias("linked_email")))


def resolve(lines: DataFrame) -> DataFrame:
    links = phone_email_links(lines)
    resolved = lines.join(links, "phone_norm", "left").withColumn(
        "customer_id",
        F.when(F.col("email_norm").isNotNull(), _cid(F.col("email_norm")))
        .when(F.col("linked_email").isNotNull(), _cid(F.col("linked_email")))
        .when(F.col("phone_norm").isNotNull(), _cid(F.concat(F.lit("tel:"), F.col("phone_norm")))),
    )
    return resolved.drop("linked_email")
