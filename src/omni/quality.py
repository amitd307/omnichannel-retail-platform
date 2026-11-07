"""Per-channel reconciliation: did every source line and rupee make it into the
unified table? Run after each load; any mismatch blocks publishing."""
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class Recon:
    channel: str
    source_lines: int
    unified_lines: int
    source_amount: float
    unified_amount: float

    @property
    def ok(self) -> bool:
        return self.source_lines == self.unified_lines and abs(self.source_amount - self.unified_amount) < 0.01


def source_totals(web: DataFrame, app: DataFrame, store: DataFrame) -> dict:
    w = web.select(F.explode("items").alias("i")).agg(
        F.count("*"), F.sum(F.col("i.qty") * F.col("i.price"))).collect()[0]
    a = app.agg(F.count("*"), F.sum(F.col("line_total_paise") / 100)).collect()[0]
    s = store.agg(F.count("*"), F.sum("amount")).collect()[0]
    return {"web": (w[0], float(w[1] or 0)), "app": (a[0], float(a[1] or 0)), "store": (s[0], float(s[1] or 0))}


def reconcile(unified: DataFrame, totals: dict) -> list:
    got = {r.channel: (r.n, float(r.amt or 0)) for r in
           unified.groupBy("channel").agg(F.count("*").alias("n"), F.sum("net_amount_inr").alias("amt")).collect()}
    return [Recon(ch, src[0], got.get(ch, (0, 0.0))[0], src[1], got.get(ch, (0, 0.0))[1])
            for ch, src in totals.items()]


def row_checks(unified: DataFrame) -> dict:
    """Counts of rows that need attention (they are kept, not dropped)."""
    return unified.agg(
        F.sum(F.when(F.col("order_ts").isNull(), 1).otherwise(0)).alias("unparseable_timestamp"),
        F.sum(F.when(F.col("city").isNull(), 1).otherwise(0)).alias("unknown_city"),
        F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0)).alias("anonymous_lines"),
        F.sum(F.when(F.col("quantity") <= 0, 1).otherwise(0)).alias("bad_quantity"),
    ).collect()[0].asDict()
