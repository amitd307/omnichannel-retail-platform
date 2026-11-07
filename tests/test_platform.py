from pyspark.sql import functions as F

from omni.channels import norm_phone, unify
from omni.gold import customer_360
from omni.identity import resolve
from omni.quality import reconcile, source_totals


def load(spark, root):
    raw = root / "data" / "raw"
    web = spark.read.json(str(raw / "web_orders.json"))
    app = spark.read.option("header", True).option("inferSchema", True).csv(str(raw / "app_orders.csv"))
    store = spark.read.option("header", True).option("inferSchema", True).csv(str(raw / "store_pos.csv"))
    return web, app, store


def test_phone_normalisation(spark):
    df = spark.createDataFrame([("+91 98450 12345",), ("098450-12345",), ("98450 12345",),
                                ("12345",), (None,), ("5845012345",)], "p string")
    got = [r.n for r in df.select(norm_phone(F.col("p")).alias("n")).collect()]
    assert got == ["9845012345", "9845012345", "9845012345", None, None, None]


def test_unified_model_reconciles_with_every_channel(spark, root):
    web, app, store = load(spark, root)
    lines = unify(web, app, store)
    for r in reconcile(lines, source_totals(web, app, store)):
        assert r.ok, r
    assert lines.filter("order_ts IS NULL OR city IS NULL").count() == 0


def test_identity_links_app_phone_to_web_email(spark):
    web = spark.createDataFrame(
        [("W1", "2026-01-01T10:00:00+00:00",
          ("Asha@Example.com ", "9845012345", "Bengaluru"), [("SKU-TV43", 1, 100)], "DELIVERED")],
        "orderId string, placedAt string, customer struct<email:string,phone:string,city:string>, "
        "items array<struct<sku:string,qty:int,price:int>>, status string")
    app = spark.createDataFrame(
        [("A1", "SKU-MIXER", 1, 5000, 1767261600000, "+919845012345", "Bengaluru")],
        "app_order_ref string, sku_code string, units int, line_total_paise long, ts_epoch_ms long, "
        "user_mobile string, delivery_city string")
    store = spark.createDataFrame(
        [("S1", 1, "BLR-01", "01-01-2026 12:00", "SKU-TV43", 1, 100, ""),
         ("S2", 1, "MUM-01", "01-01-2026 12:00", "SKU-TV43", 1, 100, "9999999999")],
        "bill_no string, line int, store_code string, bill_date string, item string, qty int, "
        "amount int, customer_phone string")

    lines = resolve(unify(web, app, store))
    ids = {r.order_id: r.customer_id for r in lines.collect()}
    assert ids["WEB-W1"] == ids["APP-A1"] is not None      # same person across channels
    assert ids["POS-S1"] is None                            # anonymous walk-in
    assert ids["POS-S2"] not in (None, ids["WEB-W1"])       # different person

    c360 = {r.customer_id: r for r in customer_360(lines).collect()}
    assert c360[ids["WEB-W1"]].channels == ["app", "web"]
    assert c360[ids["WEB-W1"]].is_omnichannel is True


def test_sample_data_has_omnichannel_customers(spark, root):
    lines = resolve(unify(*load(spark, root)))
    c360 = customer_360(lines)
    assert c360.filter("is_omnichannel").count() > 10
