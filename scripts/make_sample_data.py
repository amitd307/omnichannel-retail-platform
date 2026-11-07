"""Synthetic order extracts from three channels, each in its own format.

    python scripts/make_sample_data.py --out data/raw

  web_orders.json   nested JSON, customer email + phone, amounts in INR, ISO timestamps
  app_orders.csv    flat CSV, user_mobile only, amounts in **paise**, epoch-millis timestamps
  store_pos.csv     one row per line item, phone typed by cashiers (+91, spaces, dashes),
                    dd-MM-yyyy HH:mm dates, store codes instead of cities

Some customers buy in more than one channel; matching them is the job of the
identity-resolution step.
"""
import argparse
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

CITIES = {"BLR": "Bengaluru", "MUM": "Mumbai", "DEL": "Delhi", "HYD": "Hyderabad"}
PRODUCTS = {"SKU-TV43": 32990, "SKU-SOUNDBAR": 8990, "SKU-WM7KG": 24990, "SKU-PURIFIER": 11990,
            "SKU-TREADMILL": 45990, "SKU-MIXER": 4490, "SKU-LAPTOP14": 58990}


def people(n, rng):
    out = []
    for i in range(1, n + 1):
        phone = f"9{rng.randint(100000000, 999999999)}"
        out.append({"email": f"user{i}@example.com", "phone": phone, "city": rng.choice(list(CITIES))})
    return out


def messy_phone(p, rng):
    return rng.choice([p, f"+91{p}", f"+91 {p[:5]} {p[5:]}", f"0{p}", f"{p[:3]}-{p[3:6]}-{p[6:]}"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--customers", type=int, default=300)
    ap.add_argument("--orders", type=int, default=600)
    ap.add_argument("--seed", type=int, default=5)
    a = ap.parse_args()
    rng, out = random.Random(a.seed), Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    crowd = people(a.customers, rng)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    web, app, pos = [], [], []
    for o in range(1, a.orders + 1):
        c = rng.choice(crowd)
        ts = start + timedelta(minutes=rng.randint(0, 60 * 24 * 90))
        items = [(sku, rng.randint(1, 2)) for sku in rng.sample(list(PRODUCTS), rng.randint(1, 3))]
        channel = rng.choices(["web", "app", "store"], weights=[4, 4, 2])[0]
        if channel == "web":
            web.append({
                "orderId": f"W{o:06d}", "placedAt": ts.isoformat(),
                "customer": {"email": c["email"].upper() if rng.random() < 0.2 else c["email"],
                             "phone": c["phone"], "city": CITIES[c["city"]]},
                "items": [{"sku": s, "qty": q, "price": PRODUCTS[s]} for s, q in items],
                "status": rng.choice(["DELIVERED"] * 9 + ["CANCELLED"]),
            })
        elif channel == "app":
            for s, q in items:
                app.append({"app_order_ref": f"A{o:06d}", "sku_code": s, "units": q,
                            "line_total_paise": PRODUCTS[s] * q * 100,
                            "ts_epoch_ms": int(ts.timestamp() * 1000),
                            "user_mobile": c["phone"], "delivery_city": CITIES[c["city"]]})
        else:
            store = f"{c['city']}-{rng.randint(1, 3):02d}"
            for ln, (s, q) in enumerate(items, 1):
                pos.append({"bill_no": f"S{o:06d}", "line": ln, "store_code": store,
                            "bill_date": ts.strftime("%d-%m-%Y %H:%M"), "item": s, "qty": q,
                            "amount": PRODUCTS[s] * q,
                            "customer_phone": messy_phone(c["phone"], rng) if rng.random() < 0.85 else ""})

    (out / "web_orders.json").write_text("\n".join(json.dumps(r) for r in web), encoding="utf-8")
    for name, rows in [("app_orders.csv", app), ("store_pos.csv", pos)]:
        with (out / name).open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
    print(f"web={len(web)} orders, app={len(app)} lines, store={len(pos)} lines -> {out}/")


if __name__ == "__main__":
    main()
