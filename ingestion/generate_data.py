#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

FIRST_NAMES = ["Alex", "Taylor", "Jordan", "Morgan", "Casey", "Riley", "Sam", "Jamie", "Avery", "Quinn"]
LAST_NAMES = ["Smith", "Johnson", "Lee", "Patel", "Garcia", "Brown", "Davis", "Miller", "Wilson", "Anderson"]
STATES = ["WA", "CA", "OR", "NY", "TX", "IL", "MA", "CO", "AZ", "GA"]
CATEGORIES = ["Accessories", "Apparel", "Electronics", "Home", "Outdoors"]
URLS = ["/", "/pricing", "/docs", "/blog", "/blog/dbt", "/blog/duckdb", "/signup", "/login", "/account", "/checkout"]
REFERRERS = ["google", "bing", "newsletter", "twitter", "reddit", "direct"]


def _rand_email(rng: random.Random, first: str, last: str, i: int) -> str:
    domain = rng.choice(["example.com", "example.org", "example.net"])
    return f"{first.lower()}.{last.lower()}{i}@{domain}"


def _dt_range_days(end_utc: datetime, days: int) -> list[datetime]:
    """Midnight boundaries in UTC, oldest -> newest."""
    end_day = datetime(end_utc.year, end_utc.month, end_utc.day, tzinfo=timezone.utc)
    return [end_day - timedelta(days=d) for d in range(days)][::-1]


def generate_customers(rng: random.Random, n: int, end_utc: datetime, days: int) -> pd.DataFrame:
    dates = _dt_range_days(end_utc, days)
    rows = []
    for i in range(1, n + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        created_at = rng.choice(dates).date()
        rows.append(
            {
                "customer_id": i,
                "first_name": first,
                "last_name": last,
                "email": _rand_email(rng, first, last, i),
                "status": rng.choices(["active", "inactive"], weights=[0.92, 0.08])[0],
                "segment": rng.choices(["consumer", "business"], weights=[0.85, 0.15])[0],
                "state": rng.choice(STATES),
                "created_at": created_at,
            }
        )
    return pd.DataFrame(rows)


def generate_products(rng: random.Random, n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        category = rng.choice(CATEGORIES)
        base_price = rng.uniform(8, 250)
        rows.append(
            {
                "product_id": i,
                "product_name": f"{category} Item {i:03d}",
                "category": category,
                "price": round(base_price, 2),
                "is_active": rng.choices([True, False], weights=[0.97, 0.03])[0],
            }
        )
    return pd.DataFrame(rows)


def generate_orders(
    rng: random.Random, customers: pd.DataFrame, n_orders: int, end_utc: datetime, days: int
) -> pd.DataFrame:
    start = end_utc - timedelta(days=days)
    rows = []
    customer_ids = customers["customer_id"].tolist()
    for i in range(1, n_orders + 1):
        customer_id = int(rng.choice(customer_ids))
        seconds = rng.randint(0, int((end_utc - start).total_seconds()))
        ts = start + timedelta(seconds=seconds)
        status = rng.choices(["completed", "cancelled", "returned"], weights=[0.92, 0.05, 0.03])[0]
        rows.append({"order_id": i, "customer_id": customer_id, "order_ts_utc": ts, "status": status})
    return pd.DataFrame(rows)


def generate_order_items(rng: random.Random, orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    rows = []
    order_item_id = 1
    product_lookup = products.set_index("product_id")["price"].to_dict()
    product_ids = products["product_id"].tolist()

    for order_id in orders["order_id"].tolist():
        n_lines = rng.choices([1, 2, 3, 4], weights=[0.55, 0.25, 0.15, 0.05])[0]
        picked = rng.sample(product_ids, k=n_lines)
        for pid in picked:
            qty = rng.choices([1, 2, 3, 4], weights=[0.70, 0.20, 0.08, 0.02])[0]
            unit_price = float(product_lookup[pid])
            discount_pct = rng.choices([0.0, 0.05, 0.10, 0.15], weights=[0.75, 0.12, 0.10, 0.03])[0]
            rows.append(
                {
                    "order_item_id": order_item_id,
                    "order_id": int(order_id),
                    "product_id": int(pid),
                    "quantity": int(qty),
                    "unit_price": round(unit_price, 2),
                    "discount_pct": float(discount_pct),
                }
            )
            order_item_id += 1

    return pd.DataFrame(rows)


def generate_pageviews(
    rng: random.Random, customers: pd.DataFrame, end_utc: datetime, days: int, rows_per_day: int
) -> dict[str, pd.DataFrame]:
    """Return mapping YYYY-MM-DD -> pageviews for that day (UTC)."""
    days_list = _dt_range_days(end_utc, days)
    customer_ids = customers["customer_id"].tolist()

    out: dict[str, pd.DataFrame] = {}
    event_id = 1
    for day_start in days_list:
        day_end = day_start + timedelta(days=1)
        rows = []
        for _ in range(rows_per_day):
            seconds = rng.randint(0, int((day_end - day_start).total_seconds()) - 1)
            ts = day_start + timedelta(seconds=seconds)
            known_user = rng.random() < 0.35
            customer_id = int(rng.choice(customer_ids)) if known_user else None
            path = rng.choice(URLS)
            ref = rng.choices(REFERRERS, weights=[0.45, 0.08, 0.10, 0.08, 0.06, 0.23])[0]
            ua = rng.choice(["chrome", "safari", "firefox", "edge", "mobile"])
            utm_source = None if ref == "direct" else ref
            utm_medium = None if utm_source is None else rng.choice(["cpc", "organic", "social", "email"])
            rows.append(
                {
                    "event_id": event_id,
                    "event_ts_utc": ts,
                    "customer_id": customer_id,
                    "url_path": path,
                    "referrer": ref,
                    "user_agent": ua,
                    "utm_source": utm_source,
                    "utm_medium": utm_medium,
                }
            )
            event_id += 1
        key = day_start.date().isoformat()
        out[key] = pd.DataFrame(rows)

    return out


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate deterministic synthetic data for the local lakehouse demo.")
    ap.add_argument("--out", required=True, help="Output directory for raw files (bronze).")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--customers", type=int, default=500)
    ap.add_argument("--products", type=int, default=100)
    ap.add_argument("--orders", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    out = Path(args.out)
    rng = random.Random(args.seed)
    end_utc = datetime.now(tz=timezone.utc)

    customers = generate_customers(rng, args.customers, end_utc=end_utc, days=args.days)
    products = generate_products(rng, args.products)
    orders = generate_orders(rng, customers, args.orders, end_utc=end_utc, days=args.days)
    order_items = generate_order_items(rng, orders, products)

    rows_per_day = max(500, int(args.customers * 3))
    pageviews_by_day = generate_pageviews(rng, customers, end_utc=end_utc, days=args.days, rows_per_day=rows_per_day)

    write_parquet(customers, out / "customers.parquet")
    write_parquet(products, out / "products.parquet")
    write_parquet(orders, out / "orders.parquet")
    write_parquet(order_items, out / "order_items.parquet")

    pv_dir = out / "pageviews"
    for day, df in pageviews_by_day.items():
        write_parquet(df, pv_dir / f"pageviews_{day}.parquet")

    print("Wrote raw data:")
    print(f"  {out / 'customers.parquet'} ({len(customers):,} rows)")
    print(f"  {out / 'products.parquet'} ({len(products):,} rows)")
    print(f"  {out / 'orders.parquet'} ({len(orders):,} rows)")
    print(f"  {out / 'order_items.parquet'} ({len(order_items):,} rows)")
    print(f"  {pv_dir} ({sum(len(df) for df in pageviews_by_day.values()):,} rows across {len(pageviews_by_day)} files)")


if __name__ == "__main__":
    main()

