"""
DataPulse — Synthetic E-Commerce Data Generator
================================================
Generates realistic 2-year retail dataset:
  - 1,000 customers
  - 300 products across 10 categories
  - 8,000+ orders with line items
  - Clickstream/event data

Writes CSV files to data/raw/ for Bronze layer ingestion.
"""

import csv
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

fake = Faker("en_US")
random.seed(42)
np.random.seed(42)

# ── Config ─────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path(os.getenv("DATA_OUTPUT_DIR", "./data/raw"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", 1000))
NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", 300))
NUM_ORDERS = int(os.getenv("NUM_ORDERS", 8000))
DATE_RANGE_DAYS = int(os.getenv("DATE_RANGE_DAYS", 730))

END_DATE = datetime(2024, 12, 31)
START_DATE = END_DATE - timedelta(days=DATE_RANGE_DAYS)

# ── Product catalog ─────────────────────────────────────────────────────────
CATALOG = {
    "Electronics": {
        "subcategories": ["Smartphones", "Laptops", "Tablets", "Accessories", "Audio"],
        "brands": ["TechPro", "NovaStar", "ZephyrTech", "PulseWave", "CoreEdge"],
        "price_range": (49.99, 1499.99),
    },
    "Clothing": {
        "subcategories": ["Men's", "Women's", "Kids'", "Sportswear", "Outerwear"],
        "brands": ["UrbanThread", "StyleVault", "ActiveGear", "PrimeFit", "TrendSet"],
        "price_range": (14.99, 299.99),
    },
    "Home & Kitchen": {
        "subcategories": ["Cookware", "Furniture", "Decor", "Appliances", "Bedding"],
        "brands": ["HomeEssentials", "CraftHaven", "LivingPlus", "NestWell", "ArtisanHome"],
        "price_range": (9.99, 599.99),
    },
    "Books": {
        "subcategories": ["Fiction", "Non-Fiction", "Technical", "Self-Help", "Children's"],
        "brands": ["PageTurner", "WisdomPress", "StoryBound", "LitWorld", "ReadMore"],
        "price_range": (6.99, 59.99),
    },
    "Sports & Outdoors": {
        "subcategories": ["Fitness", "Camping", "Cycling", "Team Sports", "Water Sports"],
        "brands": ["ApexSport", "TrailBlaze", "PowerMove", "OutdoorEdge", "SportPeak"],
        "price_range": (19.99, 499.99),
    },
    "Beauty & Health": {
        "subcategories": ["Skincare", "Haircare", "Supplements", "Makeup", "Wellness"],
        "brands": ["GlowUp", "PureEssence", "VitalLife", "NaturalBliss", "LuxeBeauty"],
        "price_range": (8.99, 149.99),
    },
    "Toys & Games": {
        "subcategories": ["Board Games", "Action Figures", "Educational", "Outdoor Toys", "Video Games"],
        "brands": ["PlayMasters", "FunForge", "ToyVault", "GameBurst", "KidZone"],
        "price_range": (9.99, 199.99),
    },
    "Food & Grocery": {
        "subcategories": ["Snacks", "Beverages", "Organic", "Pantry", "International"],
        "brands": ["FreshFarm", "NaturalBite", "GourmetSelect", "EcoHarvest", "PureOrganic"],
        "price_range": (3.99, 89.99),
    },
    "Automotive": {
        "subcategories": ["Car Accessories", "Tools", "Parts", "GPS & Electronics", "Cleaning"],
        "brands": ["AutoElite", "DriveMax", "TurboFit", "RoadMaster", "CarCare"],
        "price_range": (12.99, 399.99),
    },
    "Office Supplies": {
        "subcategories": ["Stationery", "Furniture", "Tech Accessories", "Storage", "Printing"],
        "brands": ["WorkSmarter", "DeskPro", "OfficeFirst", "PaperMate", "OfficePlus"],
        "price_range": (4.99, 349.99),
    },
}

US_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
]
# Weight larger states higher (population-based)
STATE_WEIGHTS = [
    14, 9, 7, 7, 5, 5, 4, 4, 4, 4,
    4, 3, 3, 3, 3, 3, 3, 3, 3, 3,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
]

ORDER_STATUSES = ["completed", "completed", "completed", "completed",
                  "shipped", "shipped", "processing", "cancelled", "returned"]
SHIPPING_METHODS = ["Standard", "Standard", "Standard", "Express", "Express", "Overnight"]
DISCOUNT_CODES = ["SAVE10", "SUMMER20", "WELCOME15", "VIP25", "FLASH30", None, None, None, None, None]


def generate_customers(n: int) -> list[dict]:
    """Generate realistic customer profiles with B2B/B2C segmentation."""
    customers = []
    for i in range(1, n + 1):
        state = random.choices(US_STATES, weights=STATE_WEIGHTS)[0]
        segment = random.choices(["B2C", "B2B"], weights=[75, 25])[0]

        # B2B customers tend to register earlier and have higher value
        reg_offset = random.randint(0, DATE_RANGE_DAYS)
        reg_date = START_DATE + timedelta(days=reg_offset)

        customers.append({
            "customer_id": f"CUST-{i:05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "city": fake.city(),
            "state": state,
            "country": "US",
            "zip_code": fake.zipcode_in_state(state_abbr=state),
            "customer_segment": segment,
            "registration_date": reg_date.strftime("%Y-%m-%d"),
        })
    print(f"  ✓ Generated {n} customers")
    return customers


def generate_products(n: int) -> list[dict]:
    """Generate product catalog with realistic pricing and margins."""
    products = []
    categories = list(CATALOG.keys())
    product_counter = 1

    per_category = n // len(categories)
    remainder = n % len(categories)

    for i, category in enumerate(categories):
        count = per_category + (1 if i < remainder else 0)
        meta = CATALOG[category]
        lo, hi = meta["price_range"]

        for _ in range(count):
            subcategory = random.choice(meta["subcategories"])
            brand = random.choice(meta["brands"])
            selling_price = round(random.uniform(lo, hi), 2)
            # Cost is 35–60% of selling price (realistic margin)
            cost_price = round(selling_price * random.uniform(0.35, 0.60), 2)

            products.append({
                "product_id": f"PROD-{product_counter:05d}",
                "product_name": f"{brand} {subcategory} {fake.word().capitalize()} {random.randint(100, 999)}",
                "category": category,
                "subcategory": subcategory,
                "brand": brand,
                "cost_price": cost_price,
                "selling_price": selling_price,
                "stock_quantity": random.randint(0, 500),
                "sku": f"{category[:3].upper()}-{brand[:3].upper()}-{product_counter:04d}",
            })
            product_counter += 1

    print(f"  ✓ Generated {len(products)} products")
    return products


def generate_orders_and_items(
    customers: list[dict],
    products: list[dict],
    n_orders: int,
) -> tuple[list[dict], list[dict]]:
    """
    Generate orders with realistic patterns:
    - Seasonal peaks (holiday season)
    - Repeat customers (power-law distribution)
    - B2B customers buy in higher quantities
    """
    orders = []
    order_items = []
    item_counter = 1

    # Power-law customer distribution: some customers order a lot
    customer_ids = [c["customer_id"] for c in customers]
    customer_weights = np.random.zipf(1.5, len(customers))
    customer_weights = customer_weights / customer_weights.sum()

    # Map customer_id → segment for B2B logic
    segment_map = {c["customer_id"]: c["customer_segment"] for c in customers}

    product_ids = [p["product_id"] for p in products]
    product_prices = {p["product_id"]: p["selling_price"] for p in products}
    product_categories = {p["product_id"]: p["category"] for p in products}

    # Category affinity weights per product (popular categories sell more)
    category_popularity = {
        "Electronics": 3.0, "Clothing": 2.5, "Home & Kitchen": 2.0,
        "Beauty & Health": 1.8, "Books": 1.5, "Sports & Outdoors": 1.4,
        "Toys & Games": 1.2, "Food & Grocery": 1.0, "Automotive": 0.8, "Office Supplies": 0.7,
    }
    product_weights = np.array([category_popularity[product_categories[pid]] for pid in product_ids])
    product_weights = product_weights / product_weights.sum()

    for i in range(1, n_orders + 1):
        order_id = f"ORD-{i:07d}"
        customer_id = np.random.choice(customer_ids, p=customer_weights)
        segment = segment_map[customer_id]

        # Seasonal distribution: peaks in November-December
        day_offset = _seasonal_random_day(DATE_RANGE_DAYS)
        order_date = START_DATE + timedelta(days=day_offset)

        # Ships 1-7 days after order (cancelled/returned may not ship)
        status = random.choice(ORDER_STATUSES)
        ship_date = None
        if status not in ("cancelled",):
            ship_days = random.randint(1, 7)
            ship_date = order_date + timedelta(days=ship_days)

        shipping_method = random.choice(SHIPPING_METHODS)
        discount_code = random.choice(DISCOUNT_CODES)

        # Generate 1-5 items per order (B2B skews higher qty)
        n_items = random.choices([1, 2, 3, 4, 5], weights=[40, 25, 20, 10, 5])[0]
        selected_products = np.random.choice(product_ids, size=n_items, replace=False, p=product_weights)

        total_amount = 0.0
        for product_id in selected_products:
            quantity = random.randint(1, 3) if segment == "B2C" else random.randint(1, 20)
            unit_price = product_prices[product_id]
            discount_pct = _get_discount_pct(discount_code)
            line_total = round(quantity * unit_price * (1 - discount_pct / 100), 2)
            total_amount += line_total

            order_items.append({
                "item_id": f"ITEM-{item_counter:08d}",
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_pct": discount_pct,
            })
            item_counter += 1

        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date.strftime("%Y-%m-%d"),
            "ship_date": ship_date.strftime("%Y-%m-%d") if ship_date else "",
            "shipping_method": shipping_method,
            "order_status": status,
            "total_amount": round(total_amount, 2),
            "discount_code": discount_code or "",
        })

    print(f"  ✓ Generated {len(orders)} orders with {len(order_items)} line items")
    return orders, order_items


def generate_events(customers: list[dict], products: list[dict], n_events: int = 30000) -> list[dict]:
    """Generate clickstream events (page views, cart adds, purchases)."""
    events = []
    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]
    event_types = ["page_view", "page_view", "page_view", "add_to_cart", "add_to_cart", "purchase"]
    devices = ["desktop", "desktop", "mobile", "mobile", "tablet"]

    for i in range(1, n_events + 1):
        ts = START_DATE + timedelta(
            days=random.randint(0, DATE_RANGE_DAYS),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        events.append({
            "event_id": f"EVT-{i:08d}",
            "customer_id": random.choice(customer_ids),
            "event_type": random.choice(event_types),
            "product_id": random.choice(product_ids),
            "event_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "session_id": f"SESS-{random.randint(100000, 999999)}",
            "device_type": random.choice(devices),
        })

    print(f"  ✓ Generated {len(events)} clickstream events")
    return events


def _seasonal_random_day(total_days: int) -> int:
    """Returns a day offset with realistic seasonal peaks (holiday boost in Nov-Dec)."""
    # Build a weight array over 2 years where Nov-Dec months are 2.5x likely
    weights = []
    base = START_DATE
    for d in range(total_days):
        month = (base + timedelta(days=d)).month
        weight = 2.5 if month in (11, 12) else (1.5 if month in (6, 7, 8) else 1.0)
        weights.append(weight)
    weights_arr = np.array(weights)
    weights_arr = weights_arr / weights_arr.sum()
    return int(np.random.choice(total_days, p=weights_arr))


def _get_discount_pct(discount_code: str | None) -> float:
    mapping = {"SAVE10": 10.0, "SUMMER20": 20.0, "WELCOME15": 15.0, "VIP25": 25.0, "FLASH30": 30.0}
    return mapping.get(discount_code, 0.0)


def write_csv(data: list[dict], filename: str) -> None:
    path = OUTPUT_DIR / filename
    if not data:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    size_kb = path.stat().st_size / 1024
    print(f"  ✓ Saved {len(data):,} rows → {path} ({size_kb:.1f} KB)")


def main():
    print("=" * 60)
    print("DataPulse — Synthetic Data Generator")
    print(f"Date range: {START_DATE.date()} → {END_DATE.date()}")
    print("=" * 60)

    print("\n[1/4] Generating customers...")
    customers = generate_customers(NUM_CUSTOMERS)
    write_csv(customers, "customers.csv")

    print("\n[2/4] Generating product catalog...")
    products = generate_products(NUM_PRODUCTS)
    write_csv(products, "products.csv")

    print("\n[3/4] Generating orders & line items...")
    orders, order_items = generate_orders_and_items(customers, products, NUM_ORDERS)
    write_csv(orders, "orders.csv")
    write_csv(order_items, "order_items.csv")

    print("\n[4/4] Generating clickstream events...")
    events = generate_events(customers, products)
    write_csv(events, "events.csv")

    # Summary stats
    total_revenue = sum(float(o["total_amount"]) for o in orders if o["order_status"] == "completed")
    print(f"\n{'=' * 60}")
    print("DATA GENERATION SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Customers  : {len(customers):>8,}")
    print(f"  Products   : {len(products):>8,}")
    print(f"  Orders     : {len(orders):>8,}")
    print(f"  Order Items: {len(order_items):>8,}")
    print(f"  Events     : {30000:>8,}")
    print(f"  Total Rev  : ${total_revenue:>11,.2f}  (completed orders)")
    print(f"{'=' * 60}")
    print(f"\n  Output directory: {OUTPUT_DIR.resolve()}")
    print("  ✓ Data generation complete!\n")


if __name__ == "__main__":
    main()
