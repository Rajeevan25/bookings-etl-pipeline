"""
generate_dataset.py
--------------------
Generates a realistic raw dataset (10,000+ records) for a Travel Bookings
business domain. Intentionally introduces dirty/inconsistent data to simulate
a real-world raw data source.

Usage:
    python generate_dataset.py
Output:
    data/raw/travel_bookings_raw.csv
"""
from __future__ import annotations
from typing import Optional

import os
import csv
import random
import string
from datetime import datetime, timedelta

# ── reproducibility ──────────────────────────────────────────────────────────
random.seed(42)

# ── constants ────────────────────────────────────────────────────────────────
NUM_RECORDS = 12_000
OUTPUT_DIR  = os.path.join("data", "raw")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "travel_bookings_raw.csv")

CATEGORIES = [
    "Hotel", "Resort", "Hostel", "Villa", "Apartment",
    "Boutique Hotel", "Budget Hotel", "Luxury Hotel", "HOTEL", "hotel",  # dirty casing
]

COUNTRIES = [
    "UAE", "United Arab Emirates", "uae",           # intentional duplicates / variants
    "Saudi Arabia", "KSA",
    "France", "france", "FRANCE",
    "Italy", "Spain", "United States", "USA", "US",
    "United Kingdom", "UK", "Germany", "Japan",
    "Australia", "India", "Thailand", "Singapore",
]

STATUSES = ["confirmed", "CONFIRMED", "Confirmed", "pending", "Pending",
            "cancelled", "Cancelled", "CANCELLED", "refunded"]

PAYMENT_METHODS = ["credit_card", "Credit Card", "CREDIT_CARD",
                   "debit_card", "PayPal", "paypal", "bank_transfer",
                   "Bank Transfer", "crypto", None]

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer",
    "Michael", "Linda", "William", "Barbara", "David", "Susan",
    "Richard", "Jessica", "Joseph", "Sarah", "Thomas", "Karen",
    "Charles", "Lisa", "Christopher", "Nancy", "Daniel", "Betty",
    "Matthew", "Margaret", "Anthony", "Sandra", "Mark", "Ashley",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
    "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
    "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
]

HOTEL_PREFIXES = ["Grand", "Royal", "Azure", "Golden", "Sunset", "Palm",
                  "Desert", "Marina", "Coral", "Silver", "Oasis", "Summit"]
HOTEL_SUFFIXES = ["Hotel", "Resort", "Inn", "Suites", "Palace", "Lodge",
                  "Retreat", "Villas", "Residences", "Spa & Resort"]


def random_date(start_year=2022, end_year=2025) -> str:
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    delta = end - start
    rand_days = random.randint(0, delta.days)
    dt = start + timedelta(days=rand_days)

    # Introduce format inconsistencies (~15 % of records)
    fmt_choice = random.random()
    if fmt_choice < 0.10:
        return dt.strftime("%d/%m/%Y")          # DD/MM/YYYY
    elif fmt_choice < 0.15:
        return dt.strftime("%m-%d-%Y")          # MM-DD-YYYY
    else:
        return dt.strftime("%Y-%m-%d")          # ISO (majority)


def random_price() -> Optional[str]:
    if random.random() < 0.03:
        return None                             # missing
    price = round(random.uniform(50, 15_000), 2)
    if random.random() < 0.10:
        return f"${price}"                     # dirty: currency symbol in field
    if random.random() < 0.05:
        return str(price).replace(".", ",")    # dirty: European decimal
    return str(price)


def random_rating() -> Optional[str]:
    if random.random() < 0.05:
        return None                            # missing
    rating = round(random.uniform(1.0, 5.0), 1)
    if random.random() < 0.08:
        return str(rating * 2)                 # dirty: out-of-range (scale 10)
    return str(rating)


def random_nights() -> Optional[str]:
    if random.random() < 0.02:
        return None
    n = random.randint(1, 30)
    if random.random() < 0.05:
        return str(-n)                         # dirty: negative nights
    return str(n)


def random_name() -> str:
    prefix = random.choice(HOTEL_PREFIXES)
    suffix = random.choice(HOTEL_SUFFIXES)
    city   = random.choice(["Dubai", "Paris", "Rome", "London", "Tokyo",
                             "Sydney", "Riyadh", "New York", "Singapore",
                             "Barcelona", "Munich", "Mumbai"])
    return f"{prefix} {city} {suffix}"


def random_customer_name() -> Optional[str]:
    if random.random() < 0.04:
        return None                            # missing
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    fmt = random.random()
    if fmt < 0.15:
        return f"{ln}, {fn}".upper()          # dirty: LAST, FIRST uppercase
    elif fmt < 0.25:
        return f"{fn.lower()} {ln.lower()}"   # dirty: all lowercase
    return f"{fn} {ln}"


def random_email(customer_name: Optional[str]) -> Optional[str]:
    if random.random() < 0.06:
        return None                            # missing
    if not customer_name:
        customer_name = "user"
    base = customer_name.lower().replace(" ", ".").replace(",", "")
    base = "".join(c for c in base if c.isalnum() or c == ".")
    rand_suffix = "".join(random.choices(string.digits, k=3))
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "mail.com"]
    email = f"{base}{rand_suffix}@{random.choice(domains)}"
    if random.random() < 0.08:
        email = email.replace("@", " at ")    # dirty: invalid format
    return email


def random_phone() -> Optional[str]:
    if random.random() < 0.07:
        return None
    digits = "".join(random.choices(string.digits, k=9))
    formats = [
        f"+971-{digits[:2]}-{digits[2:9]}",
        f"00971{digits}",
        f"(+{random.randint(1,99)}) {digits[:3]}-{digits[3:]}",
        digits,                                # dirty: plain digits no format
    ]
    return random.choice(formats)


# ── main generation loop ─────────────────────────────────────────────────────
def generate():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    fieldnames = [
        "booking_id", "customer_name", "email", "phone",
        "hotel_name", "category", "country",
        "check_in_date", "check_out_date", "nights",
        "price_per_night", "total_price",
        "rating", "status", "payment_method", "created_date",
    ]

    # Track some IDs to intentionally duplicate later
    duplicate_pool: list[dict] = []

    rows: list[dict] = []
    for i in range(1, NUM_RECORDS + 1):
        customer = random_customer_name()
        check_in  = random_date(2022, 2024)
        nights_val = random.randint(1, 30)

        try:
            ci_dt = datetime.strptime(check_in, "%Y-%m-%d")
        except ValueError:
            try:
                ci_dt = datetime.strptime(check_in, "%d/%m/%Y")
            except ValueError:
                ci_dt = datetime.strptime(check_in, "%m-%d-%Y")

        co_dt     = ci_dt + timedelta(days=nights_val)
        check_out = co_dt.strftime("%Y-%m-%d")
        pricepn   = round(random.uniform(50, 2_000), 2)
        total     = round(pricepn * nights_val, 2)

        row = {
            "booking_id":     f"BK{i:06d}",
            "customer_name":  customer,
            "email":          random_email(customer),
            "phone":          random_phone(),
            "hotel_name":     random_name(),
            "category":       random.choice(CATEGORIES),
            "country":        random.choice(COUNTRIES),
            "check_in_date":  check_in,
            "check_out_date": check_out,
            "nights":         random_nights() or str(nights_val),
            "price_per_night": random_price() or str(pricepn),
            "total_price":    str(total) if random.random() > 0.03 else None,
            "rating":         random_rating(),
            "status":         random.choice(STATUSES),
            "payment_method": random.choice(PAYMENT_METHODS),
            "created_date":   random_date(2021, 2024),
        }

        rows.append(row)

        # Collect candidates for duplication
        if i % 200 == 0:
            duplicate_pool.append(row.copy())

    # Insert ~300 duplicates
    for _ in range(300):
        dup = random.choice(duplicate_pool).copy()
        # Slightly mutate some to make near-duplicates
        if random.random() < 0.5:
            dup["customer_name"] = (dup["customer_name"] or "").upper()
        rows.append(dup)

    random.shuffle(rows)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] Dataset generated: {OUTPUT_FILE}  ({len(rows):,} records)")


if __name__ == "__main__":
    generate()
