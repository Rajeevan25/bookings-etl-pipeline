"""
etl/transform.py
----------------
Transformation layer of the ETL pipeline.

Steps performed (in order):
  1.  Strip whitespace from all string columns
  2.  Standardize column names (lowercase, strip)
  3.  Clean & standardize 'category'
  4.  Clean & standardize 'country'
  5.  Clean & standardize 'status'
  6.  Clean & standardize 'payment_method'
  7.  Parse & standardize date columns
  8.  Clean price fields (remove symbols, fix decimal separator)
  9.  Cast numeric columns
  10. Validate constraints  →  reject invalid rows
  11. Deduplicate
  12. Derive 'revenue' (price_per_night × nights) as a cross-check
  13. Final column ordering
"""
from __future__ import annotations

import re
import logging
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ── Mapping tables ────────────────────────────────────────────────────────────

CATEGORY_MAP = {
    "hotel":          "Hotel",
    "resort":         "Resort",
    "hostel":         "Hostel",
    "villa":          "Villa",
    "apartment":      "Apartment",
    "boutique hotel": "Boutique Hotel",
    "budget hotel":   "Budget Hotel",
    "luxury hotel":   "Luxury Hotel",
}

COUNTRY_MAP = {
    "uae":                  "United Arab Emirates",
    "united arab emirates": "United Arab Emirates",
    "ksa":                  "Saudi Arabia",
    "saudi arabia":         "Saudi Arabia",
    "france":               "France",
    "italy":                "Italy",
    "spain":                "Spain",
    "united states":        "United States",
    "usa":                  "United States",
    "us":                   "United States",
    "united kingdom":       "United Kingdom",
    "uk":                   "United Kingdom",
    "germany":              "Germany",
    "japan":                "Japan",
    "australia":            "Australia",
    "india":                "India",
    "thailand":             "Thailand",
    "singapore":            "Singapore",
}

STATUS_MAP = {
    "confirmed":  "Confirmed",
    "pending":    "Pending",
    "cancelled":  "Cancelled",
    "refunded":   "Refunded",
}

PAYMENT_MAP = {
    "credit_card":    "Credit Card",
    "credit card":    "Credit Card",
    "debit_card":     "Debit Card",
    "debit card":     "Debit Card",
    "paypal":         "PayPal",
    "bank_transfer":  "Bank Transfer",
    "bank transfer":  "Bank Transfer",
    "crypto":         "Crypto",
}

DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y"]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_date(value: str) -> datetime | None:
    """Try multiple date formats; return None on failure."""
    if not value or str(value).strip() in ("", "nan", "None"):
        return None
    value = str(value).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _clean_price(value: str) -> float | None:
    """
    Strip currency symbols, fix European decimal separator,
    return float or None.
    """
    if not value or str(value).strip() in ("", "nan", "None"):
        return None
    val = str(value).strip()
    val = val.replace("$", "").replace("€", "").replace("£", "").strip()
    # European decimal: replace comma decimal (but NOT thousands separator)
    # e.g. "1.234,56" → "1234.56"
    if re.fullmatch(r"[\d]+,\d{2}", val):
        val = val.replace(",", ".")
    else:
        val = val.replace(",", "")   # remove thousands commas
    try:
        return float(val)
    except ValueError:
        return None


def _standardize_text_map(series: pd.Series, mapping: dict) -> pd.Series:
    """Lowercase strip and map to canonical value."""
    return series.str.lower().str.strip().map(mapping)


# ── Main transform function ───────────────────────────────────────────────────

def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean and transform raw DataFrame.

    Returns:
        clean_df   : DataFrame with valid, transformed records
        rejected_df: DataFrame with records that failed validation,
                     including a 'rejection_reason' column
    """
    logger.info("Starting transform on %d raw records", len(df))
    rejection_log: list[dict] = []

    # ── Step 1: Normalise column names ────────────────────────────────────────
    df.columns = [c.strip().lower() for c in df.columns]

    # ── Step 2: Strip whitespace from all string columns ─────────────────────
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # Replace empty strings / "nan" strings with actual NaN
    df.replace({"": np.nan, "nan": np.nan, "None": np.nan}, inplace=True)

    original_df = df.copy()   # keep for rejection logging

    # ── Step 3: Standardize category ─────────────────────────────────────────
    df["category"] = _standardize_text_map(
        df["category"].fillna(""), CATEGORY_MAP
    )

    # ── Step 4: Standardize country ──────────────────────────────────────────
    df["country"] = _standardize_text_map(
        df["country"].fillna(""), COUNTRY_MAP
    )

    # ── Step 5: Standardize status ───────────────────────────────────────────
    df["status"] = _standardize_text_map(
        df["status"].fillna(""), STATUS_MAP
    )

    # ── Step 6: Standardize payment_method ───────────────────────────────────
    df["payment_method"] = _standardize_text_map(
        df["payment_method"].fillna(""), PAYMENT_MAP
    )

    # ── Step 7: Parse dates ───────────────────────────────────────────────────
    for date_col in ["check_in_date", "check_out_date", "created_date"]:
        df[date_col] = df[date_col].apply(
            lambda v: _parse_date(str(v)) if pd.notna(v) else None
        )

    # ── Step 8: Clean price fields ────────────────────────────────────────────
    for price_col in ["price_per_night", "total_price"]:
        df[price_col] = df[price_col].apply(
            lambda v: _clean_price(str(v)) if pd.notna(v) else None
        )

    # ── Step 9: Cast numerics ─────────────────────────────────────────────────
    df["nights"] = pd.to_numeric(df["nights"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    # ── Step 10: Validate constraints ─────────────────────────────────────────
    def validate_row(row) -> str | None:
        reasons = []

        # booking_id must exist
        if pd.isna(row.get("booking_id")):
            reasons.append("missing booking_id")

        # customer_name must exist
        if pd.isna(row.get("customer_name")):
            reasons.append("missing customer_name")

        # price_per_night must be positive
        ppn = row.get("price_per_night")
        if pd.isna(ppn) or ppn <= 0:
            reasons.append(f"invalid price_per_night ({ppn})")

        # nights must be positive integer
        nights = row.get("nights")
        if pd.isna(nights) or nights <= 0:
            reasons.append(f"invalid nights ({nights})")

        # rating must be 1-5 (or None)
        rating = row.get("rating")
        if not pd.isna(rating) and (rating < 1 or rating > 5):
            reasons.append(f"rating out of range ({rating})")

        # check_in_date must parse
        if pd.isna(row.get("check_in_date")):
            reasons.append("unparseable check_in_date")

        # check_out > check_in
        ci = row.get("check_in_date")
        co = row.get("check_out_date")
        if ci and co and pd.notna(ci) and pd.notna(co) and co <= ci:
            reasons.append("check_out_date not after check_in_date")

        # category must be known
        if pd.isna(row.get("category")):
            reasons.append("unknown/missing category")

        # country must be known
        if pd.isna(row.get("country")):
            reasons.append("unknown/missing country")

        # status must be known
        if pd.isna(row.get("status")):
            reasons.append("unknown/missing status")

        return "; ".join(reasons) if reasons else None

    logger.info("Running row-level validation …")
    rejection_reasons = df.apply(validate_row, axis=1)
    mask_valid   = rejection_reasons.isna()
    mask_invalid = ~mask_valid

    rejected_df = original_df[mask_invalid].copy()
    rejected_df["rejection_reason"] = rejection_reasons[mask_invalid].values
    df = df[mask_valid].copy()

    logger.info(
        "Validation complete: %d valid, %d rejected",
        len(df), len(rejected_df)
    )

    # ── Step 11: Remove exact duplicates (by booking_id) ─────────────────────
    before_dedup = len(df)
    df.drop_duplicates(subset=["booking_id"], keep="first", inplace=True)
    dupes_removed = before_dedup - len(df)
    logger.info("Removed %d duplicate booking_ids", dupes_removed)

    # ── Step 12: Derive / recalculate total_price where missing ───────────────
    mask_missing_total = df["total_price"].isna()
    df.loc[mask_missing_total, "total_price"] = (
        df.loc[mask_missing_total, "price_per_night"]
        * df.loc[mask_missing_total, "nights"]
    ).round(2)

    # Clamp total_price to recalculated value when discrepancy > 5 %
    recalculated = (df["price_per_night"] * df["nights"]).round(2)
    discrepancy  = ((df["total_price"] - recalculated).abs() / recalculated > 0.05)
    df.loc[discrepancy, "total_price"] = recalculated[discrepancy]

    # ── Step 13: Final column ordering & type casting ─────────────────────────
    df["check_in_date"]  = pd.to_datetime(df["check_in_date"])
    df["check_out_date"] = pd.to_datetime(df["check_out_date"])
    df["created_date"]   = pd.to_datetime(df["created_date"])
    df["nights"]         = df["nights"].astype(int)
    df["price_per_night"] = df["price_per_night"].round(2)
    df["total_price"]    = df["total_price"].round(2)
    df["rating"]         = df["rating"].round(1)

    # Standardize customer_name: Title Case
    df["customer_name"] = df["customer_name"].str.title()

    # Clean email: remove ' at ' → '@'
    df["email"] = df["email"].str.replace(r"\s+at\s+", "@", regex=True)

    FINAL_COLS = [
        "booking_id", "customer_name", "email", "phone",
        "hotel_name", "category", "country",
        "check_in_date", "check_out_date", "nights",
        "price_per_night", "total_price",
        "rating", "status", "payment_method", "created_date",
    ]
    df = df[FINAL_COLS]

    logger.info("Transform complete. Clean records: %d", len(df))
    return df, rejected_df
