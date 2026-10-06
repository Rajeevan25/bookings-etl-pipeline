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

    # ── Step 10: Validate constraints (Vectorized) ────────────────────────────
    logger.info("Running vectorized validation …")
    
    m_missing_booking_id = df["booking_id"].isna()
    m_missing_customer = df["customer_name"].isna()
    
    ppn = pd.to_numeric(df["price_per_night"], errors="coerce")
    m_invalid_ppn = ppn.isna() | (ppn <= 0)
    
    nights = pd.to_numeric(df["nights"], errors="coerce")
    m_invalid_nights = nights.isna() | (nights <= 0)
    
    rating = pd.to_numeric(df["rating"], errors="coerce")
    m_invalid_rating = rating.notna() & ((rating < 1) | (rating > 5))
    
    m_unparseable_ci = df["check_in_date"].isna()
    
    ci = pd.to_datetime(df["check_in_date"], errors="coerce")
    co = pd.to_datetime(df["check_out_date"], errors="coerce")
    m_invalid_dates = ci.notna() & co.notna() & (co <= ci)
    
    m_unknown_cat = df["category"].isna()
    m_unknown_country = df["country"].isna()
    m_unknown_status = df["status"].isna()

    reasons = pd.Series("", index=df.index)
    conditions = [
        (m_missing_booking_id, "missing booking_id; "),
        (m_missing_customer, "missing customer_name; "),
        (m_invalid_ppn, "invalid price_per_night; "),
        (m_invalid_nights, "invalid nights; "),
        (m_invalid_rating, "rating out of range; "),
        (m_unparseable_ci, "unparseable check_in_date; "),
        (m_invalid_dates, "check_out_date not after check_in_date; "),
        (m_unknown_cat, "unknown/missing category; "),
        (m_unknown_country, "unknown/missing country; "),
        (m_unknown_status, "unknown/missing status; ")
    ]
    
    for mask, msg in conditions:
        reasons = np.where(mask, reasons + msg, reasons)
        
    reasons = pd.Series(reasons, index=df.index).str.strip("; ")
    reasons.replace("", None, inplace=True)
    
    mask_valid = reasons.isna()
    mask_invalid = ~mask_valid

    rejected_df = original_df[mask_invalid].copy()
    rejected_df["rejection_reason"] = reasons[mask_invalid].values
    df = df[mask_valid].copy()

    logger.info(
        "Validation complete: %d valid, %d rejected",
        len(df), len(rejected_df)
    )

    # ── Step 11: Remove exact duplicates (by booking_id) ─────────────────────
    dupe_mask = df.duplicated(subset=["booking_id"], keep="first")
    if dupe_mask.any():
        dupes_df = original_df.loc[df[dupe_mask].index].copy()
        dupes_df["rejection_reason"] = "duplicate booking_id"
        rejected_df = pd.concat([rejected_df, dupes_df], ignore_index=True)
        df = df[~dupe_mask].copy()

    dupes_removed = dupe_mask.sum()
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
    df["price_per_night"] = pd.to_numeric(df["price_per_night"], errors="coerce").round(2)
    df["total_price"]    = pd.to_numeric(df["total_price"], errors="coerce").round(2)
    df["rating"]         = pd.to_numeric(df["rating"], errors="coerce").round(1)

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
