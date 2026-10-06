"""
tests/test_transform.py
-----------------------
Unit tests for the ETL transform layer.

Run with:
    pytest tests/ -v
"""

import pandas as pd
import numpy as np
import pytest
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etl.transform import transform


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_df(**overrides) -> pd.DataFrame:
    """Create a single valid raw row with optional overrides."""
    base = {
        "booking_id":      "BK000001",
        "customer_name":   "john smith",
        "email":           "john.smith001@gmail.com",
        "phone":           "+971-50-1234567",
        "hotel_name":      "Grand Dubai Hotel",
        "category":        "HOTEL",
        "country":         "uae",
        "check_in_date":   "2024-01-15",
        "check_out_date":  "2024-01-20",
        "nights":          "5",
        "price_per_night": "$200.00",
        "total_price":     "1000.00",
        "rating":          "4.5",
        "status":          "CONFIRMED",
        "payment_method":  "credit_card",
        "created_date":    "2023-12-01",
    }
    base.update(overrides)
    return pd.DataFrame([base])


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestCategoryStandardization:
    def test_uppercase_maps_to_title(self):
        df = _make_df(category="HOTEL")
        clean, _ = transform(df)
        assert clean.iloc[0]["category"] == "Hotel"

    def test_lowercase_maps_to_title(self):
        df = _make_df(category="hotel")
        clean, _ = transform(df)
        assert clean.iloc[0]["category"] == "Hotel"

    def test_unknown_category_is_rejected(self):
        df = _make_df(category="unknown_type")
        clean, rejected = transform(df)
        assert len(clean) == 0
        assert len(rejected) == 1
        assert "category" in rejected.iloc[0]["rejection_reason"].lower()


class TestCountryStandardization:
    def test_uae_maps_to_full_name(self):
        for raw in ["UAE", "uae", "United Arab Emirates"]:
            df = _make_df(country=raw)
            clean, _ = transform(df)
            assert clean.iloc[0]["country"] == "United Arab Emirates", f"Failed for: {raw}"

    def test_usa_abbreviations(self):
        for raw in ["USA", "US", "United States"]:
            df = _make_df(country=raw)
            clean, _ = transform(df)
            assert clean.iloc[0]["country"] == "United States"

    def test_unknown_country_is_rejected(self):
        df = _make_df(country="Narnia")
        clean, rejected = transform(df)
        assert len(clean) == 0
        assert len(rejected) == 1


class TestPriceCleaning:
    def test_dollar_sign_stripped(self):
        df = _make_df(price_per_night="$200.50", total_price="1002.50")
        clean, _ = transform(df)
        assert clean.iloc[0]["price_per_night"] == 200.50

    def test_european_comma_decimal(self):
        df = _make_df(price_per_night="200,50", total_price="1002,50")
        clean, _ = transform(df)
        # 200,50 → 200.50
        assert clean.iloc[0]["price_per_night"] == 200.50

    def test_negative_price_is_rejected(self):
        df = _make_df(price_per_night="-100")
        clean, rejected = transform(df)
        assert len(clean) == 0
        assert "price" in rejected.iloc[0]["rejection_reason"].lower()


class TestDateParsing:
    def test_iso_date_parsed(self):
        df = _make_df(check_in_date="2024-06-15", check_out_date="2024-06-20")
        clean, _ = transform(df)
        assert clean.iloc[0]["check_in_date"].strftime("%Y-%m-%d") == "2024-06-15"

    def test_dmy_slash_format_parsed(self):
        df = _make_df(check_in_date="15/06/2024", check_out_date="2024-06-20")
        clean, _ = transform(df)
        assert clean.iloc[0]["check_in_date"].strftime("%Y-%m-%d") == "2024-06-15"

    def test_checkout_before_checkin_rejected(self):
        df = _make_df(check_in_date="2024-06-20", check_out_date="2024-06-15")
        clean, rejected = transform(df)
        assert len(clean) == 0
        assert "check_out" in rejected.iloc[0]["rejection_reason"].lower()


class TestRatingValidation:
    def test_valid_rating_passes(self):
        df = _make_df(rating="4.5")
        clean, _ = transform(df)
        assert clean.iloc[0]["rating"] == 4.5

    def test_out_of_range_rating_rejected(self):
        # Dataset had ×2 scale bug (9.0 instead of 4.5)
        df = _make_df(rating="9.0")
        clean, rejected = transform(df)
        assert len(clean) == 0
        assert "rating" in rejected.iloc[0]["rejection_reason"].lower()

    def test_missing_rating_is_allowed(self):
        df = _make_df(rating="")
        clean, _ = transform(df)
        assert len(clean) == 1
        assert pd.isna(clean.iloc[0]["rating"])


class TestDeduplication:
    def test_duplicate_booking_ids_deduplicated(self):
        row = _make_df()
        dup = _make_df()
        combined = pd.concat([row, dup], ignore_index=True)
        clean, _ = transform(combined)
        assert len(clean) == 1


class TestNightsValidation:
    def test_negative_nights_rejected(self):
        df = _make_df(nights="-3")
        clean, rejected = transform(df)
        assert len(clean) == 0
        assert "nights" in rejected.iloc[0]["rejection_reason"].lower()

    def test_positive_nights_passes(self):
        df = _make_df(nights="7", total_price="1400.00")
        clean, _ = transform(df)
        assert clean.iloc[0]["nights"] == 7


class TestCustomerNameStandardization:
    def test_name_title_cased(self):
        df = _make_df(customer_name="john smith")
        clean, _ = transform(df)
        assert clean.iloc[0]["customer_name"] == "John Smith"

    def test_uppercase_name_title_cased(self):
        df = _make_df(customer_name="JOHN SMITH")
        clean, _ = transform(df)
        assert clean.iloc[0]["customer_name"] == "John Smith"


class TestEmailCleaning:
    def test_email_at_replaced(self):
        df = _make_df(email="john.smith001 at gmail.com")
        clean, _ = transform(df)
        assert "@" in clean.iloc[0]["email"]
        assert " at " not in clean.iloc[0]["email"]
