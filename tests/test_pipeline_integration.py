"""
tests/test_pipeline_integration.py
-----------------------------------
Integration-level tests for the ETL pipeline.

Covers:
  - Row count reconciliation (extract → transform → clean + rejected)
  - Multi-row dirty data scenarios
  - Duplicate routing to rejection trail
  - Total price reconciliation after clamping
"""

import pandas as pd
import numpy as np
import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from etl.transform import transform


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_valid_row(booking_id="BK000001", **overrides):
    """Return a dict for a single valid raw record."""
    base = {
        "booking_id":      booking_id,
        "customer_name":   "john smith",
        "email":           "john.smith@example.com",
        "phone":           "+971-50-1234567",
        "hotel_name":      "Grand Dubai Hotel",
        "category":        "Hotel",
        "country":         "UAE",
        "check_in_date":   "2024-01-15",
        "check_out_date":  "2024-01-20",
        "nights":          "5",
        "price_per_night": "200.00",
        "total_price":     "1000.00",
        "rating":          "4.5",
        "status":          "Confirmed",
        "payment_method":  "credit_card",
        "created_date":    "2023-12-01",
    }
    base.update(overrides)
    return base


def _make_multi_df(rows):
    """Build a DataFrame from a list of row dicts."""
    return pd.DataFrame(rows)


# ── Reconciliation Tests ─────────────────────────────────────────────────────

class TestRowCountReconciliation:
    """Verify that input rows == clean rows + rejected rows (no silent drops)."""

    def test_all_valid_rows_reconcile(self):
        rows = [_make_valid_row(f"BK{i:06d}") for i in range(1, 51)]
        df = _make_multi_df(rows)
        clean, rejected = transform(df)
        assert len(clean) + len(rejected) == len(df)

    def test_mixed_valid_and_invalid_rows_reconcile(self):
        rows = [_make_valid_row(f"BK{i:06d}") for i in range(1, 21)]
        # Inject 5 invalid rows (missing booking_id)
        for i in range(21, 26):
            rows.append(_make_valid_row(f"BK{i:06d}", price_per_night="-50"))
        df = _make_multi_df(rows)
        clean, rejected = transform(df)
        assert len(clean) + len(rejected) == len(df)

    def test_all_invalid_rows_reconcile(self):
        rows = [_make_valid_row(f"BK{i:06d}", category="INVALID") for i in range(1, 11)]
        df = _make_multi_df(rows)
        clean, rejected = transform(df)
        assert len(clean) == 0
        assert len(rejected) == 10


# ── Duplicate Routing Tests ──────────────────────────────────────────────────

class TestDuplicateRouting:
    """Verify that duplicates are logged to rejection trail, not silently dropped."""

    def test_duplicates_appear_in_rejected(self):
        rows = [
            _make_valid_row("BK000001"),
            _make_valid_row("BK000001"),
            _make_valid_row("BK000002"),
        ]
        df = _make_multi_df(rows)
        clean, rejected = transform(df)
        assert len(clean) == 2
        assert len(rejected) == 1
        assert "duplicate" in rejected.iloc[0]["rejection_reason"].lower()

    def test_triple_duplicate_routes_two_to_rejected(self):
        rows = [
            _make_valid_row("BK000001"),
            _make_valid_row("BK000001"),
            _make_valid_row("BK000001"),
        ]
        df = _make_multi_df(rows)
        clean, rejected = transform(df)
        assert len(clean) == 1
        assert len(rejected) == 2

    def test_duplicates_plus_invalid_all_reconcile(self):
        rows = [
            _make_valid_row("BK000001"),
            _make_valid_row("BK000001"),               # dup
            _make_valid_row("BK000002", nights="-1"),   # invalid
            _make_valid_row("BK000003"),
        ]
        df = _make_multi_df(rows)
        clean, rejected = transform(df)
        # 1 dup + 1 invalid in rejected, 2 valid in clean
        assert len(clean) + len(rejected) == len(df)
        assert len(clean) == 2
        assert len(rejected) == 2


# ── Multi-Row Dirty Data Tests ───────────────────────────────────────────────

class TestMultiRowDirtyData:
    """Simulate realistic batches with multiple types of data quality issues."""

    def test_batch_with_mixed_issues(self):
        rows = [
            _make_valid_row("BK000001"),                                     # valid
            _make_valid_row("BK000002", rating="9.5"),                       # bad rating
            _make_valid_row("BK000003", country="Narnia"),                   # bad country
            _make_valid_row("BK000004", check_in_date="invalid-date"),       # bad date
            _make_valid_row("BK000005", status="UNKNOWN_STATUS"),            # bad status
            _make_valid_row("BK000006"),                                     # valid
        ]
        df = _make_multi_df(rows)
        clean, rejected = transform(df)
        assert len(clean) == 2   # BK000001, BK000006
        assert len(rejected) == 4
        # Verify each rejection has a reason
        for _, row in rejected.iterrows():
            assert row["rejection_reason"] is not None
            assert len(row["rejection_reason"]) > 0

    def test_multiple_reasons_per_row(self):
        """A row with multiple issues should list all reasons."""
        rows = [
            _make_valid_row(
                "BK000001",
                price_per_night="-100",
                nights="-3",
                rating="9.0",
            ),
        ]
        df = _make_multi_df(rows)
        _, rejected = transform(df)
        assert len(rejected) == 1
        reason = rejected.iloc[0]["rejection_reason"]
        assert "price_per_night" in reason
        assert "nights" in reason
        assert "rating" in reason


# ── Total Price Reconciliation ───────────────────────────────────────────────

class TestTotalPriceReconciliation:
    """Verify the total_price clamping logic works correctly."""

    def test_missing_total_price_is_derived(self):
        df = _make_multi_df([_make_valid_row("BK000001", total_price="")])
        clean, _ = transform(df)
        assert len(clean) == 1
        expected = 200.00 * 5  # price_per_night * nights
        assert clean.iloc[0]["total_price"] == expected

    def test_discrepant_total_price_is_clamped(self):
        """If total_price deviates >5% from recalculated, it gets clamped."""
        # price_per_night=200, nights=5 → expected=1000
        # 1200 is 20% off → should be clamped to 1000
        df = _make_multi_df([_make_valid_row("BK000001", total_price="1200.00")])
        clean, _ = transform(df)
        assert len(clean) == 1
        assert clean.iloc[0]["total_price"] == 1000.00

    def test_acceptable_total_price_is_kept(self):
        """If total_price is within 5%, it stays as-is."""
        # price_per_night=200, nights=5 → expected=1000
        # 1020 is 2% off → within tolerance
        df = _make_multi_df([_make_valid_row("BK000001", total_price="1020.00")])
        clean, _ = transform(df)
        assert len(clean) == 1
        assert clean.iloc[0]["total_price"] == 1020.00


# ── Rejection Reason Column Test ─────────────────────────────────────────────

class TestRejectionReasonColumn:
    """Verify the rejection_reason column is always present on rejected_df."""

    def test_rejected_df_has_reason_column(self):
        rows = [
            _make_valid_row("BK000001", category="INVALID"),
        ]
        df = _make_multi_df(rows)
        _, rejected = transform(df)
        assert "rejection_reason" in rejected.columns

    def test_empty_rejected_df_still_has_reason_column(self):
        rows = [_make_valid_row("BK000001")]
        df = _make_multi_df(rows)
        _, rejected = transform(df)
        assert "rejection_reason" in rejected.columns
