"""
Tests for the historical backfill pipeline.

Covers:
  - Sample CSV generation
  - Gold-layer FY calendar utilities
  - BackfillOrchestrator batch processing
  - Revenue computation logic
"""

import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

from src.transforms.backfill import (
    generate_sample_csv,
    BackfillOrchestrator,
    SAMPLE_OEM_PROFILES,
    SEASONAL_FACTORS,
)
from src.transforms.gold_refresh import (
    calendar_to_fy,
    fy_quarter_label,
    quarter_day_count,
    GoldLayerRefresh,
)


# ── FY Calendar Tests ──

class TestCalendarToFY:
    """Tests for calendar_to_fy conversion."""

    def test_april_is_q1(self):
        fy, q = calendar_to_fy(2025, 4)
        assert fy == "FY26"
        assert q == "Q1"

    def test_june_is_q1(self):
        fy, q = calendar_to_fy(2025, 6)
        assert fy == "FY26"
        assert q == "Q1"

    def test_july_is_q2(self):
        fy, q = calendar_to_fy(2025, 7)
        assert fy == "FY26"
        assert q == "Q2"

    def test_october_is_q3(self):
        fy, q = calendar_to_fy(2025, 10)
        assert fy == "FY26"
        assert q == "Q3"

    def test_january_is_q4(self):
        fy, q = calendar_to_fy(2026, 1)
        assert fy == "FY26"
        assert q == "Q4"

    def test_march_is_q4(self):
        fy, q = calendar_to_fy(2026, 3)
        assert fy == "FY26"
        assert q == "Q4"

    def test_fy17_start(self):
        fy, q = calendar_to_fy(2016, 4)
        assert fy == "FY17"
        assert q == "Q1"

    def test_fy17_end(self):
        fy, q = calendar_to_fy(2017, 3)
        assert fy == "FY17"
        assert q == "Q4"


class TestFYQuarterLabel:
    def test_standard_label(self):
        assert fy_quarter_label("FY26", "Q3") == "Q3FY26"

    def test_q1(self):
        assert fy_quarter_label("FY17", "Q1") == "Q1FY17"


class TestQuarterDayCount:
    def test_q1_fy26(self):
        # Apr-Jun 2025 = 30+31+30 = 91 days
        assert quarter_day_count("FY26", "Q1") == 91

    def test_q2_fy26(self):
        # Jul-Sep 2025 = 31+31+30 = 92 days
        assert quarter_day_count("FY26", "Q2") == 92

    def test_q3_fy26(self):
        # Oct-Dec 2025 = 31+30+31 = 92 days
        assert quarter_day_count("FY26", "Q3") == 92

    def test_q4_fy26(self):
        # Jan-Mar 2026 = 31+28+31 = 90 days
        assert quarter_day_count("FY26", "Q4") == 90


# ── Sample CSV Generation Tests ──

class TestSampleCSVGeneration:
    """Tests for the sample CSV generator."""

    def test_generates_csv_string(self):
        csv_content = generate_sample_csv(2020, 2020)
        assert isinstance(csv_content, str)
        assert "data_date" in csv_content  # Header
        assert "oem_name" in csv_content

    def test_contains_major_oems(self):
        csv_content = generate_sample_csv(2020, 2020)
        assert "MARUTI SUZUKI INDIA LTD" in csv_content
        assert "HERO MOTOCORP LTD" in csv_content
        assert "BAJAJ AUTO LTD" in csv_content

    def test_has_correct_year(self):
        csv_content = generate_sample_csv(2020, 2020)
        assert "2020-01-15" in csv_content
        assert "2020-12-15" in csv_content
        assert "2019-" not in csv_content
        assert "2021-" not in csv_content

    def test_ev_oems_have_start_year(self):
        csv_content = generate_sample_csv(2016, 2020)
        # OLA started 2022, shouldn't appear in 2016-2020
        assert "OLA ELECTRIC" not in csv_content

    def test_ev_oems_appear_after_start(self):
        csv_content = generate_sample_csv(2022, 2023)
        assert "OLA ELECTRIC" in csv_content

    def test_fuel_types_present(self):
        csv_content = generate_sample_csv(2023, 2023)
        assert "PETROL" in csv_content
        assert "ELECTRIC(BOV)" in csv_content
        assert "DIESEL" in csv_content

    def test_multi_year_range(self):
        csv_content = generate_sample_csv(2016, 2025)
        lines = csv_content.strip().split("\n")
        # At least header + many data rows
        assert len(lines) > 100

    def test_writes_to_file(self, tmp_path):
        output_file = str(tmp_path / "test_sample.csv")
        csv_content = generate_sample_csv(2020, 2020, output_path=output_file)
        import os
        assert os.path.exists(output_file)

    def test_seasonal_variation(self):
        """October/November should have higher base volumes (festive season)."""
        assert SEASONAL_FACTORS[10] > 1.0
        assert SEASONAL_FACTORS[11] > 1.0
        assert SEASONAL_FACTORS[6] < 1.0  # Lean season

    def test_all_oem_profiles_have_required_keys(self):
        for name, profile in SAMPLE_OEM_PROFILES.items():
            assert "segment" in profile, f"Missing segment for {name}"
            assert "vehicle_class" in profile, f"Missing vehicle_class for {name}"
            assert "base_monthly" in profile, f"Missing base_monthly for {name}"
            assert "ev_share" in profile, f"Missing ev_share for {name}"


# ── Gold-Layer Revenue Computation Tests ──

class TestGoldLayerRefresh:
    """Tests for GoldLayerRefresh revenue computation."""

    def setup_method(self):
        self.mock_db = AsyncMock()
        self.gold = GoldLayerRefresh(self.mock_db)

    def test_compute_revenue_basic(self):
        """Test basic revenue = registrations × ASP / 100."""
        asp_map = {
            (1, 1): {"asp_lakhs": 7.50, "asp_low_lakhs": 6.38, "asp_high_lakhs": 8.63},
        }
        agg = {
            "oem_id": 1,
            "oem_name": "Test OEM",
            "nse_ticker": "TEST",
            "fy_year": "FY26",
            "fy_quarter": "Q3FY26",
            "breakdowns": [
                {"segment_id": 1, "fuel_id": 1, "registrations": 100_000},
            ],
            "total_registrations": 100_000,
            "months_with_data": 3,
        }

        result = self.gold._compute_revenue(agg, asp_map)

        assert result is not None
        # 100,000 × 7.50 / 100 = 7,500 Cr
        assert result["est_domestic_rev_cr"] == 7500.0
        assert result["est_rev_low_cr"] == 6380.0  # 100k × 6.38 / 100
        assert result["est_rev_high_cr"] == 8630.0  # 100k × 8.63 / 100
        assert result["data_completeness_pct"] == 100.0

    def test_compute_revenue_partial_quarter(self):
        """Data completeness should reflect months with data."""
        asp_map = {
            (1, 1): {"asp_lakhs": 7.50, "asp_low_lakhs": 7.50, "asp_high_lakhs": 7.50},
        }
        agg = {
            "oem_id": 1,
            "oem_name": "Test",
            "nse_ticker": "T",
            "fy_year": "FY26",
            "fy_quarter": "Q3FY26",
            "breakdowns": [
                {"segment_id": 1, "fuel_id": 1, "registrations": 50_000},
            ],
            "total_registrations": 50_000,
            "months_with_data": 2,
        }

        result = self.gold._compute_revenue(agg, asp_map)
        assert result is not None
        # 2 of 3 months = 66.67%
        assert abs(result["data_completeness_pct"] - 66.67) < 0.1

    def test_compute_revenue_multi_segment(self):
        """Multiple segment/fuel breakdowns should sum correctly."""
        asp_map = {
            (1, 1): {"asp_lakhs": 7.50, "asp_low_lakhs": 7.50, "asp_high_lakhs": 7.50},
            (1, 2): {"asp_lakhs": 12.0, "asp_low_lakhs": 12.0, "asp_high_lakhs": 12.0},
        }
        agg = {
            "oem_id": 1,
            "oem_name": "Test",
            "nse_ticker": "T",
            "fy_year": "FY26",
            "fy_quarter": "Q3FY26",
            "breakdowns": [
                {"segment_id": 1, "fuel_id": 1, "registrations": 50_000},
                {"segment_id": 1, "fuel_id": 2, "registrations": 10_000},
            ],
            "total_registrations": 60_000,
            "months_with_data": 3,
        }

        result = self.gold._compute_revenue(agg, asp_map)
        # (50k × 7.5 + 10k × 12.0) / 100 = (375,000 + 120,000) / 100 = 4,950
        assert result["est_domestic_rev_cr"] == 4950.0

    def test_compute_revenue_no_asp_match(self):
        """Unmatched segment/fuel combos should not contribute revenue."""
        asp_map = {}  # Empty — no ASP data
        agg = {
            "oem_id": 1,
            "oem_name": "Test",
            "nse_ticker": "T",
            "fy_year": "FY26",
            "fy_quarter": "Q3FY26",
            "breakdowns": [
                {"segment_id": 99, "fuel_id": 99, "registrations": 100_000},
            ],
            "total_registrations": 100_000,
            "months_with_data": 3,
        }

        result = self.gold._compute_revenue(agg, asp_map)
        assert result is not None
        assert result["est_domestic_rev_cr"] == 0.0
        assert result["reg_volume"] == 100_000

    def test_compute_revenue_zero_volume(self):
        """Zero-volume entries should return None."""
        result = self.gold._compute_revenue(
            {
                "oem_id": 1, "oem_name": "T", "nse_ticker": "T",
                "fy_year": "FY26", "fy_quarter": "Q3FY26",
                "breakdowns": [],
                "total_registrations": 0,
                "months_with_data": 0,
            },
            {},
        )
        assert result is None


# ── Backfill Orchestrator Tests ──

class TestBackfillOrchestrator:
    """Tests for the BackfillOrchestrator."""

    def setup_method(self):
        self.mock_db = AsyncMock()
        self.mock_db.execute = AsyncMock(return_value=None)
        self.mock_db.fetch = AsyncMock(return_value=[])
        self.mock_db.fetchval = AsyncMock(return_value=None)

    def test_orchestrator_initializes(self):
        """BackfillOrchestrator should initialize with db and batch_size."""
        orchestrator = BackfillOrchestrator(self.mock_db, batch_size=2000)
        assert orchestrator._batch_size == 2000
        assert orchestrator._db is self.mock_db

    def test_default_batch_size(self):
        orchestrator = BackfillOrchestrator(self.mock_db)
        assert orchestrator._batch_size == 5000

    @pytest.mark.asyncio
    async def test_process_batch_with_mapper(self):
        """_process_batch should map records and insert them."""
        orchestrator = BackfillOrchestrator(self.mock_db, batch_size=100)

        mock_mapper = MagicMock()
        mock_mapper.resolve_vehicle_class.return_value = (1, False, True)  # segment_id=1, not excluded, mapped
        mock_mapper.resolve_fuel.return_value = (1, True)  # fuel_id=1, mapped
        mock_mapper.resolve_oem.return_value = (5, True)  # oem_id=5, mapped
        mock_mapper._segment_id_to_code = {1: "PV"}
        mock_mapper.others_oem_id = 99
        mock_mapper.get_segment_id_by_code = MagicMock(return_value=1)
        mock_mapper.get_default_fuel_id = MagicMock(return_value=1)

        records = [
            {
                "data_date": "2020-01-15",
                "oem_name": "MARUTI SUZUKI INDIA LTD",
                "vehicle_class": "MOTOR CAR",
                "fuel_type": "PETROL",
                "registration_count": 130000,
                "segment": "PV",
            },
            {
                "data_date": "2020-01-15",
                "oem_name": "TATA MOTORS LTD",
                "vehicle_class": "MOTOR CAR",
                "fuel_type": "DIESEL",
                "registration_count": 42000,
                "segment": "PV",
            },
        ]

        summary: dict = {
            "unmapped_makers": set(),
            "unmapped_fuels": set(),
            "oem_stats": {},
            "year_stats": {},
        }

        loaded, skipped = await orchestrator._process_batch(records, mock_mapper, summary)
        assert loaded == 2
        assert skipped == 0
        assert summary["year_stats"][2020]["records"] == 2
        assert summary["year_stats"][2020]["volume"] == 172000


class TestSampleCSVRoundTrip:
    """Test that generated CSV can be parsed back."""

    def test_csv_parseable(self):
        """Generated CSV should parse as valid CSV."""
        import csv
        import io

        content = generate_sample_csv(2020, 2020)
        reader = csv.DictReader(io.StringIO(content))

        rows = list(reader)
        assert len(rows) > 0

        # Check all required columns
        for row in rows[:5]:
            assert row["data_date"]
            assert row["oem_name"]
            assert row["vehicle_class"]
            assert int(row["registration_count"]) > 0

    def test_csv_dates_valid(self):
        """All dates in generated CSV should be valid."""
        import csv
        import io
        from datetime import datetime

        content = generate_sample_csv(2020, 2020)
        reader = csv.DictReader(io.StringIO(content))

        for row in reader:
            # Should not raise
            dt = datetime.strptime(row["data_date"], "%Y-%m-%d")
            assert dt.year == 2020

    def test_csv_volumes_positive(self):
        """All registration counts should be positive."""
        import csv
        import io

        content = generate_sample_csv(2020, 2020)
        reader = csv.DictReader(io.StringIO(content))

        for row in reader:
            assert int(row["registration_count"]) > 0
