"""
Tests for TransformPipeline — aggregation and filtering logic.
"""

import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

from src.transforms.pipeline import TransformPipeline
from src.transforms.mapper import DimensionMapper


@pytest.fixture
def mock_db():
    db = AsyncMock()
    db.executemany = AsyncMock()
    db.execute = AsyncMock()
    db.refresh_materialized_view = AsyncMock()
    db.fetchval = AsyncMock(return_value=100)
    db.fetchrow = AsyncMock(return_value=None)
    db.fetch = AsyncMock(return_value=[])
    return db


@pytest.fixture
def mock_mapper():
    mapper = MagicMock(spec=DimensionMapper)
    mapper._segment_id_to_code = {1: "PV", 7: "CV", 12: "2W"}
    mapper.others_oem_id = 17

    def resolve_vehicle_class(cls):
        mapping = {
            "MOTOR CAR": (1, False, True),
            "GOODS CARRIER": (7, False, True),
            "M-CYCLE/SCOOTER": (12, False, True),
            "THREE WHEELER (PASSENGER)": (None, True, True),
        }
        return mapping.get(cls.upper().strip(), (None, False, False))

    def resolve_fuel(fuel):
        mapping = {"PETROL": (4, True), "DIESEL": (7, True), "ELECTRIC(BOV)": (1, True)}
        return mapping.get(fuel.upper().strip(), (None, False))

    def resolve_oem(source, maker, segment_code=None):
        mapping = {
            "MARUTI SUZUKI INDIA LTD": (1, True),
            "TATA MOTORS LTD": (2, True),  # PV default
        }
        result = mapping.get(maker.upper().strip(), (None, False))
        oem_id, mapped = result
        # Tata split
        if oem_id == 2 and segment_code == "CV":
            return (3, True)
        return result

    mapper.resolve_vehicle_class = resolve_vehicle_class
    mapper.resolve_fuel = resolve_fuel
    mapper.resolve_oem = resolve_oem
    return mapper


@pytest.fixture
def pipeline(mock_db, mock_mapper):
    return TransformPipeline(mock_db, mock_mapper)


class TestAggregation:
    def test_basic_aggregation(self, pipeline):
        """Multiple records for same (oem, segment, fuel) should sum."""
        records = [
            {"oem_id": 1, "segment_id": 1, "fuel_id": 4, "registration_count": 100, "maker_raw": "X"},
            {"oem_id": 1, "segment_id": 1, "fuel_id": 4, "registration_count": 200, "maker_raw": "X"},
        ]
        result = pipeline._aggregate(records, date(2026, 3, 1))
        assert len(result) == 1
        assert result[0]["registrations"] == 300

    def test_different_fuels_not_merged(self, pipeline):
        """Different fuel_ids should produce separate rows."""
        records = [
            {"oem_id": 1, "segment_id": 1, "fuel_id": 4, "registration_count": 100, "maker_raw": "X"},
            {"oem_id": 1, "segment_id": 1, "fuel_id": 1, "registration_count": 50, "maker_raw": "X"},
        ]
        result = pipeline._aggregate(records, date(2026, 3, 1))
        assert len(result) == 2


class TestMapAndFilter:
    @pytest.mark.asyncio
    async def test_excluded_classes_filtered(self, pipeline):
        """Three-wheelers should be dropped, MOTOR CAR routed to Others."""
        records = [
            {"maker": "SOME MAKER", "fuel": "PETROL", "vehicle_class": "MOTOR CAR",
             "registration_count": 100},
            {"maker": "SOME MAKER", "fuel": "PETROL", "vehicle_class": "THREE WHEELER (PASSENGER)",
             "registration_count": 50},
        ]
        mapped, um_makers, um_fuels, um_classes = await pipeline._map_and_filter(records)
        # MOTOR CAR passes through → unmapped maker routes to Others/Unlisted (oem_id=17)
        assert len(mapped) == 1
        assert mapped[0]["oem_id"] == 17  # Others/Unlisted
        assert mapped[0]["registration_count"] == 100
        # 3W record is excluded — never appears in mapped
        assert "SOME MAKER" in um_makers  # Unmapped maker still flagged

    @pytest.mark.asyncio
    async def test_zero_count_skipped(self, pipeline):
        """Records with 0 registrations should be skipped."""
        records = [
            {"maker": "MARUTI SUZUKI INDIA LTD", "fuel": "PETROL",
             "vehicle_class": "MOTOR CAR", "registration_count": 0},
        ]
        mapped, _, _, _ = await pipeline._map_and_filter(records)
        assert len(mapped) == 0

    @pytest.mark.asyncio
    async def test_tata_cv_reroute(self, pipeline):
        """Tata Motors LTD + GOODS CARRIER → Tata CV entity (oem_id=3)."""
        records = [
            {"maker": "TATA MOTORS LTD", "fuel": "DIESEL",
             "vehicle_class": "GOODS CARRIER", "registration_count": 500},
        ]
        mapped, _, _, _ = await pipeline._map_and_filter(records)
        assert len(mapped) == 1
        assert mapped[0]["oem_id"] == 3  # Tata CV

    @pytest.mark.asyncio
    async def test_tata_pv_stays(self, pipeline):
        """Tata Motors LTD + MOTOR CAR → Tata PV entity (oem_id=2)."""
        records = [
            {"maker": "TATA MOTORS LTD", "fuel": "PETROL",
             "vehicle_class": "MOTOR CAR", "registration_count": 300},
        ]
        mapped, _, _, _ = await pipeline._map_and_filter(records)
        assert len(mapped) == 1
        assert mapped[0]["oem_id"] == 2  # Tata PV
