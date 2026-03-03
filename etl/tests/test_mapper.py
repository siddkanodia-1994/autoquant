"""
Tests for DimensionMapper — Tata split resolution and alias lookups.
These are unit tests that mock the database layer.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.transforms.mapper import DimensionMapper


@pytest.fixture
def mock_db():
    """Create a mock DatabaseManager."""
    db = AsyncMock()
    return db


@pytest.fixture
def mapper(mock_db):
    """Create a DimensionMapper with mock DB."""
    m = DimensionMapper(mock_db)
    # Simulate loaded state
    m._oem_alias_map = {
        ("VAHAN", "MARUTI SUZUKI INDIA LTD"): 1,
        ("VAHAN", "TATA MOTORS LTD"): 2,        # Maps to Tata PV by default
        ("VAHAN", "TATA MOTORS LIMITED"): 2,
        ("VAHAN", "TATA MARCOPOLO MOTORS LTD"): 3,  # Maps to Tata CV
        ("VAHAN", "HONDA MOTORCYCLE AND SCOOTER INDIA PVT LTD"): 17,  # Others
    }
    m._tata_pv_id = 2
    m._tata_cv_id = 3
    m._others_id = 17
    m._fuel_map = {
        "PETROL": 4,
        "DIESEL": 7,
        "ELECTRIC(BOV)": 1,
        "CNG ONLY": 8,
    }
    m._vehicle_class_map = {
        "MOTOR CAR": (1, False),     # PV
        "GOODS CARRIER": (7, False),  # CV
        "M-CYCLE/SCOOTER": (12, False),  # 2W
        "THREE WHEELER (PASSENGER)": (None, True),  # Excluded
    }
    m._segment_id_to_code = {1: "PV", 7: "CV", 12: "2W"}
    return m


class TestOEMResolution:
    def test_simple_lookup(self, mapper):
        oem_id, mapped = mapper.resolve_oem("VAHAN", "MARUTI SUZUKI INDIA LTD")
        assert oem_id == 1
        assert mapped is True

    def test_unknown_maker(self, mapper):
        oem_id, mapped = mapper.resolve_oem("VAHAN", "TOTALLY NEW MAKER XYZ")
        assert oem_id is None
        assert mapped is False

    def test_case_insensitive(self, mapper):
        oem_id, mapped = mapper.resolve_oem("VAHAN", "maruti suzuki india ltd")
        assert oem_id == 1

    def test_tata_pv_segment(self, mapper):
        """When Tata Motors LTD + PV segment → stays as Tata PV."""
        oem_id, mapped = mapper.resolve_oem("VAHAN", "TATA MOTORS LTD", segment_code="PV")
        assert oem_id == 2  # Tata PV
        assert mapped is True

    def test_tata_cv_reroute(self, mapper):
        """When Tata Motors LTD + CV segment → reroutes to Tata CV."""
        oem_id, mapped = mapper.resolve_oem("VAHAN", "TATA MOTORS LTD", segment_code="CV")
        assert oem_id == 3  # Tata CV
        assert mapped is True

    def test_tata_explicit_cv_alias(self, mapper):
        """Tata Marcopolo is explicitly CV, doesn't need segment override."""
        oem_id, mapped = mapper.resolve_oem("VAHAN", "TATA MARCOPOLO MOTORS LTD")
        assert oem_id == 3  # Tata CV


class TestFuelResolution:
    def test_known_fuel(self, mapper):
        fuel_id, mapped = mapper.resolve_fuel("PETROL")
        assert fuel_id == 4
        assert mapped is True

    def test_unknown_fuel(self, mapper):
        fuel_id, mapped = mapper.resolve_fuel("HYDROGEN PLASMA")
        assert fuel_id is None
        assert mapped is False

    def test_ev_fuel(self, mapper):
        fuel_id, mapped = mapper.resolve_fuel("ELECTRIC(BOV)")
        assert fuel_id == 1


class TestVehicleClassResolution:
    def test_pv_class(self, mapper):
        seg_id, excluded, mapped = mapper.resolve_vehicle_class("MOTOR CAR")
        assert seg_id == 1
        assert excluded is False
        assert mapped is True

    def test_excluded_class(self, mapper):
        seg_id, excluded, mapped = mapper.resolve_vehicle_class("THREE WHEELER (PASSENGER)")
        assert seg_id is None
        assert excluded is True
        assert mapped is True

    def test_unmapped_class(self, mapper):
        seg_id, excluded, mapped = mapper.resolve_vehicle_class("FLYING CAR")
        assert seg_id is None
        assert excluded is False
        assert mapped is False
