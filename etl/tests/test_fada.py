"""
Tests for FADA PDF connector.
"""

import pytest
from src.connectors.fada import FADAConnector, FADA_SEGMENT_MAP


class TestFADASegmentMap:
    """Test segment name normalization."""

    def test_pv_variants(self):
        assert FADA_SEGMENT_MAP["passenger vehicles"] == "PV"
        assert FADA_SEGMENT_MAP["pv"] == "PV"

    def test_cv_variants(self):
        assert FADA_SEGMENT_MAP["commercial vehicles"] == "CV"
        assert FADA_SEGMENT_MAP["cv"] == "CV"

    def test_tw_variants(self):
        assert FADA_SEGMENT_MAP["two wheelers"] == "2W"
        assert FADA_SEGMENT_MAP["two-wheelers"] == "2W"
        assert FADA_SEGMENT_MAP["2w"] == "2W"


class TestFADAConnectorParsing:
    """Test FADA connector parsing utilities."""

    def setup_method(self):
        self.connector = FADAConnector()

    def test_parse_numeric_indian_format(self):
        assert FADAConnector._parse_numeric("3,45,678") == 345678

    def test_parse_numeric_international_format(self):
        assert FADAConnector._parse_numeric("345,678") == 345678

    def test_parse_numeric_plain(self):
        assert FADAConnector._parse_numeric("12345") == 12345

    def test_parse_numeric_empty(self):
        assert FADAConnector._parse_numeric("") is None
        assert FADAConnector._parse_numeric("N/A") is None

    def test_parse_pct_positive(self):
        assert FADAConnector._parse_pct("+12.3%") == 12.3

    def test_parse_pct_negative(self):
        assert FADAConnector._parse_pct("-5.0%") == -5.0

    def test_parse_pct_no_sign(self):
        assert FADAConnector._parse_pct("8%") == 8.0

    def test_parse_pct_empty(self):
        assert FADAConnector._parse_pct("") is None

    def test_parse_fada_table_segment_summary(self):
        table = [
            ["Category", "Volume", "YoY %"],
            ["Passenger Vehicles", "3,45,678", "+12.3%"],
            ["Commercial Vehicles", "1,23,456", "-5.0%"],
            ["Two Wheelers", "12,34,567", "+8.5%"],
            ["Total", "16,03,701", "+7.2%"],
        ]
        records = self.connector._parse_fada_table(table, "2026-03", 1, 0)
        assert len(records) == 3  # 3 segments (Total is skipped)

        pv_record = [r for r in records if r["segment"] == "PV"][0]
        assert pv_record["volume"] == 345678
        assert pv_record["yoy_pct"] == 12.3
        assert pv_record["oem_name"] == "INDUSTRY_TOTAL"

    def test_parse_fada_table_oem_level(self):
        table = [
            ["OEM Name", "Volume", "YoY %"],
            ["Maruti Suzuki", "150000", "+5%"],
            ["Hyundai Motor India", "55000", "+3%"],
            ["Total", "205000", "+4.2%"],
        ]
        records = self.connector._parse_fada_table(table, "2026-03", 1, 0)
        # Without segment context, these are classified as "UNKNOWN" segment
        assert len(records) == 2
        assert records[0]["oem_name"] == "MARUTI SUZUKI"
        assert records[0]["volume"] == 150000

    def test_parse_fada_text_extraction(self):
        text = "Passenger Vehicles registered 3,45,678 units during March 2026."
        records = self.connector._parse_fada_text(text, "2026-03", 1)
        assert len(records) == 1
        assert records[0]["segment"] == "PV"
        assert records[0]["volume"] == 345678

    def test_parse_fada_text_lakh_units(self):
        text = "Two Wheelers clocked 12.5 lakh units in the month."
        records = self.connector._parse_fada_text(text, "2026-03", 1)
        assert len(records) == 1
        assert records[0]["segment"] == "2W"
        assert records[0]["volume"] == 1250000

    def test_detect_data_type_wholesale(self):
        header = ["oem", "wholesale dispatches", "yoy"]
        assert self.connector._detect_data_type(header) == "wholesale"

    def test_detect_data_type_retail_default(self):
        header = ["oem", "registrations", "yoy"]
        assert self.connector._detect_data_type(header) == "retail"

    def test_detect_segment_context(self):
        header = ["passenger vehicles", "volume", "yoy"]
        assert self.connector._detect_segment_context(header, []) == "PV"

    def test_detect_segment_context_none(self):
        header = ["oem name", "volume", "yoy"]
        assert self.connector._detect_segment_context(header, []) is None
