"""
Tests for BSE Wholesale connector.
"""

import pytest
from src.connectors.bse_wholesale import BSEWholesaleConnector, BSE_AUTO_OEMS, MONTH_LABELS


class TestBSEConstants:
    """Test BSE connector constants."""

    def test_all_oems_have_bse_code(self):
        for ticker, info in BSE_AUTO_OEMS.items():
            assert "bse_code" in info, f"{ticker} missing bse_code"
            assert info["bse_code"].isdigit(), f"{ticker} bse_code not numeric"

    def test_all_oems_have_name(self):
        for ticker, info in BSE_AUTO_OEMS.items():
            assert "name" in info, f"{ticker} missing name"
            assert len(info["name"]) > 0

    def test_key_oems_present(self):
        assert "MARUTI" in BSE_AUTO_OEMS
        assert "TATAMOTORS_PV" in BSE_AUTO_OEMS
        assert "TATAMOTORS_CV" in BSE_AUTO_OEMS
        assert "M&M" in BSE_AUTO_OEMS
        assert "BAJAJ-AUTO" in BSE_AUTO_OEMS
        assert "HEROMOTOCO" in BSE_AUTO_OEMS

    def test_month_labels_complete(self):
        assert len(MONTH_LABELS) == 12
        assert MONTH_LABELS[1] == "january"
        assert MONTH_LABELS[12] == "december"


class TestBSEFilingParser:
    """Test BSE filing parsing logic."""

    def setup_method(self):
        self.connector = BSEWholesaleConnector()

    def test_parse_filing_total_volume(self):
        filing = {
            "NEWSSUB": "Auto Sales data for March 2026",
            "HEADLINE": "",
            "NEWS_DT_BODY": "Total sales of 150,000 units during March 2026.",
            "NEWS_DT": "2026-04-01",
            "ATTACHMENT": "",
        }
        records = self.connector._parse_filing(filing, "MARUTI", "Maruti Suzuki", "2026-03")
        assert len(records) >= 1
        total_records = [r for r in records if r["segment"] == "TOTAL"]
        assert len(total_records) == 1
        assert total_records[0]["volume"] == 150000

    def test_parse_filing_segment_breakdown(self):
        filing = {
            "NEWSSUB": "Monthly Sales Update",
            "HEADLINE": "",
            "NEWS_DT_BODY": (
                "Passenger Vehicle sales of 80,000 units. "
                "Commercial Vehicle dispatches of 25,000 units. "
                "Total sales stood at 105,000 units."
            ),
            "NEWS_DT": "2026-04-01",
            "ATTACHMENT": "",
        }
        records = self.connector._parse_filing(filing, "TATAMOTORS_PV", "Tata Motors", "2026-03")

        segments_found = {r["segment"] for r in records}
        assert "PV" in segments_found
        assert "CV" in segments_found
        assert "TOTAL" in segments_found

    def test_parse_filing_ev_numbers(self):
        filing = {
            "NEWSSUB": "Auto Sales data",
            "HEADLINE": "",
            "NEWS_DT_BODY": (
                "Total dispatches of 50,000 units. "
                "EV sales of 5,000 units during the month."
            ),
            "NEWS_DT": "2026-04-01",
            "ATTACHMENT": "",
        }
        records = self.connector._parse_filing(filing, "TATAMOTORS_PV", "Tata Motors", "2026-03")

        ev_records = [r for r in records if r["powertrain"] == "EV"]
        assert len(ev_records) == 1
        assert ev_records[0]["volume"] == 5000

    def test_parse_filing_no_data(self):
        filing = {
            "NEWSSUB": "Board Meeting Notice",
            "HEADLINE": "",
            "NEWS_DT_BODY": "Notice is hereby given that...",
            "NEWS_DT": "2026-04-01",
            "ATTACHMENT": "",
        }
        records = self.connector._parse_filing(filing, "MARUTI", "Maruti Suzuki", "2026-03")
        assert len(records) == 0

    def test_filter_sales_announcements(self):
        announcements = [
            {"NEWSSUB": "Auto Sales data for March 2026", "HEADLINE": ""},
            {"NEWSSUB": "Board Meeting Notice", "HEADLINE": ""},
            {"NEWSSUB": "Monthly Sales Update - Mar 2026", "HEADLINE": ""},
            {"NEWSSUB": "Annual General Meeting", "HEADLINE": ""},
        ]
        filtered = self.connector._filter_sales_announcements(announcements, "march", "2026")
        assert len(filtered) == 2

    def test_clean_number(self):
        assert BSEWholesaleConnector._clean_number("1,50,000") == 150000
        assert BSEWholesaleConnector._clean_number("150000") == 150000
        assert BSEWholesaleConnector._clean_number("1,500") == 1500
        assert BSEWholesaleConnector._clean_number("abc") is None
