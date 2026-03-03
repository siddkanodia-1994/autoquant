"""
Tests for SIAM Historical CSV connector.
"""

import pytest
from src.connectors.siam_historical import SIAMHistoricalConnector


class TestSIAMDateNormalization:
    """Test date format handling."""

    def test_iso_format(self):
        assert SIAMHistoricalConnector._normalize_date("2016-01-15") == "2016-01-15"

    def test_dd_mm_yyyy(self):
        assert SIAMHistoricalConnector._normalize_date("15/01/2016") == "2016-01-15"

    def test_mon_yyyy(self):
        assert SIAMHistoricalConnector._normalize_date("Jan-2016") == "2016-01-15"

    def test_yyyy_mm(self):
        assert SIAMHistoricalConnector._normalize_date("2016-01") == "2016-01-15"

    def test_fy_format(self):
        assert SIAMHistoricalConnector._normalize_date("FY2016") == "2016-04-01"

    def test_plain_year(self):
        assert SIAMHistoricalConnector._normalize_date("2016") == "2016-01-01"

    def test_empty(self):
        assert SIAMHistoricalConnector._normalize_date("") is None

    def test_invalid(self):
        assert SIAMHistoricalConnector._normalize_date("not a date") is None


class TestSIAMPeriodFilter:
    """Test period-based record filtering."""

    def setup_method(self):
        self.records = [
            {"data_date": "2016-06-15", "registration_count": 100},
            {"data_date": "2018-03-15", "registration_count": 200},
            {"data_date": "2020-12-15", "registration_count": 300},
            {"data_date": "2024-01-15", "registration_count": 400},
        ]

    def test_all_filter(self):
        result = SIAMHistoricalConnector._filter_by_period(self.records, "all")
        assert len(result) == 4

    def test_year_range(self):
        result = SIAMHistoricalConnector._filter_by_period(self.records, "2018-2020")
        assert len(result) == 2

    def test_single_month(self):
        result = SIAMHistoricalConnector._filter_by_period(self.records, "2020-12")
        assert len(result) == 1
        assert result[0]["data_date"] == "2020-12-15"


class TestSIAMRecordValidation:
    """Test record validation."""

    def test_valid_records_pass(self):
        records = [
            {"data_date": "2016-01-15", "registration_count": 100},
            {"data_date": "2018-06-15", "registration_count": 200},
        ]
        valid, invalid = SIAMHistoricalConnector._validate_records(records)
        assert len(valid) == 2
        assert invalid == 0

    def test_missing_date_rejected(self):
        records = [
            {"data_date": None, "registration_count": 100},
            {"data_date": "2016-01-15", "registration_count": 200},
        ]
        valid, invalid = SIAMHistoricalConnector._validate_records(records)
        assert len(valid) == 1
        assert invalid == 1

    def test_zero_count_rejected(self):
        records = [
            {"data_date": "2016-01-15", "registration_count": 0},
            {"data_date": "2016-01-15", "registration_count": 100},
        ]
        valid, invalid = SIAMHistoricalConnector._validate_records(records)
        assert len(valid) == 1
        assert invalid == 1

    def test_negative_count_rejected(self):
        records = [
            {"data_date": "2016-01-15", "registration_count": -50},
        ]
        valid, invalid = SIAMHistoricalConnector._validate_records(records)
        assert len(valid) == 0
        assert invalid == 1


class TestSIAMCSVParsing:
    """Test CSV content parsing."""

    def setup_method(self):
        self.connector = SIAMHistoricalConnector()

    def test_standard_template(self):
        csv = (
            "data_date,oem_name,vehicle_class,fuel_type,registration_count,source\n"
            "2016-01-15,MARUTI SUZUKI INDIA LTD,MOTOR CAR,PETROL,45000,SIAM\n"
            "2016-01-15,TATA MOTORS LTD,GOODS CARRIER,DIESEL,12000,SIAM\n"
        )
        records = self.connector._parse_csv_content(csv)
        assert len(records) == 2
        assert records[0]["oem_name"] == "MARUTI SUZUKI INDIA LTD"
        assert records[0]["registration_count"] == 45000
        assert records[0]["fuel_type"] == "PETROL"

    def test_alternative_headers(self):
        csv = (
            "date,maker,category,fuel,count\n"
            "2016-01-15,HERO MOTOCORP LTD,M-CYCLE/SCOOTER,PETROL,90000\n"
        )
        records = self.connector._parse_csv_content(csv)
        assert len(records) == 1
        assert records[0]["oem_name"] == "HERO MOTOCORP LTD"
        assert records[0]["registration_count"] == 90000

    def test_indian_comma_numbers(self):
        csv = (
            "data_date,oem_name,vehicle_class,fuel_type,registration_count\n"
            "2016-01-15,MARUTI,MOTOR CAR,PETROL,\"1,50,000\"\n"
        )
        records = self.connector._parse_csv_content(csv)
        assert len(records) == 1
        assert records[0]["registration_count"] == 150000

    def test_skip_empty_rows(self):
        csv = (
            "data_date,oem_name,vehicle_class,fuel_type,registration_count\n"
            "2016-01-15,MARUTI,MOTOR CAR,PETROL,45000\n"
            ",,,,\n"
            "2016-01-15,HYUNDAI,MOTOR CAR,PETROL,30000\n"
        )
        records = self.connector._parse_csv_content(csv)
        assert len(records) == 2

    def test_date_range_extraction(self):
        records = [
            {"data_date": "2016-01-15"},
            {"data_date": "2020-06-15"},
            {"data_date": "2024-12-15"},
        ]
        dr = SIAMHistoricalConnector._get_date_range(records)
        assert dr["min_date"] == "2016-01-15"
        assert dr["max_date"] == "2024-12-15"

    def test_empty_records_date_range(self):
        dr = SIAMHistoricalConnector._get_date_range([])
        assert dr["min_date"] == ""
        assert dr["max_date"] == ""
