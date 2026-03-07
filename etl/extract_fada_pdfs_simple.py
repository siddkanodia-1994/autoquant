#!/usr/bin/env python3
"""
FADA Monthly Press Release PDF Extractor - Simplified Synchronous Version

Downloads FADA monthly press release PDFs and extracts OEM-wise vehicle retail data.
Outputs clean CSV data with segment, OEM, volume, and market share percentage.
"""

import csv
import io
import logging
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pdfplumber

# ============================================================================
# Configuration
# ============================================================================

FADA_PDFS = {
    '2026-02': 'https://fada.in/images/press-release/169a8f8bd834feFADA%20releases%20February%202026%20Vehicle%20Retail%20Data.pdf',
    '2026-01': 'https://fada.in/images/press-release/1698aa6afacdf0FADA%20releases%20January%202026%20Vehicle%20Retail%20Data.pdf',
    '2025-12': 'https://fada.in/images/press-release/1695cc82c07602FADA%20releases%20CY%202025%20and%20December%202025%20Vehicle%20Retail%20Data.pdf',
    '2025-11': 'https://fada.in/images/press-release/1693661428b2e7FADA%20releases%20November%202025%20Vehicle%20Retail%20Data.pdf',
    '2025-10': 'https://fada.in/images/press-release/1690d68d7a2e6cFADA%20releases%20October%202025%20%26%2042-Days%20Festive%20Vehicle%20Retail%20Data.pdf',
    '2025-09': 'https://fada.in/images/press-release/168e48945da69dFADA%20releases%20September%202025%20and%20Navratri%20Vehicle%20Retail%20Data.pdf',
    '2025-08': 'https://fada.in/images/press-release/168be66c36b877FADA%20releases%20August%202025%20Vehicle%20Retail%20Data.pdf',
    '2025-07': 'https://fada.in/images/press-release/168941dda798bcFADA%20releases%20July%202025%20Vehicle%20Retail%20Data.pdf',
    '2025-06': 'https://fada.in/images/press-release/1686b40981c6d0FADA%20releases%20June%202025%20Vehicle%20Retail%20Data.pdf',
    '2025-05': 'https://fada.in/images/press-release/1684260cc6aca5FADA%20releases%20May%202025%20Vehicle%20Retail%20Data.pdf',
    '2025-03': 'https://www.fada.in/images/press-release/167f3463b1a212FADA%20Releases%20FY%202025%20and%20March%202025%20Vehicle%20Retail%20Data.pdf',
    '2025-02': 'https://fada.in/images/press-release/167c94f2acd830FADA%20releases%20February%202025%20Vehicle%20Retail%20Data.pdf',
    '2025-01': 'https://fada.in/images/press-release/167a42affa6ff9FADA%20releases%20January%202025%20Vehicle%20Retail%20Data.pdf',
    '2024-12': 'https://fada.in/images/press-release/1677c9fa734b0cFADA%20releases%20CY%202024%20and%20December%202024%20Vehicle%20Retail%20Data.pdf',
    '2024-11': 'https://fada.in/images/press-release/167566418d08beFADA%20releases%20November%202024%20Vehicle%20Retail%20Data.pdf',
    '2024-10': 'https://fada.in/images/press-release/1672ae22942b7fFADA%20releases%20October%202024%20Vehicle%20Retail%20Data.pdf',
    '2024-09': 'https://fada.in/images/press-release/16703bd0a727fbFADA%20releases%20September%202024%20Vehicle%20Retail%20Data.pdf',
    '2024-08': 'https://fada.in/images/press-release/166d925182828bFADA%20releases%20August%202024%20Vehicle%20Retail%20Data.pdf',
    '2024-07': 'https://fada.in/images/press-release/166b046f6d37ffFADA%20releases%20July%202024%20Vehicle%20Retail%20Data.pdf',
    '2024-06': 'https://fada.in/images/press-release/1668768a35fb67FADA%20releases%20June%202024%20Vehicle%20Retail%20Data.pdf',
    '2024-05': 'https://fada.in/images/press-release/16666733b69de8FADA_releases_May_2024_Vehicle_Retail_Data.pdf',
    '2024-04': 'https://fada.in/images/press-release/1663af19a1dcbdFADA%20releases%20April%202024%20Vehicle%20Retail%20Data.pdf',
    '2024-02': 'https://fada.in/images/press-release/165e93530ae2c9FADA%20releases%20February%202024%20Vehicle%20Retail%20Data.pdf',
    '2023-12': 'https://fada.in/images/press-release/1659b6bf5b5962FADA%20Releases%20December%202023%20and%20CY%202023%20Vehicle%20Retail%20Data.pdf',
    '2023-11': 'https://fada.in/images/press-release/165707ec862f3dFADA%20Releases%20November%202023%20Vehicle%20Retail%20Data.pdf',
    '2023-10': 'https://fada.in/images/press-release/165485d8f4423bFADA%20Releases%20October%202023%20Vehicle%20Retail%20Data.pdf',
    '2023-09': 'https://fada.in/images/press-release/165237130d355aFADA%20Releases%20September%202023%20Vehicle%20Retail%20Data.pdf',
    '2023-08': 'https://fada.in/images/press-release/164f6a5ee4ada1FADA%20releases%20August%202023%20Vehicle%20Retail%20Data.pdf',
    '2023-06': 'https://fada.in/images/press-release/164a63562b4002FADA%20Releases%20June%202023%20Vehicle%20Retail%20Data.pdf',
    '2023-04': 'https://www.fada.in/images/press-release/1645339b5389cbFADA%20Releases%20April%202023%20Vehicle%20Retail%20Data.pdf',
    '2023-03': 'https://fada.in/images/press-release/1642fe78e86f4cFADA%20Press%20Release%20-%20FADA%20releases%20Mar%2723%20%26%20FY%2723%20Vehicle%20Retail%20Data_final.pdf',
    '2023-02': 'https://fada.in/images/press-release/164055e69c3f8dFADA%20Releases%20February%2723%20Vehicle%20Retail%20Data.pdf',
    '2022-09': 'https://fada.in/images/press-release/1633bbf4bd8658FADA%20Releases%20September%2722%20Vehicle%20Retail%20Data.pdf',
    '2022-06': 'https://fada.in/images/press-release/162c3b03b6f373FADA%20Releases%20Jun%2722%20Vehicle%20Retail%20Data.pdf',
    '2022-04': 'https://fada.in/images/press-release/162735070afcdaFADA%20Releases%20April%2722%20Vehicle%20Retail%20Data.pdf',
    '2022-01': 'https://fada.in/images/press-release/1620091addc405FADA%20releases%20January%202022%20Vehicle%20Retail%20Data.pdf',
    '2021-12': 'https://fada.in/images/press-release/161d510bfbc9d6FADA%20releases%20December%202021%20Vehicle%20Retail%20Data.pdf',
    '2021-11': 'https://fada.in/images/press-release/161b0274270644FADA%20releases%20November%202021%20Vehicle%20Retail%20Data.pdf',
    '2021-08': 'https://fada.in/images/press-release/16136dce05fe1bFADA%20releases%20August%202021%20Vehicle%20Retail%20Data.pdf',
    '2021-06': 'https://fada.in/images/press-release/160e672556b96fFADA_releases_June_2021_Vehicle_Retail_Data.pdf',
    '2021-05': 'https://www.fada.in/images/press-release/160c187f005d7eFADA%20releases%20May%202021%20Vehicle%20Retail%20Data.pdf',
}

# Parent-subsidiary relationships
GROUP_RULES = {
    '2W': {
        'BAJAJ AUTO GROUP': ['BAJAJ AUTO LTD', 'CHETAK TECHNOLOGY LIMITED'],
    },
    'CV': {
        'MAHINDRA & MAHINDRA LIMITED': ['MAHINDRA SUSTEN LIMITED', 'MAHINDRA LOGISTICS LIMITED'],
        'ASHOK LEYLAND LTD': ['SWITCH MOBILITY'],
        'VE COMMERCIAL VEHICLES LTD': ['VOLVO BUSES DIVISION'],
    },
    'PV': {
        'SKODA AUTO VOLKSWAGEN GROUP': ['VOLKSWAGEN', 'SKODA'],
        'MERCEDES-BENZ GROUP': ['MERCEDES-BENZ', 'SMART'],
        'STELLANTIS GROUP': ['JEEP', 'CITROEN', 'PEUGEOT'],
    },
}

# ============================================================================
# Logging Setup
# ============================================================================

def setup_logger():
    """Setup console logger."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

logger = setup_logger()

# ============================================================================
# Data Model
# ============================================================================

@dataclass
class FADARecord:
    """Extracted FADA data record."""
    report_period: str
    oem_name: str
    segment: str
    volume: int
    market_share_pct: Optional[float] = None

    def to_dict(self):
        return asdict(self)

# ============================================================================
# PDF Parsing
# ============================================================================

class FADAParser:
    """Parse FADA PDF tables."""

    @staticmethod
    def detect_segment_from_header(header_text: str) -> Optional[str]:
        """Detect segment from table header text."""
        header_lower = header_text.lower()
        if 'two-wheeler' in header_lower or 'two wheeler' in header_lower or '2w' in header_lower:
            return '2W'
        elif 'three-wheeler' in header_lower or 'three wheeler' in header_lower or '3w' in header_lower:
            return '3W'
        elif 'commercial vehicle' in header_lower or 'cv' in header_lower:
            return 'CV'
        elif 'construction equipment' in header_lower or 'ce' in header_lower:
            return 'CE'
        elif 'passenger vehicle' in header_lower or 'pv' in header_lower:
            return 'PV'
        elif 'tractor' in header_lower or 'trac' in header_lower:
            return 'TRAC'
        return None

    @staticmethod
    def parse_number(val: str) -> int:
        """Parse number handling Indian format."""
        if not val or val == '-' or val.strip() == '':
            return 0
        clean = re.sub(r'[^\d.]', '', val.replace(',', ''))
        if not clean:
            return 0
        try:
            return int(float(clean))
        except ValueError:
            return 0

    @staticmethod
    def parse_percentage(val: str) -> Optional[float]:
        """Parse percentage."""
        if not val or val == '-' or val.strip() == '':
            return None
        match = re.search(r'([-+]?\d+(?:\.\d+)?)', val)
        return float(match.group(1)) if match else None

    @staticmethod
    def clean_oem_name(name: str) -> str:
        """Clean OEM name."""
        name = name.strip()
        # Handle line breaks in PDF text
        name = re.sub(r'\s+', ' ', name)
        return name.upper()

    @staticmethod
    def parse_table(table: List[List[Optional[str]]], period: str) -> List[FADARecord]:
        """Parse a single table from PDF."""
        records = []

        if len(table) < 2:
            return records

        # Clean cells
        clean_table = [
            [(cell or '').strip() for cell in row]
            for row in table
        ]

        # Get header
        header = clean_table[0]
        header_full = ' '.join(header).lower()

        # Detect segment from header
        segment = FADAParser.detect_segment_from_header(header_full)
        if not segment:
            return records  # Skip if we can't determine segment

        # Find column indices
        oem_col = -1
        volume_col = -1
        share_col = -1

        for i, col in enumerate(header):
            col_lower = col.lower()
            if not oem_col and ('oem' in col_lower or 'company' in col_lower or 'brand' in col_lower):
                oem_col = i
            if not volume_col and ('feb' in col_lower or 'jan' in col_lower or 'mar' in col_lower or
                                   'apr' in col_lower or 'may' in col_lower or 'jun' in col_lower or
                                   'jul' in col_lower or 'aug' in col_lower or 'sep' in col_lower or
                                   'oct' in col_lower or 'nov' in col_lower or 'dec' in col_lower or
                                   'volume' in col_lower or 'units' in col_lower or 'nos' in col_lower):
                # Make sure it's not market share column
                if 'share' not in col_lower and '%' not in col_lower:
                    volume_col = i
            if not share_col and ('share' in col_lower and '%' in col_lower):
                share_col = i

        if oem_col < 0:
            # If no OEM column found, first column is OEM
            oem_col = 0
        if volume_col < 0:
            # If no volume column found, second column is volume
            volume_col = 1 if len(header) > 1 else -1

        # Parse data rows
        for row in clean_table[1:]:
            if not row or not row[0].strip():
                continue

            # Skip total rows
            first_cell = row[0].upper()
            if first_cell in ('TOTAL', 'GRAND TOTAL', 'SUB TOTAL'):
                continue

            # Extract OEM name
            oem_name = row[oem_col].strip() if oem_col < len(row) else ""
            if not oem_name:
                continue

            oem_name = FADAParser.clean_oem_name(oem_name)

            # Extract volume
            volume = 0
            if volume_col >= 0 and volume_col < len(row):
                volume = FADAParser.parse_number(row[volume_col])

            if volume <= 0:
                continue

            # Extract market share
            share_pct = None
            if share_col >= 0 and share_col < len(row):
                share_pct = FADAParser.parse_percentage(row[share_col])

            record = FADARecord(
                report_period=period,
                oem_name=oem_name,
                segment=segment,
                volume=volume,
                market_share_pct=share_pct
            )
            records.append(record)

        return records

    @staticmethod
    def parse_pdf(pdf_bytes: bytes, period: str) -> List[FADARecord]:
        """Extract records from PDF."""
        records = []

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                logger.info(f"Parsing {period}: {len(pdf.pages)} pages")

                for page_num, page in enumerate(pdf.pages, 1):
                    tables = page.extract_tables()

                    for table in tables or []:
                        parsed = FADAParser.parse_table(table, period)
                        records.extend(parsed)

        except Exception as e:
            logger.error(f"PDF parsing error for {period}: {e}")

        # Deduplicate using group rules
        records = FADAParser.deduplicate(records)
        logger.info(f"  Extracted {len(records)} records")

        return records

    @staticmethod
    def deduplicate(records: List[FADARecord]) -> List[FADARecord]:
        """Remove sub-brand records if parent group is present."""
        by_segment = {}
        for r in records:
            key = (r.segment, r.oem_name)
            if r.segment not in by_segment:
                by_segment[r.segment] = {}
            by_segment[r.segment][r.oem_name] = r

        result = []
        for segment, oems in by_segment.items():
            rules = GROUP_RULES.get(segment, {})
            groups_present = {g for g in rules.keys() if g in oems}

            for oem_name, record in oems.items():
                skip = False
                for group in groups_present:
                    if oem_name in rules[group]:
                        skip = True
                        break

                if not skip:
                    result.append(record)

        return result

# ============================================================================
# Download
# ============================================================================

def download_pdf(url: str, period: str, max_retries: int = 3) -> Optional[bytes]:
    """Download PDF with retry."""
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading {period} (attempt {attempt + 1}/{max_retries})")
            resp = httpx.get(
                url,
                timeout=60,
                follow_redirects=True,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'},
            )
            resp.raise_for_status()

            if len(resp.content) > 1000:
                logger.info(f"  Downloaded {len(resp.content)} bytes")
                return resp.content

        except Exception as e:
            logger.warning(f"  Failed: {e}")

        if attempt < max_retries - 1:
            import time
            time.sleep(2 ** attempt)

    logger.error(f"Failed to download {period}")
    return None

# ============================================================================
# Export
# ============================================================================

def export_csv(records: List[FADARecord], path: Path):
    """Export to CSV."""
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['report_period', 'oem_name', 'segment', 'volume', 'market_share_pct']
        )
        writer.writeheader()
        for r in records:
            writer.writerow(r.to_dict())

    logger.info(f"Exported {len(records)} records to {path}")

def generate_sql(records: List[FADARecord]) -> str:
    """Generate SQL INSERT statements."""
    lines = [
        "-- FADA Historical Vehicle Retail Data",
        f"-- Generated: {datetime.now().isoformat()}",
        "",
    ]

    for r in records:
        oem_escaped = r.oem_name.replace("'", "''")
        sql = (
            f"INSERT INTO fada_monthly_retail "
            f"(report_period, oem_name, segment, volume, market_share_pct) "
            f"VALUES ('{r.report_period}', '{oem_escaped}', '{r.segment}', {r.volume}, "
            f"{r.market_share_pct if r.market_share_pct is not None else 'NULL'}) "
            f"ON CONFLICT DO NOTHING;"
        )
        lines.append(sql)

    return "\n".join(lines)

# ============================================================================
# Main
# ============================================================================

def main(output_dir: Optional[Path] = None):
    """Download and extract all PDFs."""
    if output_dir is None:
        output_dir = Path(__file__).parent.parent.parent / "data"

    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Output: {output_dir}")
    logger.info(f"Processing {len(FADA_PDFS)} PDFs...")

    all_records = []
    failed = []

    for period, url in sorted(FADA_PDFS.items(), reverse=True):
        try:
            pdf_bytes = download_pdf(url, period, max_retries=2)
            if pdf_bytes:
                records = FADAParser.parse_pdf(pdf_bytes, period)
                all_records.extend(records)
            else:
                failed.append(period)

            # Rate limit
            import time
            time.sleep(1)

        except KeyboardInterrupt:
            logger.warning("Interrupted")
            break
        except Exception as e:
            logger.error(f"Error processing {period}: {e}")
            failed.append(period)

    if not all_records:
        logger.error("No records extracted")
        return

    # Sort and export
    all_records.sort(key=lambda r: (r.report_period, r.segment, r.oem_name))

    csv_path = output_dir / "fada_historical.csv"
    sql_path = output_dir / "fada_historical.sql"

    export_csv(all_records, csv_path)

    sql_content = generate_sql(all_records)
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write(sql_content)
    logger.info(f"Exported SQL to {sql_path}")

    # Summary
    logger.info(f"\n=== SUMMARY ===")
    logger.info(f"Total records: {len(all_records)}")

    by_segment = {}
    for r in all_records:
        seg = r.segment
        if seg not in by_segment:
            by_segment[seg] = 0
        by_segment[seg] += 1

    for seg in sorted(by_segment.keys()):
        logger.info(f"  {seg}: {by_segment[seg]} records")

    if failed:
        logger.warning(f"Failed periods: {', '.join(failed)}")

if __name__ == "__main__":
    output_path = None
    if len(sys.argv) > 1:
        output_path = Path(sys.argv[1])

    main(output_path)
