"""
AutoQuant ETL — Dimension Mapping & Tata Split Resolution.

Resolves raw VAHAN maker names, fuel types, and vehicle classes
to their canonical dimension table IDs.

Special handling for Tata Motors PV/CV split:
  When maker = 'TATA MOTORS LTD' (or similar), the vehicle class
  determines whether the record routes to Tata PV or Tata CV entity.
"""

import logging
from typing import Optional

from src.utils.database import DatabaseManager

logger = logging.getLogger(__name__)


class DimensionMapper:
    """
    Loads dimension mappings into memory for fast lookup during transforms.
    Refreshed at the start of each ETL run.
    """

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

        # Lookup caches (populated by load())
        self._oem_alias_map: dict[tuple[str, str], int] = {}  # (source, alias) → oem_id
        self._fuel_map: dict[str, int] = {}                    # fuel_code → fuel_id
        self._vehicle_class_map: dict[str, tuple[Optional[int], bool]] = {}  # class → (segment_id, is_excluded)
        self._tata_pv_id: Optional[int] = None
        self._tata_cv_id: Optional[int] = None
        self._others_id: Optional[int] = None

    async def load(self) -> None:
        """Load all dimension mappings from the database."""
        logger.info("Loading dimension mappings...")

        # OEM aliases
        rows = await self._db.fetch(
            "SELECT source, UPPER(TRIM(alias_name)) AS alias_name, oem_id "
            "FROM dim_oem_alias WHERE is_active = TRUE"
        )
        self._oem_alias_map = {
            (row["source"], row["alias_name"]): row["oem_id"] for row in rows
        }
        logger.info("Loaded %d OEM aliases", len(self._oem_alias_map))

        # Tata PV/CV entity IDs for split resolution
        self._tata_pv_id = await self._db.fetchval(
            "SELECT oem_id FROM dim_oem WHERE oem_name = 'Tata Motors Ltd (PV)'"
        )
        self._tata_cv_id = await self._db.fetchval(
            "SELECT oem_id FROM dim_oem WHERE oem_name = 'Tata Motors Ltd (CV)'"
        )
        self._others_id = await self._db.fetchval(
            "SELECT oem_id FROM dim_oem WHERE oem_name = 'Others/Unlisted'"
        )

        # Fuel mappings
        rows = await self._db.fetch(
            "SELECT UPPER(TRIM(fuel_code)) AS fuel_code, fuel_id FROM dim_fuel"
        )
        self._fuel_map = {row["fuel_code"]: row["fuel_id"] for row in rows}
        logger.info("Loaded %d fuel mappings", len(self._fuel_map))

        # Vehicle class mappings
        rows = await self._db.fetch(
            "SELECT UPPER(TRIM(vahan_class_name)) AS vahan_class_name, segment_id, is_excluded "
            "FROM dim_vehicle_class_map"
        )
        self._vehicle_class_map = {
            row["vahan_class_name"]: (row["segment_id"], row["is_excluded"])
            for row in rows
        }
        logger.info("Loaded %d vehicle class mappings", len(self._vehicle_class_map))

    def resolve_oem(
        self, source: str, maker_name: str, segment_code: Optional[str] = None
    ) -> tuple[Optional[int], bool]:
        """
        Resolve a raw maker name to an oem_id.

        Special logic: If maker is a Tata ambiguous alias (maps to Tata PV by default),
        but the vehicle class resolves to CV segment → reroute to Tata CV entity.

        Returns:
            (oem_id, is_mapped) — oem_id is None if unmapped
        """
        key = (source.upper(), maker_name.upper().strip())
        oem_id = self._oem_alias_map.get(key)

        if oem_id is None:
            return None, False

        # Tata split resolution
        if oem_id == self._tata_pv_id and segment_code == "CV":
            return self._tata_cv_id, True
        if oem_id == self._tata_cv_id and segment_code == "PV":
            return self._tata_pv_id, True

        return oem_id, True

    def resolve_fuel(self, fuel_code: str) -> tuple[Optional[int], bool]:
        """Resolve a raw fuel code to a fuel_id."""
        fuel_id = self._fuel_map.get(fuel_code.upper().strip())
        return fuel_id, fuel_id is not None

    def resolve_vehicle_class(
        self, vehicle_class: str
    ) -> tuple[Optional[int], bool, bool]:
        """
        Resolve a VAHAN vehicle class to a segment_id.

        Returns:
            (segment_id, is_excluded, is_mapped)
            - segment_id: The dim_segment FK (None if excluded)
            - is_excluded: True if this class is in the exclusion list
            - is_mapped: True if the class was found in dim_vehicle_class_map
        """
        key = vehicle_class.upper().strip()
        result = self._vehicle_class_map.get(key)
        if result is None:
            return None, False, False
        segment_id, is_excluded = result
        return segment_id, is_excluded, True

    def get_segment_code_for_class(self, vehicle_class: str) -> Optional[str]:
        """
        Quick lookup: vehicle class → segment code ('PV', 'CV', '2W').
        Used by the Tata split resolver.
        """
        segment_id, is_excluded, is_mapped = self.resolve_vehicle_class(vehicle_class)
        if not is_mapped or is_excluded or segment_id is None:
            return None
        # We'd need a reverse lookup; for efficiency, maintain a segment_id → code map
        # This is populated during load() as well
        return self._segment_id_to_code.get(segment_id)

    async def load_segment_codes(self) -> None:
        """Load segment_id → segment_code reverse mapping."""
        rows = await self._db.fetch(
            "SELECT segment_id, segment_code FROM dim_segment WHERE sub_segment IS NULL"
        )
        self._segment_id_to_code = {row["segment_id"]: row["segment_code"] for row in rows}

    async def load_all(self) -> None:
        """Load all mappings including segment codes."""
        await self.load()
        await self.load_segment_codes()

    def get_segment_id_by_code(self, segment_code: str) -> Optional[int]:
        """Reverse lookup: segment_code ('PV', 'CV', '2W') → segment_id."""
        for seg_id, code in self._segment_id_to_code.items():
            if code == segment_code.upper():
                return seg_id
        return None

    def get_default_fuel_id(self) -> Optional[int]:
        """Return fuel_id for PETROL as fallback for unspecified fuel."""
        return self._fuel_map.get("PETROL")

    @property
    def unmapped_oem_count(self) -> int:
        """Not a real count — for interface completeness."""
        return 0

    @property
    def others_oem_id(self) -> Optional[int]:
        return self._others_id
