"""MAPPING tab parser - reads template mapping from Google Sheet."""

import logging
from dataclasses import dataclass
from typing import List, Optional

from .utils import normalize_for_compare

logger = logging.getLogger("price_sheet_bot.mapping")


@dataclass
class MappingRow:
    """A single row from the MAPPING tab."""
    community: str
    floorplan: str
    file_name: str
    invisible_code: str
    header_row: int  # 1-based, default 2
    row_index: int

    @staticmethod
    def from_dict(record: dict, row_index: int) -> Optional["MappingRow"]:
        """Parse a dict into a MappingRow. Returns None if required fields missing."""
        community = str(record.get("community", "")).strip()
        floorplan = str(record.get("floorplan", "")).strip()
        file_name = str(record.get("file_name", record.get("file name", ""))).strip()
        invisible_code = str(record.get("invisible_code", record.get("invisible code", ""))).strip()

        if not community or not floorplan or not file_name or not invisible_code:
            logger.warning(
                "MAPPING row %d missing required fields, skipping.", row_index
            )
            return None

        header_row_raw = record.get("header_row", record.get("header row", "2"))
        try:
            header_row = int(str(header_row_raw).strip()) if str(header_row_raw).strip() else 2
        except ValueError:
            header_row = 2

        return MappingRow(
            community=community,
            floorplan=floorplan,
            file_name=file_name,
            invisible_code=invisible_code,
            header_row=header_row,
            row_index=row_index,
        )


def parse_mapping_tab(records: list) -> List[MappingRow]:
    """Parse all records from the MAPPING tab into MappingRow objects."""
    rows = []
    for i, record in enumerate(records):
        row = MappingRow.from_dict(record, row_index=i + 2)
        if row is not None:
            rows.append(row)
    logger.info("Parsed %d MAPPING rows from %d total.", len(rows), len(records))
    return rows


def find_mapping_row(
    mapping_rows: List[MappingRow],
    community: str,
    floorplan: str,
) -> Optional[MappingRow]:
    """Find a MAPPING row matching (community, floorplan) case-insensitive."""
    c_norm = normalize_for_compare(community)
    f_norm = normalize_for_compare(floorplan)

    for row in mapping_rows:
        if (normalize_for_compare(row.community) == c_norm
                and normalize_for_compare(row.floorplan) == f_norm):
            return row
    return None
