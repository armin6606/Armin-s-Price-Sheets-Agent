"""CONTROL tab parser - reads and filters control rows from Google Sheet."""

import logging
from dataclasses import dataclass
from typing import List, Optional

from .utils import normalize_for_compare

logger = logging.getLogger("price_sheet_bot.control")


@dataclass
class ControlRow:
    """A single row from the CONTROL tab."""
    enabled: bool
    community: str
    homesite: str
    floorplan: str
    price: str
    address: str
    ready_by: str   # Parsed/formatted move-in date (MM/DD/YYYY) used in Word templates
    move_in: str    # Raw move-in date as shown in Google Sheet (may be "April, 2026" etc.)
    notes: str
    row_index: int  # 1-based row in sheet (for debugging)

    @staticmethod
    def from_dict(record: dict, row_index: int) -> Optional["ControlRow"]:
        """Parse a dict (from gspread) into a ControlRow. Returns None if invalid."""
        enabled_raw = str(record.get("enabled", "")).strip().upper()
        if enabled_raw not in ("TRUE", "1", "YES"):
            return None

        community = str(record.get("community", "")).strip()
        homesite = str(record.get("homesite", "")).strip()
        floorplan = str(record.get("floorplan", "")).strip()

        if not community or not homesite or not floorplan:
            logger.warning(
                "CONTROL row %d missing community/homesite/floorplan, skipping.",
                row_index,
            )
            return None

        # Move-in date: read from "move_in" or "move in" column if present,
        # otherwise fall back to ready_by column.  The ready_by field stores
        # the parsed MM/DD/YYYY version used for the Word template.
        move_in_raw = str(
            record.get("move_in",
            record.get("move in",
            record.get("move in date",
            record.get("movein", "")
        )))).strip()

        ready_by_raw = str(record.get("ready_by", record.get("ready by", ""))).strip()

        # If move_in is present but ready_by is blank, use move_in as the source
        # for ready_by (after date parsing).  If both present, ready_by wins for
        # the Word template (it may already be formatted).
        if move_in_raw and not ready_by_raw:
            ready_by_raw = move_in_raw
        elif not move_in_raw and ready_by_raw:
            move_in_raw = ready_by_raw  # Mirror back so sheet shows it

        return ControlRow(
            enabled=True,
            community=community,
            homesite=homesite,
            floorplan=floorplan,
            price=str(record.get("price", "")).strip(),
            address=str(record.get("address", "")).strip(),
            ready_by=ready_by_raw,
            move_in=move_in_raw,
            notes=str(record.get("notes", "")).strip(),
            row_index=row_index,
        )


def parse_control_tab(records: list) -> List[ControlRow]:
    """Parse all records from the CONTROL tab into ControlRow objects.

    Only returns enabled rows with valid community/homesite/floorplan.
    """
    rows = []
    for i, record in enumerate(records):
        row = ControlRow.from_dict(record, row_index=i + 2)  # +2: header is row 1
        if row is not None:
            rows.append(row)
    logger.info("Parsed %d enabled CONTROL rows from %d total.", len(rows), len(records))
    return rows


def find_control_row(
    control_rows: List[ControlRow],
    community: str,
    homesite: str,
    floorplan: str,
) -> Optional[ControlRow]:
    """Find a CONTROL row matching (community, homesite, floorplan) case-insensitive."""
    c_norm = normalize_for_compare(community)
    h_norm = normalize_for_compare(homesite)
    f_norm = normalize_for_compare(floorplan)

    for row in control_rows:
        if (normalize_for_compare(row.community) == c_norm
                and normalize_for_compare(row.homesite) == h_norm
                and normalize_for_compare(row.floorplan) == f_norm):
            return row
    return None
