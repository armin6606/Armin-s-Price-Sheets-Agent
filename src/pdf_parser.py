"""PDF parser - extracts homesite release data from New Release PDFs.

These PDFs follow a standard internal format with:
- Table 0: Header metadata (Community, Phase, Release Date, COE)
- Table 2: Core homesite rows (COE date, HS #, Plan, Plan Elev, Base price)
- Table 3: Premium values (Elevation, HS Size, Detached, Corner, Location/View, Premium Total)
- Table 4: Options (Increase, Option 1, Option 2, Option Total)
- Table 5: Total Released Price
- Table 6: NRCC and Total Price Change
- Table 7: Net Price

Tables 2-7 share the same row alignment (row 1..N = data, last row = TOTALS).
"""

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

import pdfplumber

logger = logging.getLogger("price_sheet_bot.pdf_parser")


@dataclass
class ReleaseMeta:
    """Metadata from the PDF header row."""
    community: str
    phase: str
    release_date: str
    coe: str


@dataclass
class ReleaseHomesite:
    """A single homesite row extracted from a release PDF."""
    coe_date: str          # Close of escrow date for THIS homesite
    homesite: str          # HS #
    plan: str              # Floorplan / Plan number
    plan_elev: str         # Plan Elevation letter(s)
    base_price: str        # Base price
    # Premiums
    elevation_premium: str = ""
    hs_size_premium: str = ""
    detached_premium: str = ""
    corner_premium: str = ""
    location_view_premium: str = ""
    premium_total: str = ""
    # Options
    price_adj_increase: str = ""
    option_1: str = ""
    option_2: str = ""
    option_total: str = ""
    # Totals
    total_released_price: str = ""
    nrcc: str = ""
    total_price_change: str = ""
    net_price: str = ""

    # Derived from metadata
    community: str = ""
    phase: str = ""
    release_date: str = ""
    default_coe: str = ""   # From header, used if row-level COE is blank


@dataclass
class ParsedReleasePDF:
    """Full result of parsing a release PDF."""
    meta: ReleaseMeta
    homesites: List[ReleaseHomesite]
    filename: str = ""
    errors: List[str] = field(default_factory=list)


def _clean_price(raw: str) -> str:
    """Clean a price string: remove spaces inside numbers, normalize $ sign.

    pdfplumber sometimes inserts spaces like '$ 1 ,087,990' or '$ -'
    """
    if not raw or not raw.strip():
        return ""
    s = raw.strip()
    if s == "$" or s == "$ -" or s == "$-" or s == "-":
        return ""
    # Remove ALL spaces between dollar parts, then normalize
    s = s.replace(" ", "")
    # Should now be like $1,087,990 or $1,045,990
    return s


def _safe_get(table: list, row: int, col: int) -> str:
    """Safely get a cell value from a table grid."""
    if row >= len(table):
        return ""
    if col >= len(table[row]):
        return ""
    val = table[row][col]
    if val is None:
        return ""
    return str(val).strip()


def _parse_metadata(table0: list) -> Optional[ReleaseMeta]:
    """Parse Table 0 to extract community, phase, release date, COE.

    Expected format: ['Community:', 'NOVA', 'Phase:', '2D', 'Release Date:', 'February 9, 2026', 'COE:', 'April, 2026', ...]
    """
    if not table0 or not table0[0]:
        return None

    row = table0[0]
    meta = {"community": "", "phase": "", "release_date": "", "coe": ""}

    # Build a flat key-value parse from the row
    for i, cell in enumerate(row):
        if cell is None:
            continue
        cell_lower = str(cell).strip().lower().rstrip(":")
        if cell_lower == "community" and i + 1 < len(row) and row[i + 1]:
            meta["community"] = str(row[i + 1]).strip()
        elif cell_lower == "phase" and i + 1 < len(row) and row[i + 1]:
            meta["phase"] = str(row[i + 1]).strip()
        elif cell_lower in ("release date", "release") and i + 1 < len(row) and row[i + 1]:
            meta["release_date"] = str(row[i + 1]).strip()
        elif cell_lower == "coe" and i + 1 < len(row) and row[i + 1]:
            meta["coe"] = str(row[i + 1]).strip()

    if not meta["community"]:
        return None

    return ReleaseMeta(**meta)


def _is_totals_row(row: list) -> bool:
    """Check if a row is the TOTALS summary row."""
    if not row:
        return False
    first = str(row[0] or "").strip().upper()
    return first in ("TOTALS", "TOTAL")


def _is_valid_homesite(hs: str) -> bool:
    """Check if a homesite value looks like a real homesite number.

    Real homesites are numeric (e.g. '54', '10', '3').
    Rejects junk like 'Option 1', '$ -', 'Square Footage', '2,013', etc.
    """
    if not hs:
        return False
    cleaned = hs.strip()
    # Must be a simple number (digits, maybe with a letter suffix like '10A')
    return bool(re.match(r"^\d+[A-Za-z]?$", cleaned))


def parse_release_pdf(pdf_path: str) -> ParsedReleasePDF:
    """Parse a New Release PDF and extract all homesite data rows.

    Args:
        pdf_path: Local path to the downloaded PDF file.

    Returns:
        ParsedReleasePDF with metadata, list of homesites, and any errors.
    """
    errors = []
    filename = pdf_path.rsplit("\\", 1)[-1].rsplit("/", 1)[-1]

    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        return ParsedReleasePDF(
            meta=ReleaseMeta("", "", "", ""),
            homesites=[],
            filename=filename,
            errors=[f"Failed to open PDF: {e}"],
        )

    try:
        page = pdf.pages[0]
        tables = page.extract_tables()

        if len(tables) < 3:
            return ParsedReleasePDF(
                meta=ReleaseMeta("", "", "", ""),
                homesites=[],
                filename=filename,
                errors=[f"Expected at least 3 tables, found {len(tables)}"],
            )

        # ── Parse metadata from Table 0 ──
        meta = _parse_metadata(tables[0])
        if not meta:
            errors.append("Could not parse metadata from Table 0")
            meta = ReleaseMeta("", "", "", "")

        logger.info(
            "PDF metadata: community=%s phase=%s release_date=%s coe=%s",
            meta.community, meta.phase, meta.release_date, meta.coe,
        )

        # ── Parse core homesite rows from Table 2 ──
        # Table 2 has: Row 0 = headers, Row 1..N-1 = data, Row N = TOTALS
        core_table = tables[2] if len(tables) > 2 else []
        if not core_table or len(core_table) < 2:
            return ParsedReleasePDF(
                meta=meta, homesites=[], filename=filename,
                errors=errors + ["Core table (table 2) is empty or too small"],
            )

        # Determine data row count (skip header row 0 and TOTALS row at end)
        data_rows = []
        for r_idx in range(1, len(core_table)):
            if _is_totals_row(core_table[r_idx]):
                break
            data_rows.append(r_idx)

        num_data = len(data_rows)
        if num_data == 0:
            return ParsedReleasePDF(
                meta=meta, homesites=[], filename=filename,
                errors=errors + ["No data rows found in core table"],
            )

        logger.info("Found %d homesite data rows in PDF.", num_data)

        # ── Extract premium table (Table 3) ──
        premium_table = tables[3] if len(tables) > 3 else []
        # Table 3 headers: Elevation, HS-Size, Detached, Corner, Location/View, Premium Total

        # ── Extract options table (Table 4) ──
        options_table = tables[4] if len(tables) > 4 else []
        # Table 4 headers: Increase, Option 1, Option 2, Option Total

        # ── Extract Total Released Price (Table 5) ──
        released_table = tables[5] if len(tables) > 5 else []

        # ── Extract NRCC / Price Change (Table 6) ──
        nrcc_table = tables[6] if len(tables) > 6 else []
        # Table 6: NRCC, (blank), Total Price Change

        # ── Extract Net Price (Table 7) ──
        net_table = tables[7] if len(tables) > 7 else []

        # ── Build homesite objects ──
        homesites = []
        for i, r_idx in enumerate(data_rows):
            hs = ReleaseHomesite(
                coe_date=_safe_get(core_table, r_idx, 0),
                homesite=_safe_get(core_table, r_idx, 1),
                plan=_safe_get(core_table, r_idx, 2),
                plan_elev=_safe_get(core_table, r_idx, 3),
                base_price=_clean_price(_safe_get(core_table, r_idx, 4)),
                # Premiums (table 3, same row offset: header=0, data starts at 1)
                elevation_premium=_clean_price(_safe_get(premium_table, r_idx, 0)),
                hs_size_premium=_clean_price(_safe_get(premium_table, r_idx, 1)),
                detached_premium=_clean_price(_safe_get(premium_table, r_idx, 2)),
                corner_premium=_clean_price(_safe_get(premium_table, r_idx, 3)),
                location_view_premium=_clean_price(_safe_get(premium_table, r_idx, 4)),
                premium_total=_clean_price(_safe_get(premium_table, r_idx, 5)),
                # Options (table 4, same row offset)
                price_adj_increase=_clean_price(_safe_get(options_table, r_idx, 0)),
                option_1=_clean_price(_safe_get(options_table, r_idx, 1)),
                option_2=_clean_price(_safe_get(options_table, r_idx, 2)),
                option_total=_clean_price(_safe_get(options_table, r_idx, 3)),
                # Total Released Price (table 5)
                total_released_price=_clean_price(_safe_get(released_table, r_idx, 0)),
                # NRCC and Price Change (table 6)
                nrcc=_clean_price(_safe_get(nrcc_table, r_idx, 0)),
                total_price_change=_clean_price(_safe_get(nrcc_table, r_idx, 2) if len(nrcc_table) > r_idx and len(nrcc_table[r_idx]) > 2 else ""),
                # Net Price (table 7)
                net_price=_clean_price(_safe_get(net_table, r_idx, 0)),
                # Metadata
                community=meta.community,
                phase=meta.phase,
                release_date=meta.release_date,
                default_coe=meta.coe,
            )

            # Validate: must have a real numeric homesite and a plan
            if not hs.homesite or not hs.plan:
                errors.append(f"Data row {r_idx}: missing homesite or plan, skipping")
                continue
            if not _is_valid_homesite(hs.homesite):
                logger.warning(
                    "Data row %d: homesite '%s' is not a valid number, skipping (likely PDF footer/summary data)",
                    r_idx, hs.homesite,
                )
                continue

            homesites.append(hs)
            logger.info(
                "  HS %s: plan=%s elev=%s base=%s net=%s",
                hs.homesite, hs.plan, hs.plan_elev, hs.base_price, hs.net_price,
            )

        return ParsedReleasePDF(
            meta=meta,
            homesites=homesites,
            filename=filename,
            errors=errors,
        )

    finally:
        pdf.close()


def parse_release_filename(filename: str) -> Optional[dict]:
    """Parse community and phase from a release PDF filename.

    Expected patterns:
    - 'Nova Phase 2D.pdf' -> community=Nova, phase=2D
    - 'Isla Phase 3A.pdf' -> community=Isla, phase=3A
    - 'Cielo Vista Phase 1B.pdf' -> community=Cielo Vista, phase=1B
    - Also supports old format: 'Community_Homesite_Floorplan.pdf'

    Returns dict with 'community' and 'phase' keys, or None if can't parse.
    """
    if not filename:
        return None

    # Remove extension
    name = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE).strip()
    if not name:
        return None

    # Try "Community Phase XX" pattern (most common for release PDFs)
    # Phase part is typically like: 2D, 3A, 1B, etc. (number + optional letter(s))
    m = re.match(r"^(.+?)\s+Phase\s+(\S+)$", name, re.IGNORECASE)
    if m:
        return {
            "community": m.group(1).strip(),
            "phase": m.group(2).strip(),
        }

    # Fallback: just use the whole name as community, no phase
    # (the PDF content itself will have the real metadata)
    return {
        "community": name,
        "phase": "",
    }
