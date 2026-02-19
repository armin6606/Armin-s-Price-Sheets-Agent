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

        # ── Find the core homesite table (the one with HS # column) ──
        # Some PDFs have core data in Table 1 (merged with premiums),
        # others have it in Table 2.  Detect by looking for "HS" header.
        core_table_idx = None
        core_header_row = None
        hs_col = 1  # default column for homesite number
        for t_idx in range(1, len(tables)):
            tbl = tables[t_idx]
            # Check first 2 rows for a header containing "HS"
            for r_idx in range(min(2, len(tbl))):
                row = tbl[r_idx]
                for c_idx, cell in enumerate(row):
                    if cell and re.search(r"\bHS\b", str(cell), re.IGNORECASE):
                        core_table_idx = t_idx
                        core_header_row = r_idx
                        hs_col = c_idx
                        break
                if core_table_idx is not None:
                    break
            if core_table_idx is not None:
                break

        if core_table_idx is None:
            # Fallback: assume Table 2 like before
            logger.warning("Could not find HS# header; falling back to Table 2")
            core_table_idx = 2
            core_header_row = 0
            hs_col = 1

        core_table = tables[core_table_idx] if len(tables) > core_table_idx else []
        if not core_table or len(core_table) < 2:
            return ParsedReleasePDF(
                meta=meta, homesites=[], filename=filename,
                errors=errors + [f"Core table (table {core_table_idx}) is empty or too small"],
            )

        logger.info(
            "Core homesite table: table[%d], header row %d, HS# in col %d",
            core_table_idx, core_header_row, hs_col,
        )

        # Detect column layout from the header row
        header = core_table[core_header_row]
        col_map = {}  # Maps logical name -> column index
        for c_idx, cell in enumerate(header):
            if not cell:
                continue
            cell_upper = str(cell).strip().upper()
            if "COE" in cell_upper:
                col_map["coe"] = c_idx
            elif cell_upper in ("HS #", "HS", "HS#"):
                col_map["hs"] = c_idx
            elif cell_upper in ("PLAN", "PLAN #"):
                col_map["plan"] = c_idx
            elif "ELEV" in cell_upper:
                col_map["elev"] = c_idx
            elif "BASE" in cell_upper:
                col_map["base"] = c_idx
        # Ensure defaults if not found
        col_map.setdefault("coe", 0)
        col_map.setdefault("hs", hs_col)
        col_map.setdefault("plan", 2)
        col_map.setdefault("elev", 3)
        col_map.setdefault("base", 4)

        # Determine data rows (after header, before TOTALS)
        data_rows = []
        for r_idx in range(core_header_row + 1, len(core_table)):
            if _is_totals_row(core_table[r_idx]):
                break
            # Skip completely empty rows
            row_vals = [str(c or "").strip() for c in core_table[r_idx]]
            if not any(row_vals):
                break
            data_rows.append(r_idx)

        num_data = len(data_rows)
        if num_data == 0:
            return ParsedReleasePDF(
                meta=meta, homesites=[], filename=filename,
                errors=errors + ["No data rows found in core table"],
            )

        logger.info("Found %d homesite data rows in PDF.", num_data)

        # ── Identify remaining tables by header keywords ──
        # After the core table, locate Options, Released Price, NRCC, Net Price
        options_table = []
        released_table = []
        nrcc_table = []
        net_table = []

        for t_idx in range(len(tables)):
            if t_idx == 0 or t_idx == core_table_idx:
                continue
            tbl = tables[t_idx]
            if not tbl or not tbl[0]:
                continue
            # Check first 2 rows for identifying keywords
            header_text = " ".join(str(c or "") for row in tbl[:2] for c in row).upper()
            if "OPTION" in header_text and "INCREASE" in header_text and not options_table:
                options_table = tbl
            elif "TOTAL" in header_text and "RELEASED" in header_text and not released_table:
                released_table = tbl
            elif "NRCC" in header_text and not nrcc_table:
                nrcc_table = tbl
            elif "NET PRICE" in header_text and not net_table:
                net_table = tbl

        # The row offset for companion tables: they share the same row alignment
        # Data starts after their own header rows.  We'll map by data row index.
        def _companion_data_start(tbl):
            """Find the first data row in a companion table (after its header)."""
            for r in range(min(2, len(tbl))):
                # If a row has a dollar value, that's data
                for c in tbl[r]:
                    if c and "$" in str(c):
                        return r
            return 1  # default: row 1

        opt_start = _companion_data_start(options_table) if options_table else 1
        rel_start = _companion_data_start(released_table) if released_table else 1
        nrcc_start = _companion_data_start(nrcc_table) if nrcc_table else 1
        net_start = _companion_data_start(net_table) if net_table else 1

        # ── Build homesite objects ──
        homesites = []
        for i, r_idx in enumerate(data_rows):
            # Map companion table row index from data row order
            opt_r = opt_start + i
            rel_r = rel_start + i
            nrcc_r = nrcc_start + i
            net_r = net_start + i

            hs = ReleaseHomesite(
                coe_date=_safe_get(core_table, r_idx, col_map["coe"]),
                homesite=_safe_get(core_table, r_idx, col_map["hs"]),
                plan=_safe_get(core_table, r_idx, col_map["plan"]),
                plan_elev=_safe_get(core_table, r_idx, col_map["elev"]),
                base_price=_clean_price(_safe_get(core_table, r_idx, col_map["base"])),
                # Options
                price_adj_increase=_clean_price(_safe_get(options_table, opt_r, 0)),
                option_1=_clean_price(_safe_get(options_table, opt_r, 1)),
                option_2=_clean_price(_safe_get(options_table, opt_r, 2)),
                option_total=_clean_price(_safe_get(options_table, opt_r, 3)),
                # Total Released Price
                total_released_price=_clean_price(_safe_get(released_table, rel_r, 0)),
                # NRCC and Price Change
                nrcc=_clean_price(_safe_get(nrcc_table, nrcc_r, 0)),
                total_price_change=_clean_price(
                    _safe_get(nrcc_table, nrcc_r, 2)
                    if len(nrcc_table) > nrcc_r and len(nrcc_table[nrcc_r]) > 2
                    else _safe_get(nrcc_table, nrcc_r, 1)
                ),
                # Net Price
                net_price=_clean_price(_safe_get(net_table, net_r, 0)),
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
