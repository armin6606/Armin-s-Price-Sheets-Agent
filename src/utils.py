"""Utility functions for Price Sheet Bot."""

import hashlib
import re
import string
from datetime import date, datetime, timedelta
from typing import Optional

# Month name lookup for parsing date strings like "April, 2026" or "April 15, 2026"
_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


# ── Header normalization ──

def normalize_header(text: str) -> str:
    """Normalize a header cell for matching.

    Steps: trim, uppercase, replace - and _ with space,
    remove other punctuation, collapse whitespace.
    """
    if not text:
        return ""
    s = text.strip().upper()
    s = s.replace("-", " ").replace("_", " ")
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


HEADER_ALIASES = {
    "SITE": ["SITE", "HOMESITE", "HOME SITE", "HS"],
    "PRICE": ["PRICE", "SALES PRICE", "FINAL PRICE"],
    "ADDRESS": ["ADDRESS", "PROPERTY ADDRESS"],
    "READY BY": ["READY BY", "READYBY", "READY BY DATE", "MOVE IN", "MOVEIN", "MOVE IN DATE"],
    "NOTES": ["NOTES", "NOTE"],
}

REQUIRED_HEADERS = ["SITE", "PRICE", "READY BY"]
OPTIONAL_HEADERS = ["ADDRESS", "NOTES"]


def resolve_header(normalized: str) -> Optional[str]:
    """Resolve a normalized header string to its canonical name."""
    for canonical, aliases in HEADER_ALIASES.items():
        if normalized in aliases:
            return canonical
    return None


def build_header_map(cells: list) -> dict:
    """Build {canonical_name: column_index} from a list of header cell texts.

    Returns dict like {"SITE": 0, "PRICE": 1, ...}
    """
    hmap = {}
    for idx, cell_text in enumerate(cells):
        norm = normalize_header(cell_text)
        canonical = resolve_header(norm)
        if canonical and canonical not in hmap:
            hmap[canonical] = idx
    return hmap


def validate_headers(header_map: dict, strict: bool = True) -> list:
    """Validate that required headers are present. Returns list of missing."""
    missing = [h for h in REQUIRED_HEADERS if h not in header_map]
    return missing


# ── Price formatting ──

def format_price(value) -> str:
    """Format price as $1,234,567 (no decimals)."""
    if value is None or str(value).strip() == "":
        return ""
    s = str(value).strip()
    # Remove existing formatting
    s = s.replace("$", "").replace(",", "").strip()
    try:
        num = float(s)
        return f"${int(num):,}"
    except (ValueError, TypeError):
        return str(value).strip()


# ── Date parsing ──

SHEETS_EPOCH = date(1899, 12, 30)

def parse_ready_by(value) -> str:
    """Parse ready_by into MM/DD/YYYY zero-padded string.

    Accepts:
      - MM/DD/YYYY (e.g. "04/15/2026")
      - YYYY-MM-DD (e.g. "2026-04-15")
      - Python date or datetime object
      - Google Sheets serial number (e.g. 45849)
      - "Month Day, Year" (e.g. "April 15, 2026") -> "04/15/2026"
      - "Month, Year" or "Month Year" (e.g. "April, 2026") -> "04/01/2026"
    """
    if value is None or str(value).strip() == "":
        return ""

    if isinstance(value, datetime):
        return value.strftime("%m/%d/%Y")
    if isinstance(value, date):
        return value.strftime("%m/%d/%Y")

    s = str(value).strip()

    # Try Sheets serial number (pure digits or float)
    try:
        serial = float(s)
        if 1 < serial < 200000:  # reasonable range for dates
            d = SHEETS_EPOCH + timedelta(days=int(serial))
            return d.strftime("%m/%d/%Y")
    except ValueError:
        pass

    # Try MM/DD/YYYY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{month:02d}/{day:02d}/{year}"

    # Try YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{month:02d}/{day:02d}/{year}"

    # Try "Month Day, Year" e.g. "April 15, 2026"
    m = re.match(r"^([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})$", s)
    if m:
        month_name = m.group(1).lower()
        month_num = _MONTH_NAMES.get(month_name)
        if month_num:
            day = int(m.group(2))
            year = int(m.group(3))
            return f"{month_num:02d}/{day:02d}/{year}"

    # Try "Month, Year" or "Month Year" e.g. "April, 2026" or "April 2026"
    # Use day 1 as default
    m = re.match(r"^([A-Za-z]+),?\s+(\d{4})$", s)
    if m:
        month_name = m.group(1).lower()
        month_num = _MONTH_NAMES.get(month_name)
        if month_num:
            year = int(m.group(2))
            return f"{month_num:02d}/01/{year}"

    return s


# ── Filename parsing ──

def parse_pdf_filename(filename: str) -> Optional[dict]:
    """Parse community, homesite, floorplan from PDF filename.

    Expected patterns:
    - Community_HomesteNumber_FloorplanNumber.pdf
    - Community HomesteNumber FloorplanNumber.pdf
    - Various separators: _, -, space

    Returns dict with keys: community, homesite, floorplan or None.
    """
    if not filename:
        return None

    # Remove extension
    name = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE).strip()
    if not name:
        return None

    # Split on common separators (underscore, hyphen, space, multiple)
    # Try underscore first, then space, then hyphen
    for sep_pattern in [r"[_]", r"[\s]+", r"[-]"]:
        parts = re.split(sep_pattern, name)
        parts = [p.strip() for p in parts if p.strip()]
        if len(parts) >= 3:
            community = parts[0]
            homesite = parts[1]
            floorplan = parts[2]
            return {
                "community": community,
                "homesite": homesite,
                "floorplan": floorplan,
            }

    # Fallback: try to find numbers at the end
    m = re.match(r"^(.+?)\s*(\d+)\s*(\d+)$", name)
    if m:
        return {
            "community": m.group(1).strip(),
            "homesite": m.group(2),
            "floorplan": m.group(3),
        }

    return None


# ── Hashing ──

def compute_hash(data: bytes) -> str:
    """Compute SHA-256 hash of bytes."""
    return hashlib.sha256(data).hexdigest()


# ── String comparison ──

def normalize_for_compare(value) -> str:
    """Normalize a value for case-insensitive trimmed comparison."""
    if value is None:
        return ""
    return str(value).strip().upper()
