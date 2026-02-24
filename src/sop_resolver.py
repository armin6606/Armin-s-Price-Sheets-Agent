"""SOP address resolver - looks up addresses from SOP folder files.

Supports two file types:
  1. Structured files (CSV/XLSX) with community, homesite, address columns
  2. SOP PDF files parsed with pdfplumber (schedule of properties PDFs)
"""

import csv
import io
import logging
import os
import re
from typing import Dict, List, Optional, Tuple

import pdfplumber

from .utils import normalize_for_compare

logger = logging.getLogger("price_sheet_bot.sop")

# ── Module-level cache ──
# {sop_folder_id: {(community_norm, homesite_norm): address}}
_address_cache: Dict[str, Dict[Tuple[str, str], str]] = {}


def _clear_cache():
    """Clear the SOP address cache (for testing)."""
    _address_cache.clear()


# ── Community name extraction from SOP filename ──

_COMMUNITY_PATTERNS = [
    # "02 ISLA (BA 602)_..." → ISLA
    re.compile(r"^\d*\s*([A-Za-z ]+?)\s*\(", re.IGNORECASE),
    # "SIGNED LENNAR Schedule of Properties Nova AQ12 Phase 2 ..." → Nova
    re.compile(r"Schedule of Properties\s+([A-Za-z]+)", re.IGNORECASE),
    # "Sella SOP - Phase 8 - 11.6.24 - signed.pdf" → Sella
    re.compile(r"^([A-Za-z ]+?)\s+SOP\b", re.IGNORECASE),
    # "STRATA_(AQ22)_SOP_PH2..." → STRATA
    re.compile(r"^([A-Za-z ]+?)[\s_]*\(", re.IGNORECASE),
]


def _extract_community_from_filename(filename: str) -> str:
    """Extract the community name from a SOP PDF filename."""
    name = os.path.splitext(filename)[0].strip()
    for pattern in _COMMUNITY_PATTERNS:
        m = pattern.search(name)
        if m:
            return m.group(1).strip()
    # Fallback: first word(s) before underscore/digit
    m = re.match(r"^(\d+\s+)?([A-Za-z ]+)", name)
    if m:
        return m.group(2).strip()
    return ""


# ── PDF SOP parser ──

def _parse_sop_pdf(pdf_bytes: bytes) -> List[dict]:
    """Parse a SOP PDF and extract homesite → address mappings.

    Returns list of dicts: [{"homesite": "27", "address": "680 Furrow Rd."}, ...]
    """
    results = []
    try:
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
    except Exception as e:
        logger.warning("Could not open SOP PDF: %s", e)
        return results

    try:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or not table[0]:
                    continue

                # Normalize header row
                raw_headers = table[0]
                headers = [
                    re.sub(r"\s+", " ", str(c or "").strip().lower())
                    for c in raw_headers
                ]

                # Find homesite and address columns
                hs_col = -1
                addr_col = -1
                for i, h in enumerate(headers):
                    if h in ("homesite #", "home site #", "homesite", "home site",
                             "hs", "hs #"):
                        hs_col = i
                    elif h == "address":
                        addr_col = i

                if hs_col < 0 or addr_col < 0:
                    continue

                # Extract data rows
                for row in table[1:]:
                    if hs_col >= len(row) or addr_col >= len(row):
                        continue

                    hs_val = str(row[hs_col] or "").strip()
                    addr_val = str(row[addr_col] or "").strip()

                    # Skip blank, building-level (BD prefix), TOTAL, and sub-header rows
                    if not hs_val or not addr_val:
                        continue
                    if hs_val.upper().startswith("BD"):
                        continue
                    if "TOTAL" in hs_val.upper():
                        continue
                    if addr_val in ("-", "None", ""):
                        continue
                    # Skip rows where address looks like a building reference
                    if addr_val.upper().startswith("SHELL"):
                        continue

                    results.append({"homesite": hs_val, "address": addr_val})

    finally:
        pdf.close()

    return results


def _build_sop_cache(drive_client, sop_folder_id: str) -> Dict[Tuple[str, str], str]:
    """Build a complete (community, homesite) → address cache from ALL SOP files.

    Downloads and parses every PDF in the SOP folder. Also checks CSV/XLSX.
    Results are cached at module level so subsequent calls are instant.
    """
    if sop_folder_id in _address_cache:
        return _address_cache[sop_folder_id]

    cache: Dict[Tuple[str, str], str] = {}

    try:
        files = drive_client.list_files(sop_folder_id)
    except Exception as e:
        logger.warning("Failed to list SOP folder: %s", e)
        _address_cache[sop_folder_id] = cache
        return cache

    logger.info("Building SOP address cache from %d file(s)...", len(files))

    for f in files:
        fname = f["name"]
        community = _extract_community_from_filename(fname)
        if not community:
            logger.debug("Could not extract community from SOP file '%s'", fname)
            continue

        c_norm = normalize_for_compare(community)

        if fname.lower().endswith(".pdf"):
            # Parse PDF
            try:
                pdf_bytes = drive_client.download_to_bytes(f["id"])
                rows = _parse_sop_pdf(pdf_bytes)
                for row in rows:
                    h_norm = normalize_for_compare(row["homesite"])
                    key = (c_norm, h_norm)
                    if key not in cache:
                        cache[key] = row["address"]
                if rows:
                    logger.info(
                        "SOP PDF '%s': extracted %d address(es) for community=%s",
                        fname, len(rows), community,
                    )
            except Exception as e:
                logger.warning("Failed to parse SOP PDF '%s': %s", fname, e)

        elif (fname.lower().endswith(".csv")
              or fname.lower().endswith(".xlsx")
              or f.get("mimeType") == "application/vnd.google-apps.spreadsheet"):
            # Parse CSV (existing logic)
            try:
                data = drive_client.download_to_bytes(f["id"])
                if fname.lower().endswith(".csv"):
                    _load_csv_into_cache(data, c_norm, cache)
            except Exception as e:
                logger.warning("Failed to parse SOP file '%s': %s", fname, e)

    _address_cache[sop_folder_id] = cache
    logger.info("SOP address cache built: %d total address(es)", len(cache))
    return cache


def _load_csv_into_cache(
    data: bytes,
    default_community_norm: str,
    cache: Dict[Tuple[str, str], str],
):
    """Load CSV address data into the cache dict."""
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return

    field_map = {f.strip().lower(): f for f in reader.fieldnames}

    comm_key = None
    hs_key = None
    addr_key = None

    for norm, orig in field_map.items():
        if norm in ("community", "comm"):
            comm_key = orig
        elif norm in ("homesite", "home site", "site", "hs"):
            hs_key = orig
        elif norm in ("address", "property address", "street address"):
            addr_key = orig

    if not hs_key or not addr_key:
        return

    for row in reader:
        c = normalize_for_compare(row.get(comm_key, "")) if comm_key else default_community_norm
        h = normalize_for_compare(row.get(hs_key, ""))
        addr = row.get(addr_key, "").strip()
        if c and h and addr:
            key = (c, h)
            if key not in cache:
                cache[key] = addr


# ── Legacy structured-file resolver (kept for backward compat) ──

def resolve_address_from_structured(
    drive_client,
    sop_folder_id: str,
    community: str,
    homesite: str,
    floorplan: str = "",
) -> Optional[str]:
    """Try to find address from a structured file (CSV/XLSX) in the SOP folder."""
    try:
        files = drive_client.list_files(sop_folder_id)
    except Exception as e:
        logger.warning("Failed to list SOP folder: %s", e)
        return None

    address_files = [
        f for f in files
        if "address" in f["name"].lower()
        and (f["name"].lower().endswith(".csv")
             or f["name"].lower().endswith(".xlsx")
             or f["mimeType"] == "application/vnd.google-apps.spreadsheet")
    ]

    if not address_files:
        return None

    c_norm = normalize_for_compare(community)
    h_norm = normalize_for_compare(homesite)

    for af in address_files:
        try:
            data = drive_client.download_to_bytes(af["id"])
            if af["name"].lower().endswith(".csv"):
                address = _search_csv(data, c_norm, h_norm)
                if address:
                    logger.info("Found address from SOP CSV '%s': %s", af["name"], address)
                    return address
        except Exception as e:
            logger.warning("Failed to parse SOP file '%s': %s", af["name"], e)

    return None


def _search_csv(data: bytes, community_norm: str, homesite_norm: str) -> Optional[str]:
    """Search a CSV for an address matching community + homesite."""
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return None

    field_map = {f.strip().lower(): f for f in reader.fieldnames}

    comm_key = None
    hs_key = None
    addr_key = None

    for norm, orig in field_map.items():
        if norm in ("community", "comm"):
            comm_key = orig
        elif norm in ("homesite", "home site", "site", "hs"):
            hs_key = orig
        elif norm in ("address", "property address", "street address"):
            addr_key = orig

    if not comm_key or not hs_key or not addr_key:
        return None

    for row in reader:
        c = normalize_for_compare(row.get(comm_key, ""))
        h = normalize_for_compare(row.get(hs_key, ""))
        if c == community_norm and h == homesite_norm:
            addr = row.get(addr_key, "").strip()
            if addr:
                return addr

    return None


# ── Public API ──

def resolve_address(
    drive_client,
    sop_folder_id: str,
    community: str,
    homesite: str,
    floorplan: str = "",
) -> Optional[str]:
    """Try all SOP sources to resolve an address.

    Priority:
    1. Cached SOP data (PDFs + CSVs, loaded once per run)
    2. Legacy structured file lookup (CSV/XLSX with 'address' in name)
    """
    c_norm = normalize_for_compare(community)
    h_norm = normalize_for_compare(homesite)

    # Check the combined cache (PDFs + CSVs)
    cache = _build_sop_cache(drive_client, sop_folder_id)
    addr = cache.get((c_norm, h_norm))
    if addr:
        logger.info(
            "SOP address found for community=%s homesite=%s: %s",
            community, homesite, addr,
        )
        return addr

    # Fallback: legacy structured file search (in case cache missed something)
    result = resolve_address_from_structured(
        drive_client, sop_folder_id, community, homesite, floorplan
    )
    if result:
        return result

    logger.info(
        "SOP address resolution failed for community=%s homesite=%s. "
        "Address will remain blank.",
        community, homesite,
    )
    return None
