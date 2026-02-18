"""Google Sheets client for reading AND writing CONTROL and MAPPING tabs.

Includes rate-limiting with exponential backoff to avoid Google API 429 errors,
and batch upsert operations to minimise API calls.
"""

import logging
import time
from typing import Optional, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger("price_sheet_bot.sheets")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",   # Read + Write
    "https://www.googleapis.com/auth/drive",
]

# Rate-limit settings
_MIN_CALL_INTERVAL = 1.1     # seconds between API calls (Google allows ~60/min)
_MAX_RETRIES = 5
_BASE_BACKOFF = 2.0           # seconds


class SheetsClient:
    """Reads and writes data to the Google Sheet (CONTROL + MAPPING tabs).

    Includes built-in rate limiting and exponential backoff for 429 errors.
    """

    def __init__(self, credentials_path: str, spreadsheet_id: str):
        self.credentials_path = credentials_path
        self.spreadsheet_id = spreadsheet_id
        self._client: Optional[gspread.Client] = None
        self._spreadsheet: Optional[gspread.Spreadsheet] = None
        self._last_api_call: float = 0.0

    # ── Rate limiting helpers ──

    def _throttle(self):
        """Ensure minimum interval between API calls."""
        elapsed = time.time() - self._last_api_call
        if elapsed < _MIN_CALL_INTERVAL:
            time.sleep(_MIN_CALL_INTERVAL - elapsed)
        self._last_api_call = time.time()

    def _api_call_with_retry(self, func, *args, **kwargs):
        """Execute an API call with throttle + exponential backoff on 429."""
        for attempt in range(_MAX_RETRIES):
            self._throttle()
            try:
                result = func(*args, **kwargs)
                return result
            except gspread.exceptions.APIError as e:
                if "429" in str(e) and attempt < _MAX_RETRIES - 1:
                    wait = _BASE_BACKOFF * (2 ** attempt)
                    logger.warning(
                        "Rate limited (429). Waiting %.1fs before retry %d/%d...",
                        wait, attempt + 1, _MAX_RETRIES,
                    )
                    time.sleep(wait)
                else:
                    raise
        # Should not reach here, but just in case
        raise RuntimeError("Max retries exceeded for API call.")

    # ── Connection ──

    def connect(self):
        """Authenticate and open the spreadsheet."""
        creds = Credentials.from_service_account_file(
            self.credentials_path, scopes=SCOPES
        )
        self._client = gspread.authorize(creds)
        self._spreadsheet = self._api_call_with_retry(
            self._client.open_by_key, self.spreadsheet_id
        )
        logger.info("Connected to spreadsheet: %s", self.spreadsheet_id)

    @property
    def spreadsheet(self) -> gspread.Spreadsheet:
        if self._spreadsheet is None:
            raise RuntimeError("SheetsClient not connected. Call connect() first.")
        return self._spreadsheet

    # ── Read operations ──

    def get_all_records(self, tab_name: str) -> list:
        """Get all records from a tab as list of dicts (header-keyed).

        Uses case-insensitive, trimmed keys.
        """
        ws = self._get_worksheet(tab_name)
        rows = self._api_call_with_retry(ws.get_all_values)
        if len(rows) < 2:
            return []

        raw_headers = rows[0]
        headers = [h.strip().lower() for h in raw_headers]

        records = []
        for row in rows[1:]:
            record = {}
            for i, header in enumerate(headers):
                if header:
                    val = row[i] if i < len(row) else ""
                    record[header] = val
            records.append(record)

        return records

    def _get_worksheet(self, tab_name: str) -> gspread.Worksheet:
        """Get a worksheet by name (internal, no retry needed on worksheet lookup)."""
        try:
            return self.spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            raise ValueError(f"Tab '{tab_name}' not found in spreadsheet.")

    def get_worksheet(self, tab_name: str) -> gspread.Worksheet:
        """Get a worksheet by name (public)."""
        return self._get_worksheet(tab_name)

    def get_headers(self, tab_name: str) -> list:
        """Get header row (row 1) from a tab as a list of lowercase trimmed strings."""
        ws = self._get_worksheet(tab_name)
        rows = self._api_call_with_retry(ws.get_all_values)
        if not rows:
            return []
        return [h.strip().lower() for h in rows[0]]

    def find_row(self, tab_name: str, col_name: str, value: str) -> Optional[int]:
        """Find the 1-based row number where col_name == value (case-insensitive).

        Returns None if not found.
        """
        ws = self._get_worksheet(tab_name)
        rows = self._api_call_with_retry(ws.get_all_values)
        if len(rows) < 2:
            return None

        headers = [h.strip().lower() for h in rows[0]]
        col_norm = col_name.strip().lower()
        if col_norm not in headers:
            return None
        col_idx = headers.index(col_norm)

        val_norm = str(value).strip().upper()
        for i, row in enumerate(rows[1:], start=2):
            if col_idx < len(row):
                if str(row[col_idx]).strip().upper() == val_norm:
                    return i
        return None

    # ── Write operations ──

    def append_row(self, tab_name: str, row_data: list):
        """Append a single row to the end of a tab."""
        ws = self._get_worksheet(tab_name)
        self._api_call_with_retry(
            ws.append_row, row_data, value_input_option="USER_ENTERED"
        )
        logger.info("Appended row to '%s': %s", tab_name, row_data[:3])

    def update_cell(self, tab_name: str, row: int, col: int, value: str):
        """Update a single cell (1-based row, 1-based col)."""
        ws = self._get_worksheet(tab_name)
        self._api_call_with_retry(ws.update_cell, row, col, value)

    def update_row(self, tab_name: str, row_number: int, row_data: list):
        """Update an entire row by 1-based row number."""
        ws = self._get_worksheet(tab_name)
        end_col = chr(ord('A') + len(row_data) - 1) if len(row_data) <= 26 else 'Z'
        cell_range = f"A{row_number}:{end_col}{row_number}"
        self._api_call_with_retry(
            ws.update, cell_range, [row_data], value_input_option="USER_ENTERED"
        )
        logger.info("Updated row %d in '%s'", row_number, tab_name)

    # ── Single upsert (kept for small operations) ──

    def upsert_control_row(self, tab_name: str, community: str, homesite: str,
                            floorplan: str, price: str, address: str,
                            ready_by: str, notes: str,
                            move_in: str = "") -> str:
        """Insert or update a row in the CONTROL tab.

        Matches on (community, homesite, floorplan).
        If found, updates price/address/ready_by/move_in/notes.
        If not found, appends a new row.

        Returns: "inserted", "updated", or "skipped" (if values unchanged).
        """
        ws = self._get_worksheet(tab_name)
        rows = self._api_call_with_retry(ws.get_all_values)
        if not rows:
            self._api_call_with_retry(
                ws.append_row,
                ["enabled", "community", "homesite", "floorplan",
                 "price", "address", "ready_by", "move_in", "notes"],
                value_input_option="USER_ENTERED",
            )
            self._api_call_with_retry(
                ws.append_row,
                ["TRUE", community, homesite, floorplan,
                 price, address, ready_by, move_in or ready_by, notes],
                value_input_option="USER_ENTERED",
            )
            return "inserted"

        headers = [h.strip().lower() for h in rows[0]]
        col_map = _build_col_map(headers)

        if col_map["community"] < 0 or col_map["homesite"] < 0 or col_map["floorplan"] < 0:
            logger.error("CONTROL tab missing required columns (community, homesite, floorplan)")
            return "skipped"

        # Search for existing row
        c_upper = community.strip().upper()
        h_upper = str(homesite).strip().upper()
        f_upper = str(floorplan).strip().upper()

        for i, row in enumerate(rows[1:], start=2):
            if (_get_cell(row, col_map["community"]).upper() == c_upper
                    and _get_cell(row, col_map["homesite"]).upper() == h_upper
                    and _get_cell(row, col_map["floorplan"]).upper() == f_upper):
                # Found existing row - update changed fields
                changed = False

                if col_map["price"] >= 0 and price:
                    old_val = _get_cell(row, col_map["price"])
                    if old_val != str(price).strip():
                        self._api_call_with_retry(
                            ws.update_cell, i, col_map["price"] + 1, price
                        )
                        changed = True

                if col_map["address"] >= 0 and address:
                    old_val = _get_cell(row, col_map["address"])
                    if not old_val:  # Only fill if currently blank
                        self._api_call_with_retry(
                            ws.update_cell, i, col_map["address"] + 1, address
                        )
                        changed = True

                if col_map["ready_by"] >= 0 and ready_by:
                    old_val = _get_cell(row, col_map["ready_by"])
                    if old_val != str(ready_by).strip():
                        self._api_call_with_retry(
                            ws.update_cell, i, col_map["ready_by"] + 1, ready_by
                        )
                        changed = True

                # move_in: update if blank or different
                mi_val = move_in or ready_by
                if col_map.get("move_in", -1) >= 0 and mi_val:
                    old_val = _get_cell(row, col_map["move_in"])
                    if not old_val:
                        self._api_call_with_retry(
                            ws.update_cell, i, col_map["move_in"] + 1, mi_val
                        )
                        changed = True

                if col_map["notes"] >= 0 and notes:
                    old_val = _get_cell(row, col_map["notes"])
                    if not old_val:  # Only fill if currently blank
                        self._api_call_with_retry(
                            ws.update_cell, i, col_map["notes"] + 1, notes
                        )
                        changed = True

                return "updated" if changed else "skipped"

        # Not found - append new row
        new_row = _build_new_row(headers, col_map, community, homesite,
                                  floorplan, price, address, ready_by, notes,
                                  move_in=move_in)
        self._api_call_with_retry(
            ws.append_row, new_row, value_input_option="USER_ENTERED"
        )
        return "inserted"

    # ── Batch upsert (for syncing many rows efficiently) ──

    def batch_upsert_control_rows(
        self,
        tab_name: str,
        rows_to_upsert: List[dict],
    ) -> List[Tuple[dict, str]]:
        """Batch insert/update multiple rows in the CONTROL tab.

        Reads the sheet ONCE, computes all changes in memory, then writes
        updates in batched API calls. This avoids 429 rate-limit errors
        when syncing many rows.

        Args:
            tab_name: Name of the CONTROL tab.
            rows_to_upsert: List of dicts, each with keys:
                community, homesite, floorplan, price, address, ready_by,
                move_in (optional), notes

        Returns:
            List of (row_dict, action_str) tuples where action_str is
            "inserted", "updated", or "skipped".
        """
        ws = self._get_worksheet(tab_name)
        all_rows = self._api_call_with_retry(ws.get_all_values)

        # Handle empty sheet — create header row with move_in column
        if not all_rows:
            headers = ["enabled", "community", "homesite", "floorplan",
                       "price", "address", "ready_by", "move_in", "notes"]
            self._api_call_with_retry(
                ws.append_row, headers, value_input_option="USER_ENTERED"
            )
            all_rows = [headers]

        headers = [h.strip().lower() for h in all_rows[0]]

        # If the sheet exists but has no move_in column, add it now
        move_in_col_name = "move_in"
        if move_in_col_name not in headers and "move in" not in headers and "movein" not in headers:
            # Append move_in column header right before "notes" or at the end
            notes_col_idx = None
            for idx, h in enumerate(headers):
                if h in ("notes", "note"):
                    notes_col_idx = idx
                    break

            if notes_col_idx is not None:
                # Insert before notes
                new_headers = headers[:notes_col_idx] + [move_in_col_name] + headers[notes_col_idx:]
                col_letter = chr(ord('A') + notes_col_idx)
                # Use insertDimension via gspread to insert a column
                try:
                    ws.insert_cols([["move_in"]], col=notes_col_idx + 1)
                    logger.info("Inserted 'move_in' column before 'notes' in '%s'", tab_name)
                    # Re-read all rows after column insertion
                    all_rows = self._api_call_with_retry(ws.get_all_values)
                    headers = [h.strip().lower() for h in all_rows[0]]
                except Exception as e:
                    logger.warning(
                        "Could not insert move_in column (will append instead): %s", e
                    )
                    # Append at end as fallback
                    try:
                        end_col = len(headers) + 1
                        ws.update_cell(1, end_col, "move_in")
                        all_rows = self._api_call_with_retry(ws.get_all_values)
                        headers = [h.strip().lower() for h in all_rows[0]]
                    except Exception as e2:
                        logger.warning("Could not add move_in column at all: %s", e2)
            else:
                # Append at end
                try:
                    end_col = len(headers) + 1
                    ws.update_cell(1, end_col, "move_in")
                    all_rows = self._api_call_with_retry(ws.get_all_values)
                    headers = [h.strip().lower() for h in all_rows[0]]
                except Exception as e:
                    logger.warning("Could not add move_in column: %s", e)

        col_map = _build_col_map(headers)

        if col_map["community"] < 0 or col_map["homesite"] < 0 or col_map["floorplan"] < 0:
            logger.error("CONTROL tab missing required columns (community, homesite, floorplan)")
            return [(r, "skipped") for r in rows_to_upsert]

        # Build lookup index: (community_upper, homesite_upper, floorplan_upper) -> 1-based row num
        existing_index = {}
        for i, row in enumerate(all_rows[1:], start=2):
            key = (
                _get_cell(row, col_map["community"]).upper(),
                _get_cell(row, col_map["homesite"]).upper(),
                _get_cell(row, col_map["floorplan"]).upper(),
            )
            if key not in existing_index:
                existing_index[key] = (i, row)

        # Compute all changes
        cells_to_update = []  # list of (row_1based, col_1based, value)
        rows_to_append = []   # list of [cell_values]
        results = []

        for urow in rows_to_upsert:
            community = str(urow.get("community", "")).strip()
            homesite = str(urow.get("homesite", "")).strip()
            floorplan = str(urow.get("floorplan", "")).strip()
            price = str(urow.get("price", "")).strip()
            address = str(urow.get("address", "")).strip()
            ready_by = str(urow.get("ready_by", "")).strip()
            move_in = str(urow.get("move_in", "")).strip()
            notes = str(urow.get("notes", "")).strip()

            # move_in should show the human-readable date (same as ready_by if not set)
            mi_val = move_in or ready_by

            key = (community.upper(), homesite.upper(), floorplan.upper())

            if key in existing_index:
                row_num, existing_row = existing_index[key]
                changed = False

                # Price: always update if different
                if col_map["price"] >= 0 and price:
                    old_val = _get_cell(existing_row, col_map["price"])
                    if old_val != price:
                        cells_to_update.append((row_num, col_map["price"] + 1, price))
                        changed = True

                # Address: only fill if blank
                if col_map["address"] >= 0 and address:
                    old_val = _get_cell(existing_row, col_map["address"])
                    if not old_val:
                        cells_to_update.append((row_num, col_map["address"] + 1, address))
                        changed = True

                # Ready by: always update if different
                if col_map["ready_by"] >= 0 and ready_by:
                    old_val = _get_cell(existing_row, col_map["ready_by"])
                    if old_val != ready_by:
                        cells_to_update.append((row_num, col_map["ready_by"] + 1, ready_by))
                        changed = True

                # Move in: fill if blank
                if col_map.get("move_in", -1) >= 0 and mi_val:
                    old_val = _get_cell(existing_row, col_map["move_in"])
                    if not old_val:
                        cells_to_update.append((row_num, col_map["move_in"] + 1, mi_val))
                        changed = True

                # Notes: only fill if blank
                if col_map["notes"] >= 0 and notes:
                    old_val = _get_cell(existing_row, col_map["notes"])
                    if not old_val:
                        cells_to_update.append((row_num, col_map["notes"] + 1, notes))
                        changed = True

                results.append((urow, "updated" if changed else "skipped"))
            else:
                # New row
                new_row = _build_new_row(headers, col_map, community, homesite,
                                          floorplan, price, address, ready_by, notes,
                                          move_in=mi_val)
                rows_to_append.append(new_row)
                results.append((urow, "inserted"))

                # Add to index so subsequent duplicates don't also insert
                existing_index[key] = (len(all_rows) + len(rows_to_append), new_row)

        # ── Write all changes to sheet ──

        # 1. Batch cell updates using batch_update (one API call for all updates)
        if cells_to_update:
            logger.info("Batch updating %d cells in '%s'...", len(cells_to_update), tab_name)
            # Group into chunks of up to 50 cells per API call to stay safe
            CHUNK_SIZE = 50
            for chunk_start in range(0, len(cells_to_update), CHUNK_SIZE):
                chunk = cells_to_update[chunk_start:chunk_start + CHUNK_SIZE]
                cell_list = []
                for row_num, col_num, value in chunk:
                    cell_list.append(gspread.Cell(row=row_num, col=col_num, value=value))
                self._api_call_with_retry(
                    ws.update_cells, cell_list, value_input_option="USER_ENTERED"
                )

        # 2. Append new rows (batch append: one API call per chunk)
        if rows_to_append:
            logger.info("Appending %d new rows to '%s'...", len(rows_to_append), tab_name)
            APPEND_CHUNK = 50
            for chunk_start in range(0, len(rows_to_append), APPEND_CHUNK):
                chunk = rows_to_append[chunk_start:chunk_start + APPEND_CHUNK]
                # Use append_rows (plural) if available, else one by one
                try:
                    self._api_call_with_retry(
                        ws.append_rows, chunk, value_input_option="USER_ENTERED"
                    )
                except AttributeError:
                    # Older gspread versions may not have append_rows
                    for row in chunk:
                        self._api_call_with_retry(
                            ws.append_row, row, value_input_option="USER_ENTERED"
                        )

        inserted = sum(1 for _, a in results if a == "inserted")
        updated = sum(1 for _, a in results if a == "updated")
        skipped = sum(1 for _, a in results if a == "skipped")
        logger.info(
            "Batch upsert complete: %d inserted, %d updated, %d skipped",
            inserted, updated, skipped,
        )

        return results

    # ── Verify ──

    def verify_connection(self) -> bool:
        """Verify we can access the spreadsheet. Returns True if OK."""
        try:
            self.connect()
            _ = self.spreadsheet.title
            logger.info("Sheet access verified: %s", self.spreadsheet.title)
            return True
        except Exception as e:
            logger.error("Sheet access failed: %s", e)
            return False


# ── Module-level helpers ──

def _build_col_map(headers: list) -> dict:
    """Build column index map from header list. Returns -1 for missing columns."""
    def _find(name):
        name_l = name.strip().lower()
        for variant in [name_l, name_l.replace("_", " "), name_l.replace(" ", "_")]:
            if variant in headers:
                return headers.index(variant)
        return -1

    rb_col = _find("ready_by")
    if rb_col < 0:
        rb_col = _find("ready by")

    mi_col = _find("move_in")
    if mi_col < 0:
        mi_col = _find("move in")
    if mi_col < 0:
        mi_col = _find("move in date")
    if mi_col < 0:
        mi_col = _find("movein")

    return {
        "community": _find("community"),
        "homesite": _find("homesite"),
        "floorplan": _find("floorplan"),
        "price": _find("price"),
        "address": _find("address"),
        "ready_by": rb_col,
        "move_in": mi_col,
        "notes": _find("notes"),
        "enabled": _find("enabled"),
    }


def _get_cell(row: list, col_idx: int) -> str:
    """Safely get a cell value as a stripped string."""
    if col_idx < 0 or col_idx >= len(row):
        return ""
    return str(row[col_idx]).strip()


def _build_new_row(headers, col_map, community, homesite,
                    floorplan, price, address, ready_by, notes,
                    move_in: str = "") -> list:
    """Build a new row list matching the header layout."""
    new_row = [""] * len(headers)
    if col_map["enabled"] >= 0:
        new_row[col_map["enabled"]] = "TRUE"
    if col_map["community"] >= 0:
        new_row[col_map["community"]] = community
    if col_map["homesite"] >= 0:
        new_row[col_map["homesite"]] = str(homesite)
    if col_map["floorplan"] >= 0:
        new_row[col_map["floorplan"]] = str(floorplan)
    if col_map["price"] >= 0:
        new_row[col_map["price"]] = str(price)
    if col_map["address"] >= 0:
        new_row[col_map["address"]] = address
    if col_map["ready_by"] >= 0:
        new_row[col_map["ready_by"]] = ready_by
    if col_map.get("move_in", -1) >= 0:
        new_row[col_map["move_in"]] = move_in or ready_by  # mirror date if no dedicated value
    if col_map["notes"] >= 0:
        new_row[col_map["notes"]] = notes
    return new_row
