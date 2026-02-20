"""Agent coordination via shared Google Sheet (agent_status tab).

Two agents coordinate through a shared spreadsheet:
  - pricing_agent (this bot) — row 3
  - map_agent (external) — row 2

Status values: IDLE | WORKING | DONE

Signal flow:
  1. Pricing agent sets WORKING before writing to CONTROL sheet
  2. Pricing agent sets DONE after finishing CONTROL updates
  3. Map agent sees DONE → colors maps in templates → sets map_agent = DONE
  4. Pricing agent sees map_agent = DONE → exports PDFs
  5. Pricing agent resets map_agent → IDLE
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger("price_sheet_bot.agent_sync")

# Spreadsheet and tab configuration
AGENT_STATUS_SPREADSHEET_ID = "13c4WnzWgt5XTjqoRwesp0yB1i6Ww77v3Vj_7xZSTu_I"
AGENT_STATUS_TAB = "agent_status"

# Row positions (1-based)
MAP_AGENT_ROW = 2
PRICING_AGENT_ROW = 3

# Column positions (1-based)
COL_AGENT = 1     # A: Agent name
COL_STATUS = 2    # B: Status (IDLE/WORKING/DONE)
COL_TIMESTAMP = 3 # C: Timestamp
COL_NOTE = 4      # D: Note

# Status values
IDLE = "IDLE"
WORKING = "WORKING"
DONE = "DONE"

# Polling settings
DEFAULT_POLL_INTERVAL = 15   # seconds between checks
DEFAULT_TIMEOUT = 600        # 10 minutes max wait

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class AgentSync:
    """Manages coordination with the map agent via shared status sheet."""

    def __init__(self, credentials_path: str):
        self.credentials_path = credentials_path
        self._client: Optional[gspread.Client] = None
        self._sheet: Optional[gspread.Spreadsheet] = None
        self._ws: Optional[gspread.Worksheet] = None

    def connect(self):
        """Connect to the agent_status spreadsheet."""
        creds = Credentials.from_service_account_file(
            self.credentials_path, scopes=SCOPES
        )
        self._client = gspread.authorize(creds)
        self._sheet = self._client.open_by_key(AGENT_STATUS_SPREADSHEET_ID)
        self._ws = self._sheet.worksheet(AGENT_STATUS_TAB)
        logger.info("Connected to agent_status sheet.")

    @property
    def ws(self) -> gspread.Worksheet:
        if self._ws is None:
            raise RuntimeError("AgentSync not connected. Call connect() first.")
        return self._ws

    # ── Read status ──

    def get_map_agent_status(self) -> str:
        """Read map_agent's current status (row 2, col B)."""
        val = self.ws.cell(MAP_AGENT_ROW, COL_STATUS).value
        return (val or "").strip().upper()

    def get_pricing_agent_status(self) -> str:
        """Read pricing_agent's current status (row 3, col B)."""
        val = self.ws.cell(PRICING_AGENT_ROW, COL_STATUS).value
        return (val or "").strip().upper()

    # ── Write status ──

    def _set_status(self, row: int, status: str, note: str = ""):
        """Update status, timestamp, and note for a row."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        self.ws.update_cell(row, COL_STATUS, status)
        self.ws.update_cell(row, COL_TIMESTAMP, now)
        if note:
            self.ws.update_cell(row, COL_NOTE, note)
        logger.info("Set row %d status = %s (note: %s)", row, status, note or "(none)")

    def set_pricing_working(self, note: str = "Updating CONTROL sheet"):
        """Signal: pricing agent is WORKING (map agent should pause)."""
        self._set_status(PRICING_AGENT_ROW, WORKING, note)
        print(f"  [SYNC] Set pricing_agent = WORKING")

    def set_pricing_done(self, note: str = "CONTROL updates complete"):
        """Signal: pricing agent finished CONTROL updates (triggers map agent)."""
        self._set_status(PRICING_AGENT_ROW, DONE, note)
        print(f"  [SYNC] Set pricing_agent = DONE (map agent will start)")

    def set_pricing_idle(self, note: str = ""):
        """Reset pricing agent to IDLE."""
        self._set_status(PRICING_AGENT_ROW, IDLE, note)
        print(f"  [SYNC] Set pricing_agent = IDLE")

    def reset_map_agent(self, note: str = "Reset by pricing agent after PDF export"):
        """Reset map_agent status to IDLE after we're done with PDF export."""
        self._set_status(MAP_AGENT_ROW, IDLE, note)
        print(f"  [SYNC] Reset map_agent = IDLE")

    # ── Wait for map agent ──

    def wait_for_map_agent(
        self,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> bool:
        """Poll until map_agent status = DONE, then return True.

        Returns False if timeout is reached.
        """
        print(f"\n  [SYNC] Waiting for map agent to finish (polling every {poll_interval}s, "
              f"timeout {timeout}s)...")
        start = time.time()
        last_status = ""

        while True:
            elapsed = time.time() - start
            if elapsed >= timeout:
                print(f"  [SYNC] TIMEOUT after {timeout}s. Map agent did not finish.")
                logger.warning("Timed out waiting for map_agent = DONE after %ds", timeout)
                return False

            status = self.get_map_agent_status()

            if status != last_status:
                print(f"  [SYNC] map_agent status = {status} ({int(elapsed)}s elapsed)")
                last_status = status

            if status == DONE:
                print(f"  [SYNC] Map agent is DONE! Proceeding with PDF export.")
                return True

            time.sleep(poll_interval)

    # ── Safety check ──

    def check_map_agent_not_working(self) -> bool:
        """Check that map_agent is not currently WORKING.

        Per the protocol, we should NOT write to CONTROL while map_agent = WORKING.
        """
        status = self.get_map_agent_status()
        if status == WORKING:
            logger.warning("map_agent is currently WORKING. Should not write to CONTROL.")
            return False
        return True
