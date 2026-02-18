"""Google Drive client for file operations.

Uses OAuth2 (your personal Google login) for uploads/writes,
since service accounts have no storage quota on personal Drive.
Falls back to service account for read-only operations.
"""

import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

from google.oauth2.service_account import Credentials as SACredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload

logger = logging.getLogger("price_sheet_bot.drive")

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2

# Path to store OAuth2 user token (so you only sign in once)
OAUTH_TOKEN_PATH = "./secrets/user_token.json"
OAUTH_CREDENTIALS_PATH = "./secrets/oauth_credentials.json"


def _retry(func, *args, **kwargs):
    """Execute with exponential backoff for transient errors."""
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_str = str(e).lower()
            transient = any(kw in err_str for kw in [
                "rate limit", "500", "503", "backend", "timeout", "timed out"
            ])
            if transient and attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning("Transient error (attempt %d/%d), retrying in %ds: %s",
                               attempt + 1, MAX_RETRIES, delay, e)
                time.sleep(delay)
            else:
                raise


def _build_oauth_service():
    """Build a Drive API service using OAuth2 user credentials.

    First time: opens browser for Google login.
    After that: uses saved token.
    """
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    creds = None

    # Load existing token
    if os.path.exists(OAUTH_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(OAUTH_TOKEN_PATH, SCOPES)

    # If no valid token, do the login flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None

        if not creds:
            if not os.path.exists(OAUTH_CREDENTIALS_PATH):
                raise FileNotFoundError(
                    f"\n{'='*60}\n"
                    f"OAuth credentials file not found: {OAUTH_CREDENTIALS_PATH}\n\n"
                    f"To fix this:\n"
                    f"1. Go to https://console.cloud.google.com/apis/credentials\n"
                    f"2. Click '+ CREATE CREDENTIALS' -> 'OAuth client ID'\n"
                    f"3. Application type: 'Desktop app'\n"
                    f"4. Click 'Download JSON'\n"
                    f"5. Save the file as: {os.path.abspath(OAUTH_CREDENTIALS_PATH)}\n"
                    f"{'='*60}"
                )
            flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save token for next time
        os.makedirs(os.path.dirname(OAUTH_TOKEN_PATH), exist_ok=True)
        with open(OAUTH_TOKEN_PATH, "w") as f:
            f.write(creds.to_json())
        logger.info("OAuth2 user token saved.")

    service = build("drive", "v3", credentials=creds)
    logger.info("Connected to Google Drive API via OAuth2 (user account).")
    return service


class DriveClient:
    """Manages all Google Drive file operations.

    Uses service account for reads, OAuth2 user account for writes.
    """

    def __init__(self, credentials_path: str, shared_drive_id: Optional[str] = None):
        self.credentials_path = credentials_path
        self.shared_drive_id = shared_drive_id
        self._sa_service = None   # Service account (reads)
        self._user_service = None  # OAuth2 user (writes)

    def connect(self):
        """Authenticate with service account for reads."""
        creds = SACredentials.from_service_account_file(
            self.credentials_path, scopes=SCOPES
        )
        self._sa_service = build("drive", "v3", credentials=creds)
        logger.info("Connected to Google Drive API (service account for reads).")

    def connect_for_writes(self):
        """Authenticate with OAuth2 for uploads/writes. Opens browser first time."""
        self._user_service = _build_oauth_service()

    @property
    def service(self):
        """Service for read operations (service account)."""
        if self._sa_service is None:
            raise RuntimeError("DriveClient not connected. Call connect() first.")
        return self._sa_service

    @property
    def write_service(self):
        """Service for write operations (OAuth2 user account).
        Falls back to service account if OAuth2 not set up."""
        if self._user_service is not None:
            return self._user_service
        # Fallback to service account (works for Shared Drives)
        return self.service

    def _drive_params(self, extra: dict = None) -> dict:
        """Build common params for Shared Drive support."""
        params = {"supportsAllDrives": True, "includeItemsFromAllDrives": True}
        if self.shared_drive_id:
            params["corpora"] = "drive"
            params["driveId"] = self.shared_drive_id
        if extra:
            params.update(extra)
        return params

    # ── Listing ──

    def list_files(self, folder_id: str, mime_filter: str = None) -> list:
        """List files in a Drive folder."""
        query = f"'{folder_id}' in parents and trashed = false"
        if mime_filter:
            query += f" and mimeType = '{mime_filter}'"

        results = []
        page_token = None
        while True:
            params = self._drive_params({
                "q": query,
                "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, appProperties, size)",
                "pageSize": 100,
            })
            if page_token:
                params["pageToken"] = page_token
            resp = _retry(self.service.files().list(**params).execute)
            results.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        logger.debug("Listed %d files in folder %s", len(results), folder_id)
        return results

    def list_pdfs(self, folder_id: str) -> list:
        """List PDF files in a folder."""
        return self.list_files(folder_id, mime_filter="application/pdf")

    def find_file_by_name(self, folder_id: str, file_name: str) -> Optional[dict]:
        """Find a specific file by exact name in a folder."""
        escaped = file_name.replace("'", "\\'")
        query = f"'{folder_id}' in parents and name = '{escaped}' and trashed = false"
        params = self._drive_params({
            "q": query,
            "fields": "files(id, name, mimeType, modifiedTime, appProperties, size)",
            "pageSize": 5,
        })
        resp = _retry(self.service.files().list(**params).execute)
        files = resp.get("files", [])
        if files:
            return files[0]
        return None

    # ── Download ──

    def download_file(self, file_id: str, dest_path: str) -> str:
        """Download a file from Drive to local path."""
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        request = self.service.files().get_media(fileId=file_id, supportsAllDrives=True)
        with open(dest_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = _retry(downloader.next_chunk)
        logger.info("Downloaded %s -> %s", file_id, dest_path)
        return dest_path

    def download_to_bytes(self, file_id: str) -> bytes:
        """Download a file from Drive into memory."""
        request = self.service.files().get_media(fileId=file_id, supportsAllDrives=True)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = _retry(downloader.next_chunk)
        return buffer.getvalue()

    # ── Upload (uses OAuth2 write_service) ──

    def upload_file(self, local_path: str, folder_id: str, file_name: str,
                    mime_type: str = None) -> dict:
        """Upload a local file to Drive. Returns file metadata."""
        if not mime_type:
            if file_name.endswith(".pdf"):
                mime_type = "application/pdf"
            elif file_name.endswith(".docx"):
                mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            else:
                mime_type = "application/octet-stream"

        metadata = {"name": file_name, "parents": [folder_id]}
        media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
        result = _retry(
            self.write_service.files().create(
                body=metadata, media_body=media,
                fields="id, name, size, modifiedTime",
                supportsAllDrives=True,
            ).execute
        )
        logger.info("Uploaded %s as %s (id=%s)", local_path, file_name, result["id"])
        return result

    def upload_bytes(self, data: bytes, folder_id: str, file_name: str,
                     mime_type: str = None) -> dict:
        """Upload bytes to Drive. Returns file metadata."""
        if not mime_type:
            if file_name.endswith(".pdf"):
                mime_type = "application/pdf"
            elif file_name.endswith(".docx"):
                mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            else:
                mime_type = "application/octet-stream"

        metadata = {"name": file_name, "parents": [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, resumable=True)
        result = _retry(
            self.write_service.files().create(
                body=metadata, media_body=media,
                fields="id, name, size, modifiedTime",
                supportsAllDrives=True,
            ).execute
        )
        logger.info("Uploaded bytes as %s (id=%s, size=%s)", file_name, result["id"], result.get("size"))
        return result

    # ── Rename / Move / Delete (uses write_service) ──

    def rename_file(self, file_id: str, new_name: str) -> dict:
        """Rename a file on Drive."""
        result = _retry(
            self.write_service.files().update(
                fileId=file_id, body={"name": new_name},
                fields="id, name", supportsAllDrives=True,
            ).execute
        )
        logger.debug("Renamed %s -> %s", file_id, new_name)
        return result

    def move_file(self, file_id: str, new_parent_id: str, old_parent_id: str = None) -> dict:
        """Move a file to a different folder."""
        params = {"fileId": file_id, "addParents": new_parent_id, "supportsAllDrives": True,
                  "fields": "id, name, parents"}
        if old_parent_id:
            params["removeParents"] = old_parent_id
        result = _retry(self.write_service.files().update(**params).execute)
        logger.debug("Moved %s to folder %s", file_id, new_parent_id)
        return result

    def delete_file(self, file_id: str):
        """Permanently delete a file (use with caution!)."""
        _retry(self.write_service.files().delete(fileId=file_id, supportsAllDrives=True).execute)
        logger.warning("Deleted file %s", file_id)

    def trash_file(self, file_id: str) -> dict:
        """Move a file to trash."""
        result = _retry(
            self.write_service.files().update(
                fileId=file_id, body={"trashed": True},
                supportsAllDrives=True, fields="id, name, trashed",
            ).execute
        )
        logger.info("Trashed file %s", file_id)
        return result

    # ── App Properties (idempotency tagging) ──

    def set_app_properties(self, file_id: str, properties: dict):
        """Set appProperties on a Drive file."""
        _retry(
            self.write_service.files().update(
                fileId=file_id,
                body={"appProperties": properties},
                supportsAllDrives=True,
                fields="id, appProperties",
            ).execute
        )
        logger.debug("Set appProperties on %s: %s", file_id, properties)

    def get_app_properties(self, file_id: str) -> dict:
        """Get appProperties from a Drive file."""
        result = _retry(
            self.service.files().get(
                fileId=file_id, fields="appProperties",
                supportsAllDrives=True,
            ).execute
        )
        return result.get("appProperties", {})

    # ── Folder operations ──

    def ensure_subfolder(self, parent_id: str, folder_name: str) -> str:
        """Find or create a subfolder. Returns folder ID."""
        existing = self.find_file_by_name(parent_id, folder_name)
        if existing:
            return existing["id"]

        metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        result = _retry(
            self.write_service.files().create(
                body=metadata, fields="id, name",
                supportsAllDrives=True,
            ).execute
        )
        logger.info("Created subfolder '%s' (id=%s) in %s", folder_name, result["id"], parent_id)
        return result["id"]

    # ── Safe replace (atomic swap) ──

    def safe_replace(self, data: bytes, folder_id: str, final_name: str,
                     mime_type: str = None, allow_deletions: bool = False,
                     archive_folder_name: str = "Archive") -> dict:
        """Upload with atomic safe-replace logic.

        1. Upload as temp name
        2. Verify upload
        3. Archive or delete old file
        4. Rename temp to final
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        temp_name = f"{final_name}.tmp.{timestamp}"

        # Step 1: Upload as temp
        new_file = self.upload_bytes(data, folder_id, temp_name, mime_type)
        new_size = int(new_file.get("size", 0))
        if new_size == 0 and len(data) > 0:
            logger.error("Safe replace: uploaded file is empty! Aborting.")
            self.delete_file(new_file["id"])
            raise RuntimeError(f"Safe replace failed: uploaded temp file is empty for {final_name}")

        # Step 2: Find existing file with final name
        existing = self.find_file_by_name(folder_id, final_name)

        # Step 3: Archive or delete old
        if existing:
            if allow_deletions:
                self.delete_file(existing["id"])
                logger.info("Deleted old file: %s (%s)", final_name, existing["id"])
            else:
                archive_id = self.ensure_subfolder(folder_id, archive_folder_name)
                archive_name = f"{final_name}.{timestamp}"
                self.rename_file(existing["id"], archive_name)
                self.move_file(existing["id"], archive_id, folder_id)
                logger.info("Archived old file: %s -> %s/%s", final_name, archive_folder_name, archive_name)

        # Step 4: Rename temp to final
        self.rename_file(new_file["id"], final_name)
        logger.info("Safe replace complete: %s (id=%s)", final_name, new_file["id"])
        return {"id": new_file["id"], "name": final_name, "size": new_size}

    # ── PDF export via Drive ──

    def export_as_pdf(self, docx_file_id: str) -> bytes:
        """Export a Google Docs-compatible file as PDF via Drive export."""
        request = self.write_service.files().export_media(
            fileId=docx_file_id, mimeType="application/pdf"
        )
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = _retry(downloader.next_chunk)
        return buffer.getvalue()

    def upload_docx_as_google_doc(self, data: bytes, folder_id: str, name: str) -> dict:
        """Upload DOCX bytes as a Google Doc (for PDF conversion)."""
        metadata = {
            "name": name,
            "parents": [folder_id],
            "mimeType": "application/vnd.google-apps.document",
        }
        media = MediaIoBaseUpload(
            io.BytesIO(data),
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            resumable=True,
        )
        result = _retry(
            self.write_service.files().create(
                body=metadata, media_body=media,
                fields="id, name", supportsAllDrives=True,
            ).execute
        )
        logger.debug("Uploaded DOCX as Google Doc: %s (id=%s)", name, result["id"])
        return result

    # ── Verify access ──

    def verify_folder_access(self, folder_id: str, label: str = "") -> bool:
        """Verify we can list files in a folder."""
        try:
            self.list_files(folder_id)
            logger.info("Folder access OK: %s (%s)", label, folder_id)
            return True
        except Exception as e:
            logger.error("Folder access FAILED: %s (%s): %s", label, folder_id, e)
            return False

    def verify_upload_ability(self, folder_id: str) -> bool:
        """Test that we can upload to a folder by creating and deleting a test file."""
        try:
            test_data = b"price_sheet_bot_health_check"
            result = self.upload_bytes(test_data, folder_id, ".health_check_test.tmp")
            self.delete_file(result["id"])
            logger.info("Upload ability verified for folder %s", folder_id)
            return True
        except Exception as e:
            logger.error("Upload ability FAILED for folder %s: %s", folder_id, e)
            return False
