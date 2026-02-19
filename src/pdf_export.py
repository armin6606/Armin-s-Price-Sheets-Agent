"""PDF export - converts DOCX to PDF via LibreOffice or Google Drive.

LibreOffice is preferred because Google Drive's DOCX→Google Docs conversion
corrupts design elements:
  - Page borders/frames are dropped (Google Docs has no page border support)
  - Floating/anchored images can be duplicated across pages
  - Precisely positioned elements (shapes, text boxes) get displaced

LibreOffice faithfully preserves all Word formatting including borders,
images, and positioned elements.
"""

import logging
import os
import platform
import subprocess
import tempfile
from typing import Optional

logger = logging.getLogger("price_sheet_bot.pdf_export")


def _find_libreoffice() -> Optional[str]:
    """Find the LibreOffice executable on the current platform.

    Returns the path to soffice, or None if not found.
    """
    # Try 'soffice' in PATH first (Linux, macOS, or manually added)
    try:
        subprocess.run(["soffice", "--version"], capture_output=True, check=True, timeout=10)
        return "soffice"
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        pass

    # Windows: check common install locations
    if platform.system() == "Windows":
        common_paths = [
            r"C:\Program Files\LibreOffice\program\soffice.exe",
            r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
            os.path.expandvars(r"%PROGRAMFILES%\LibreOffice\program\soffice.exe"),
        ]
        for path in common_paths:
            if os.path.exists(path):
                return path

    # macOS: check Applications
    if platform.system() == "Darwin":
        mac_path = "/Applications/LibreOffice.app/Contents/MacOS/soffice"
        if os.path.exists(mac_path):
            return mac_path

    return None


def export_pdf_via_libreoffice(docx_bytes: bytes) -> Optional[bytes]:
    """Export DOCX to PDF using LibreOffice.

    Preferred method — preserves all Word formatting including page borders,
    floating images, positioned shapes, headers/footers, etc.

    Returns PDF bytes or None if LibreOffice not available.
    """
    soffice = _find_libreoffice()
    if not soffice:
        logger.debug("LibreOffice not available for PDF conversion.")
        return None

    logger.info("Exporting PDF via LibreOffice...")

    with tempfile.TemporaryDirectory() as tmpdir:
        docx_path = os.path.join(tmpdir, "input.docx")
        with open(docx_path, "wb") as f:
            f.write(docx_bytes)

        try:
            subprocess.run(
                [soffice, "--headless", "--convert-to", "pdf", "--outdir", tmpdir, docx_path],
                capture_output=True, check=True, timeout=120,
            )
        except subprocess.CalledProcessError as e:
            logger.error("LibreOffice conversion failed: %s", e.stderr)
            return None
        except subprocess.TimeoutExpired:
            logger.error("LibreOffice conversion timed out.")
            return None

        pdf_path = os.path.join(tmpdir, "input.pdf")
        if not os.path.exists(pdf_path):
            logger.error("LibreOffice did not produce PDF output.")
            return None

        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()

        logger.info("PDF export via LibreOffice successful (%d bytes).", len(pdf_bytes))
        return pdf_bytes


def export_pdf_via_drive(drive_client, docx_bytes: bytes, temp_folder_id: str,
                          temp_name: str = "_temp_convert") -> bytes:
    """Export DOCX to PDF using Google Drive conversion (fallback).

    WARNING: Google Drive conversion corrupts some formatting:
    - Page borders/frames are dropped
    - Floating images can be duplicated
    - Positioned elements get displaced

    Use only as a fallback when LibreOffice is not available.
    """
    logger.info("Exporting PDF via Google Drive conversion (fallback)...")

    # Upload DOCX as Google Doc
    temp_doc = drive_client.upload_docx_as_google_doc(
        docx_bytes, temp_folder_id, temp_name
    )
    temp_id = temp_doc["id"]

    try:
        # Export as PDF
        pdf_bytes = drive_client.export_as_pdf(temp_id)
        if not pdf_bytes:
            raise RuntimeError("Drive PDF export returned empty bytes.")
        logger.info("PDF export via Drive successful (%d bytes).", len(pdf_bytes))
        return pdf_bytes
    finally:
        # Always clean up temp Google Doc
        try:
            drive_client.delete_file(temp_id)
        except Exception as e:
            logger.warning("Failed to delete temp Google Doc %s: %s", temp_id, e)


def export_to_pdf(drive_client, docx_bytes: bytes, temp_folder_id: str,
                   temp_name: str = "_temp_convert",
                   prefer_drive: bool = False) -> bytes:
    """Export DOCX to PDF using best available method.

    Tries LibreOffice first (preserves all formatting faithfully),
    falls back to Google Drive conversion if LibreOffice unavailable.
    """
    # Try LibreOffice first (preferred — preserves design faithfully)
    result = export_pdf_via_libreoffice(docx_bytes)
    if result:
        return result

    logger.info("LibreOffice not available, falling back to Google Drive conversion.")

    # Fallback to Google Drive conversion
    if prefer_drive or True:  # Always try Drive as fallback
        try:
            return export_pdf_via_drive(drive_client, docx_bytes, temp_folder_id, temp_name)
        except Exception as e:
            logger.error("Google Drive PDF export also failed: %s", e)

    raise RuntimeError(
        "PDF export failed: neither LibreOffice nor Google Drive conversion worked. "
        "Install LibreOffice for best results: sudo apt install libreoffice-writer"
    )
