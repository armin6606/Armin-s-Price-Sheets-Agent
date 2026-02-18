"""PDF export - converts DOCX to PDF via Google Drive or LibreOffice."""

import logging
import os
import subprocess
import tempfile
from typing import Optional

logger = logging.getLogger("price_sheet_bot.pdf_export")


def export_pdf_via_drive(drive_client, docx_bytes: bytes, temp_folder_id: str,
                          temp_name: str = "_temp_convert") -> bytes:
    """Export DOCX to PDF using Google Drive conversion.

    1. Upload DOCX as Google Doc (triggers conversion)
    2. Export Google Doc as PDF
    3. Delete temp Google Doc

    Returns PDF bytes.
    """
    logger.info("Exporting PDF via Google Drive conversion...")

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


def export_pdf_via_libreoffice(docx_bytes: bytes) -> Optional[bytes]:
    """Export DOCX to PDF using LibreOffice (fallback).

    Requires LibreOffice installed and 'soffice' in PATH.
    Returns PDF bytes or None if LibreOffice not available.
    """
    # Check if LibreOffice is available
    try:
        subprocess.run(["soffice", "--version"], capture_output=True, check=True, timeout=10)
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        logger.debug("LibreOffice not available for PDF conversion.")
        return None

    logger.info("Exporting PDF via LibreOffice...")

    with tempfile.TemporaryDirectory() as tmpdir:
        docx_path = os.path.join(tmpdir, "input.docx")
        with open(docx_path, "wb") as f:
            f.write(docx_bytes)

        try:
            subprocess.run(
                ["soffice", "--headless", "--convert-to", "pdf", "--outdir", tmpdir, docx_path],
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


def export_to_pdf(drive_client, docx_bytes: bytes, temp_folder_id: str,
                   temp_name: str = "_temp_convert",
                   prefer_drive: bool = True) -> bytes:
    """Export DOCX to PDF using best available method.

    Tries Drive conversion first (preferred), falls back to LibreOffice.
    """
    if prefer_drive:
        try:
            return export_pdf_via_drive(drive_client, docx_bytes, temp_folder_id, temp_name)
        except Exception as e:
            logger.warning("Drive PDF export failed, trying LibreOffice fallback: %s", e)

    # Try LibreOffice fallback
    result = export_pdf_via_libreoffice(docx_bytes)
    if result:
        return result

    raise RuntimeError(
        "PDF export failed: neither Google Drive conversion nor LibreOffice worked. "
        "Ensure the service account has Drive access, or install LibreOffice."
    )
