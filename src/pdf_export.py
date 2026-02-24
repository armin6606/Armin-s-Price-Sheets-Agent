"""PDF export - converts DOCX to PDF.

Priority order:
  1. Microsoft Word COM (Windows only) - pixel-perfect rendering
  2. LibreOffice (cross-platform) - good but minor style differences
  3. Google Drive (fallback) - corrupts design elements

Google Drive's DOCX->Google Docs conversion corrupts:
  - Page borders/frames (dropped - Google Docs has no support)
  - Floating/anchored images (duplicated across pages)
  - Precisely positioned elements (shapes, text boxes get displaced)

LibreOffice is much better but renders Word table styles (PlainTable1,
conditional formatting with cnfStyle) slightly differently.

Microsoft Word via COM automation gives pixel-perfect output because it's
the same engine that created the DOCX files.
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


def _find_word() -> Optional[str]:
    """Find Microsoft Word executable on Windows.

    Returns the path to WINWORD.EXE, or None if not found.
    Only works on Windows (Word COM requires Windows).
    """
    if platform.system() != "Windows":
        return None

    common_paths = [
        r"C:\Program Files\Microsoft Office\Root\Office16\WINWORD.EXE",
        r"C:\Program Files (x86)\Microsoft Office\Root\Office16\WINWORD.EXE",
        r"C:\Program Files\Microsoft Office\Root\Office15\WINWORD.EXE",
        os.path.expandvars(r"%PROGRAMFILES%\Microsoft Office\Root\Office16\WINWORD.EXE"),
    ]
    for path in common_paths:
        if os.path.exists(path):
            return path

    return None


def export_pdf_via_word(docx_bytes: bytes) -> Optional[bytes]:
    """Export DOCX to PDF using Microsoft Word COM automation.

    BEST method - pixel-perfect rendering because Word is the engine that
    created the DOCX files. Only available on Windows with Word installed.

    Uses win32com.client to open the DOCX in Word and SaveAs PDF.
    Returns PDF bytes or None if Word/COM not available.
    """
    if platform.system() != "Windows":
        return None

    word_path = _find_word()
    if not word_path:
        logger.debug("Microsoft Word not found for PDF conversion.")
        return None

    try:
        import comtypes.client
    except ImportError:
        try:
            import win32com.client  # noqa: F401
        except ImportError:
            logger.debug("Neither comtypes nor win32com available for Word COM automation.")
            return None

    logger.info("Exporting PDF via Microsoft Word COM...")

    with tempfile.TemporaryDirectory() as tmpdir:
        docx_path = os.path.join(tmpdir, "input.docx")
        pdf_path = os.path.join(tmpdir, "input.pdf")

        with open(docx_path, "wb") as f:
            f.write(docx_bytes)

        # Use absolute paths (Word COM requires them)
        docx_abs = os.path.abspath(docx_path)
        pdf_abs = os.path.abspath(pdf_path)

        word = None
        doc = None
        try:
            # Try comtypes first (more reliable, no pywin32 dependency)
            try:
                import comtypes.client
                word = comtypes.client.CreateObject("Word.Application")
            except Exception:
                import win32com.client
                word = win32com.client.Dispatch("Word.Application")

            word.Visible = False
            word.DisplayAlerts = False

            # Open the document
            doc = word.Documents.Open(docx_abs, ReadOnly=True)

            # ExportAsFixedFormat: Type=0 is PDF
            # wdExportFormatPDF = 17
            doc.SaveAs2(pdf_abs, FileFormat=17)

            doc.Close(SaveChanges=False)
            doc = None

        except Exception as e:
            logger.error("Word COM conversion failed: %s", e)
            if doc:
                try:
                    doc.Close(SaveChanges=False)
                except Exception:
                    pass
            return None
        finally:
            if word:
                try:
                    word.Quit()
                except Exception:
                    pass

        if not os.path.exists(pdf_path):
            logger.error("Word COM did not produce PDF output.")
            return None

        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()

        logger.info("PDF export via Microsoft Word successful (%d bytes).", len(pdf_bytes))
        return pdf_bytes


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


def _count_pdf_pages(pdf_bytes: bytes) -> int:
    """Count the number of pages in a PDF."""
    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            return len(pdf.pages)
    except Exception:
        return -1


def export_to_pdf(drive_client, docx_bytes: bytes, temp_folder_id: str,
                   temp_name: str = "_temp_convert",
                   prefer_drive: bool = False,
                   expected_pages: int = 0) -> bytes:
    """Export DOCX to PDF using best available method.

    Priority:
      1. Microsoft Word COM (pixel-perfect, Windows only)
      2. LibreOffice (good cross-platform, minor style differences)
      3. Google Drive (fallback, corrupts some formatting)

    If expected_pages > 0, validates the generated PDF has the correct
    page count. This catches LibreOffice rendering differences that can
    cause tables to overflow to extra pages.
    """
    # Try Microsoft Word COM first (pixel-perfect rendering)
    result = export_pdf_via_word(docx_bytes)
    if result:
        if expected_pages > 0:
            actual = _count_pdf_pages(result)
            if actual != expected_pages:
                logger.warning(
                    "Word COM PDF has %d pages (expected %d). Continuing anyway.",
                    actual, expected_pages,
                )
        return result

    # Try LibreOffice next (preserves design faithfully, minor table style diffs)
    result = export_pdf_via_libreoffice(docx_bytes)
    if result:
        if expected_pages > 0:
            actual = _count_pdf_pages(result)
            if actual != expected_pages:
                logger.warning(
                    "LibreOffice PDF has %d pages (expected %d). "
                    "This may indicate font metric differences causing table overflow.",
                    actual, expected_pages,
                )
        return result

    logger.info("Neither Word nor LibreOffice available, falling back to Google Drive.")

    # Fallback to Google Drive conversion
    try:
        return export_pdf_via_drive(drive_client, docx_bytes, temp_folder_id, temp_name)
    except Exception as e:
        logger.error("Google Drive PDF export also failed: %s", e)

    raise RuntimeError(
        "PDF export failed: no conversion method available. "
        "Install Microsoft Word (Windows) or LibreOffice for best results."
    )
