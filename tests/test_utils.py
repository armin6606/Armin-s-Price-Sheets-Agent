"""Unit tests for utility functions."""

import json
import os
import sys
import unittest
from datetime import date, datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils import (
    normalize_header,
    resolve_header,
    build_header_map,
    validate_headers,
    format_price,
    parse_ready_by,
    parse_pdf_filename,
    compute_hash,
    normalize_for_compare,
)


class TestNormalizeHeader(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(normalize_header("Site"), "SITE")

    def test_dash_to_space(self):
        self.assertEqual(normalize_header("Ready-By"), "READY BY")

    def test_underscore_to_space(self):
        self.assertEqual(normalize_header("ready_by"), "READY BY")

    def test_trim(self):
        self.assertEqual(normalize_header("  Price  "), "PRICE")

    def test_punctuation_removed(self):
        self.assertEqual(normalize_header("Ready-By!"), "READY BY")

    def test_collapse_whitespace(self):
        self.assertEqual(normalize_header("Home   Site"), "HOME SITE")

    def test_empty(self):
        self.assertEqual(normalize_header(""), "")
        self.assertEqual(normalize_header(None), "")


class TestResolveHeader(unittest.TestCase):
    def test_site_aliases(self):
        self.assertEqual(resolve_header("SITE"), "SITE")
        self.assertEqual(resolve_header("HOMESITE"), "SITE")
        self.assertEqual(resolve_header("HOME SITE"), "SITE")
        self.assertEqual(resolve_header("HS"), "SITE")

    def test_price_aliases(self):
        self.assertEqual(resolve_header("PRICE"), "PRICE")
        self.assertEqual(resolve_header("SALES PRICE"), "PRICE")
        self.assertEqual(resolve_header("FINAL PRICE"), "PRICE")

    def test_address_aliases(self):
        self.assertEqual(resolve_header("ADDRESS"), "ADDRESS")
        self.assertEqual(resolve_header("PROPERTY ADDRESS"), "ADDRESS")

    def test_ready_by_aliases(self):
        self.assertEqual(resolve_header("READY BY"), "READY BY")
        self.assertEqual(resolve_header("READYBY"), "READY BY")
        self.assertEqual(resolve_header("READY BY DATE"), "READY BY")

    def test_notes_aliases(self):
        self.assertEqual(resolve_header("NOTES"), "NOTES")
        self.assertEqual(resolve_header("NOTE"), "NOTES")

    def test_unknown(self):
        self.assertIsNone(resolve_header("SOMETHING ELSE"))


class TestBuildHeaderMap(unittest.TestCase):
    def test_standard_headers(self):
        cells = ["Site", "Price", "Address", "Ready-By", "Notes"]
        hmap = build_header_map(cells)
        self.assertEqual(hmap["SITE"], 0)
        self.assertEqual(hmap["PRICE"], 1)
        self.assertEqual(hmap["ADDRESS"], 2)
        self.assertEqual(hmap["READY BY"], 3)
        self.assertEqual(hmap["NOTES"], 4)

    def test_alias_headers(self):
        cells = ["HS", "Sales Price", "Property Address", "Ready By Date", "Note"]
        hmap = build_header_map(cells)
        self.assertEqual(hmap["SITE"], 0)
        self.assertEqual(hmap["PRICE"], 1)
        self.assertEqual(hmap["ADDRESS"], 2)
        self.assertEqual(hmap["READY BY"], 3)
        self.assertEqual(hmap["NOTES"], 4)

    def test_first_match_wins(self):
        cells = ["Site", "Price", "Homesite"]
        hmap = build_header_map(cells)
        self.assertEqual(hmap["SITE"], 0)  # first match


class TestValidateHeaders(unittest.TestCase):
    def test_all_present(self):
        hmap = {"SITE": 0, "PRICE": 1, "READY BY": 2}
        self.assertEqual(validate_headers(hmap), [])

    def test_missing_required(self):
        hmap = {"SITE": 0, "PRICE": 1}
        missing = validate_headers(hmap)
        self.assertIn("READY BY", missing)

    def test_empty(self):
        missing = validate_headers({})
        self.assertEqual(len(missing), 3)


class TestFormatPrice(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(format_price(1234567), "$1,234,567")

    def test_string(self):
        self.assertEqual(format_price("1234567"), "$1,234,567")

    def test_with_dollar(self):
        self.assertEqual(format_price("$1,234,567"), "$1,234,567")

    def test_float(self):
        self.assertEqual(format_price("1234567.89"), "$1,234,567")

    def test_empty(self):
        self.assertEqual(format_price(""), "")
        self.assertEqual(format_price(None), "")


class TestParseReadyBy(unittest.TestCase):
    def test_mm_dd_yyyy(self):
        self.assertEqual(parse_ready_by("12/27/2026"), "12/27/2026")

    def test_zero_pad(self):
        self.assertEqual(parse_ready_by("1/5/2026"), "01/05/2026")

    def test_yyyy_mm_dd(self):
        self.assertEqual(parse_ready_by("2026-12-27"), "12/27/2026")

    def test_python_date(self):
        self.assertEqual(parse_ready_by(date(2026, 12, 27)), "12/27/2026")

    def test_python_datetime(self):
        self.assertEqual(parse_ready_by(datetime(2026, 12, 27, 10, 30)), "12/27/2026")

    def test_sheets_serial(self):
        # 44922 = 12/27/2022 (approx)
        result = parse_ready_by("44922")
        self.assertTrue(result.endswith("/2022") or result.endswith("/2023"))

    def test_empty(self):
        self.assertEqual(parse_ready_by(""), "")
        self.assertEqual(parse_ready_by(None), "")


class TestParsePdfFilename(unittest.TestCase):
    def test_underscore_separated(self):
        result = parse_pdf_filename("Isla_101_2.pdf")
        self.assertEqual(result["community"], "Isla")
        self.assertEqual(result["homesite"], "101")
        self.assertEqual(result["floorplan"], "2")

    def test_space_separated(self):
        result = parse_pdf_filename("Isla 101 2.pdf")
        self.assertEqual(result["community"], "Isla")
        self.assertEqual(result["homesite"], "101")
        self.assertEqual(result["floorplan"], "2")

    def test_hyphen_separated(self):
        result = parse_pdf_filename("Isla-101-2.pdf")
        self.assertEqual(result["community"], "Isla")
        self.assertEqual(result["homesite"], "101")
        self.assertEqual(result["floorplan"], "2")

    def test_none_on_invalid(self):
        self.assertIsNone(parse_pdf_filename(""))
        self.assertIsNone(parse_pdf_filename(None))
        self.assertIsNone(parse_pdf_filename("random.pdf"))


class TestComputeHash(unittest.TestCase):
    def test_deterministic(self):
        h1 = compute_hash(b"hello")
        h2 = compute_hash(b"hello")
        self.assertEqual(h1, h2)

    def test_different(self):
        h1 = compute_hash(b"hello")
        h2 = compute_hash(b"world")
        self.assertNotEqual(h1, h2)


class TestNormalizeForCompare(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(normalize_for_compare("  Hello  "), "HELLO")

    def test_none(self):
        self.assertEqual(normalize_for_compare(None), "")


if __name__ == "__main__":
    unittest.main()
