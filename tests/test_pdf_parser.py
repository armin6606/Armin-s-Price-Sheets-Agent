"""Unit tests for PDF parser and release filename parser."""

import os
import sys
import unittest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.pdf_parser import (
    parse_release_filename,
    _clean_price,
    _safe_get,
    _is_totals_row,
    _parse_metadata,
    ReleaseHomesite,
    ReleaseMeta,
)


class TestParseReleaseFilename(unittest.TestCase):
    """Tests for parse_release_filename()."""

    def test_basic(self):
        result = parse_release_filename("Nova Phase 2D.pdf")
        self.assertEqual(result["community"], "Nova")
        self.assertEqual(result["phase"], "2D")

    def test_multiword_community(self):
        result = parse_release_filename("Cielo Vista Phase 1B.pdf")
        self.assertEqual(result["community"], "Cielo Vista")
        self.assertEqual(result["phase"], "1B")

    def test_numeric_phase(self):
        result = parse_release_filename("Strata Phase 2.pdf")
        self.assertEqual(result["community"], "Strata")
        self.assertEqual(result["phase"], "2")

    def test_case_insensitive(self):
        result = parse_release_filename("NOVA PHASE 3A.PDF")
        self.assertEqual(result["community"], "NOVA")
        self.assertEqual(result["phase"], "3A")

    def test_no_phase(self):
        result = parse_release_filename("SomeDocument.pdf")
        self.assertEqual(result["community"], "SomeDocument")
        self.assertEqual(result["phase"], "")

    def test_empty(self):
        self.assertIsNone(parse_release_filename(""))
        self.assertIsNone(parse_release_filename(None))


class TestCleanPrice(unittest.TestCase):
    """Tests for _clean_price() helper."""

    def test_normal(self):
        self.assertEqual(_clean_price("$1,045,990"), "$1,045,990")

    def test_spaces_in_price(self):
        self.assertEqual(_clean_price("$ 1 ,095,990"), "$1,095,990")

    def test_dollar_dash(self):
        self.assertEqual(_clean_price("$ -"), "")
        self.assertEqual(_clean_price("$-"), "")

    def test_empty(self):
        self.assertEqual(_clean_price(""), "")
        self.assertEqual(_clean_price(None), "")

    def test_just_dollar(self):
        self.assertEqual(_clean_price("$"), "")

    def test_whitespace(self):
        self.assertEqual(_clean_price("   "), "")


class TestSafeGet(unittest.TestCase):
    """Tests for _safe_get() helper."""

    def test_normal(self):
        table = [["a", "b", "c"], ["d", "e", "f"]]
        self.assertEqual(_safe_get(table, 0, 1), "b")

    def test_out_of_bounds_row(self):
        table = [["a"]]
        self.assertEqual(_safe_get(table, 5, 0), "")

    def test_out_of_bounds_col(self):
        table = [["a"]]
        self.assertEqual(_safe_get(table, 0, 5), "")

    def test_none_value(self):
        table = [["a", None, "c"]]
        self.assertEqual(_safe_get(table, 0, 1), "")


class TestIsTotalsRow(unittest.TestCase):
    """Tests for _is_totals_row() helper."""

    def test_totals(self):
        self.assertTrue(_is_totals_row(["TOTALS", None, None]))

    def test_total(self):
        self.assertTrue(_is_totals_row(["Total", "123"]))

    def test_not_totals(self):
        self.assertFalse(_is_totals_row(["4/9", "28", "1"]))

    def test_empty(self):
        self.assertFalse(_is_totals_row([]))
        self.assertFalse(_is_totals_row(None))


class TestParseMetadata(unittest.TestCase):
    """Tests for _parse_metadata() helper."""

    def test_normal(self):
        table = [["Community:", "NOVA", "Phase:", "2D", "Release Date:", "February 9, 2026", "COE:", "April, 2026", ""]]
        result = _parse_metadata(table)
        self.assertIsNotNone(result)
        self.assertEqual(result.community, "NOVA")
        self.assertEqual(result.phase, "2D")
        self.assertEqual(result.release_date, "February 9, 2026")
        self.assertEqual(result.coe, "April, 2026")

    def test_missing_community(self):
        table = [["Phase:", "2D"]]
        result = _parse_metadata(table)
        self.assertIsNone(result)

    def test_empty(self):
        result = _parse_metadata([])
        self.assertIsNone(result)
        result = _parse_metadata(None)
        self.assertIsNone(result)


class TestReleaseHomesiteDataclass(unittest.TestCase):
    """Tests for ReleaseHomesite dataclass defaults."""

    def test_defaults(self):
        hs = ReleaseHomesite(
            coe_date="4/9",
            homesite="28",
            plan="1",
            plan_elev="AR",
            base_price="$1,045,990",
        )
        self.assertEqual(hs.homesite, "28")
        self.assertEqual(hs.plan, "1")
        self.assertEqual(hs.net_price, "")
        self.assertEqual(hs.community, "")

    def test_with_all_fields(self):
        hs = ReleaseHomesite(
            coe_date="4/9",
            homesite="28",
            plan="1",
            plan_elev="AR",
            base_price="$1,045,990",
            premium_total="$50,000",
            total_released_price="$1,095,990",
            nrcc="$8,000",
            net_price="$1,087,990",
            community="NOVA",
            phase="2D",
        )
        self.assertEqual(hs.total_released_price, "$1,095,990")
        self.assertEqual(hs.net_price, "$1,087,990")
        self.assertEqual(hs.community, "NOVA")


class TestParsePDFIntegration(unittest.TestCase):
    """Integration test - only runs if the sample PDF exists."""

    SAMPLE_PDF = r"C:\Users\7316\Downloads\Nova Phase 2D.pdf"

    @unittest.skipUnless(
        os.path.exists(r"C:\Users\7316\Downloads\Nova Phase 2D.pdf"),
        "Sample PDF not found"
    )
    def test_parse_sample_pdf(self):
        from src.pdf_parser import parse_release_pdf
        result = parse_release_pdf(self.SAMPLE_PDF)

        self.assertEqual(result.meta.community, "NOVA")
        self.assertEqual(result.meta.phase, "2D")
        self.assertEqual(len(result.homesites), 2)

        hs1 = result.homesites[0]
        self.assertEqual(hs1.homesite, "28")
        self.assertEqual(hs1.plan, "1")
        self.assertEqual(hs1.plan_elev, "AR")
        self.assertEqual(hs1.base_price, "$1,045,990")
        self.assertEqual(hs1.total_released_price, "$1,095,990")
        self.assertEqual(hs1.net_price, "$1,087,990")

        hs2 = result.homesites[1]
        self.assertEqual(hs2.homesite, "42")
        self.assertEqual(hs2.plan, "2X")
        self.assertEqual(hs2.plan_elev, "BR")


if __name__ == "__main__":
    unittest.main()
