"""Unit tests for runner module - lock, manifest, idempotency."""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.runner import (
    acquire_lock,
    release_lock,
    load_manifest,
    save_manifest,
    is_already_processed,
    load_certifications,
    save_certifications,
    LOCK_FILE,
)


class TestLock(unittest.TestCase):
    def setUp(self):
        self._original_lock = LOCK_FILE
        # Use temp dir for lock
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        release_lock()

    def test_acquire_release(self):
        """Lock can be acquired and released."""
        import src.runner as runner
        runner.LOCK_FILE = os.path.join(self.tmpdir, ".process_lock")
        self.assertTrue(acquire_lock())
        release_lock()
        runner.LOCK_FILE = self._original_lock

    def test_force_override(self):
        """Force flag overrides existing lock."""
        import src.runner as runner
        runner.LOCK_FILE = os.path.join(self.tmpdir, ".process_lock")
        acquire_lock()
        self.assertTrue(acquire_lock(force=True))
        release_lock()
        runner.LOCK_FILE = self._original_lock


class TestManifest(unittest.TestCase):
    def test_save_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "manifest.json")
            data = {"file1": {"name": "test.pdf", "processed_at": "2024-01-01"}}
            save_manifest(path, data)
            loaded = load_manifest(path)
            self.assertEqual(loaded["file1"]["name"], "test.pdf")

    def test_load_missing(self):
        loaded = load_manifest("/nonexistent/path/manifest.json")
        self.assertEqual(loaded, {})


class TestIdempotency(unittest.TestCase):
    def test_processed_by_app_properties(self):
        pdf = {"id": "abc", "name": "test.pdf", "appProperties": {"processed": "true"}}
        self.assertTrue(is_already_processed(pdf, {}))

    def test_processed_by_manifest(self):
        pdf = {"id": "abc", "name": "test.pdf"}
        manifest = {"abc": {"processed_at": "2024-01-01"}}
        self.assertTrue(is_already_processed(pdf, manifest))

    def test_not_processed(self):
        pdf = {"id": "abc", "name": "test.pdf"}
        self.assertFalse(is_already_processed(pdf, {}))


class TestCertification(unittest.TestCase):
    def test_save_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            import src.runner as runner
            original = runner.CERT_FILE
            runner.CERT_FILE = os.path.join(tmpdir, "certs.json")

            certs = {"file1": {"file_name": "test.docx", "modifiedTime": "2024-01-01"}}
            save_certifications(certs)
            loaded = load_certifications()
            self.assertEqual(loaded["file1"]["file_name"], "test.docx")

            runner.CERT_FILE = original


if __name__ == "__main__":
    unittest.main()
