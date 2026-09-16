"""Exercise the shared Make data target using tiny split gzip fixtures."""

import gzip
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent


class DataTargetTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="benchmarker-data-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.archive = self.root / "data.csv.gz"
        self.output = self.root / "data.csv"
        self.checksums = self.root / "SHA256SUMS"
        self.csv = (
            'id,body\n1,"A body with commas, doubled ""quotes"", and\n'
            'an embedded newline."\n2,"Unicode: café, 東京, Ελληνικά."\n'
        ).encode("utf-8")
        self.compressed = gzip.compress(self.csv, mtime=0)
        self.checksums.write_text(
            f"{hashlib.sha256(self.csv).hexdigest()}  data.csv\n"
        )

    def split(self):
        parts = []
        for number, start in enumerate(range(0, len(self.compressed), 7)):
            path = self.root / f"data.csv.gz.part{number:04d}"
            path.write_bytes(self.compressed[start:start + 7])
            parts.append(path)
        self.assertGreater(len(parts), 10)
        return parts

    def make_data(self):
        return subprocess.run(
            [
                "make", "--no-print-directory", "-f",
                str(REPO_ROOT / "Makefile.wikipedia"), "data",
                f"DATA_GZ={self.archive}", f"DATA_CSV={self.output}",
                f"CHECKSUMS={self.checksums}",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=20,
        )

    def assert_no_partial_output(self):
        self.assertEqual(list(self.root.glob("data.csv.partial.*")), [])

    def test_split_stream_and_reuse(self):
        self.split()
        result = self.make_data()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.output.read_bytes(), self.csv)
        self.assertFalse(self.archive.exists())
        self.assert_no_partial_output()
        modified = self.output.stat().st_mtime_ns
        result = self.make_data()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.output.stat().st_mtime_ns, modified)

    def test_single_gzip_fixture(self):
        self.archive.write_bytes(self.compressed)
        result = self.make_data()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.output.read_bytes(), self.csv)

    def test_parts_take_precedence_over_single_archive(self):
        self.split()
        self.archive.write_bytes(b"unused archive")
        result = self.make_data()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.output.read_bytes(), self.csv)

    def test_missing_middle_part(self):
        self.split()[1].unlink()
        result = self.make_data()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Missing archive part:", result.stderr)
        self.assertFalse(self.output.exists())
        self.assert_no_partial_output()

    def test_missing_tail(self):
        self.split()[-1].unlink()
        result = self.make_data()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())
        self.assert_no_partial_output()

    def test_corrupt_part(self):
        tail = self.split()[-1]
        contents = bytearray(tail.read_bytes())
        contents[-1] ^= 0xFF
        tail.write_bytes(contents)
        result = self.make_data()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())
        self.assert_no_partial_output()

    def test_wrong_csv_checksum_preserves_existing_output(self):
        self.split()
        self.output.write_bytes(b"previous CSV")
        os.utime(self.output, (1, 1))
        self.checksums.write_text(f"{'0' * 64}  data.csv\n")
        result = self.make_data()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CSV checksum mismatch", result.stderr)
        self.assertEqual(self.output.read_bytes(), b"previous CSV")
        self.assert_no_partial_output()


if __name__ == "__main__":
    unittest.main()
