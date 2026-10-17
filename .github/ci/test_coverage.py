import contextlib
import copy
import io
from pathlib import Path
import unittest

from check_coverage import check


def report(lines=10, covered_lines=10, branches=4, covered_branches=4):
    return {"type": "llvm.coverage.json.export", "data": [{"files": [{
        "filename": str(Path("src/main.rs").resolve()),
        "summary": {"lines": {"count": lines, "covered": covered_lines},
                    "branches": {"count": branches, "covered": covered_branches}},
    }]}]}


class CoverageGateTests(unittest.TestCase):
    def run_check(self, value):
        with contextlib.redirect_stdout(io.StringIO()):
            return check(value)

    def test_exact_full_coverage(self):
        self.assertEqual(self.run_check(report()), {"lines": [10, 10], "branches": [4, 4]})

    def test_one_uncovered_line_cannot_round_to_one_hundred(self):
        with self.assertRaisesRegex(ValueError, "1 uncovered"):
            self.run_check(report(lines=1000000, covered_lines=999999))

    def test_one_uncovered_branch_blocks(self):
        with self.assertRaisesRegex(ValueError, "branches: 1 uncovered"):
            self.run_check(report(covered_branches=3))

    def test_absent_or_zero_branch_measurement_blocks(self):
        with self.assertRaisesRegex(ValueError, "No branches measured"):
            self.run_check(report(branches=0, covered_branches=0))
        value = report()
        del value["data"][0]["files"][0]["summary"]["branches"]
        with self.assertRaisesRegex(ValueError, "Missing integer branches"):
            self.run_check(value)

    def test_absent_empty_and_malformed_data_block(self):
        for value in ({}, {"type": "llvm.coverage.json.export", "data": []},
                      {"type": "llvm.coverage.json.export", "data": [{"files": []}]}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.run_check(value)

    def test_invalid_counts_block(self):
        for count, covered in ((-1, 0), (1, 2), (1, -1), (True, 1), ("1", 1)):
            value = report()
            value["data"][0]["files"][0]["summary"]["lines"] = {"count": count, "covered": covered}
            with self.subTest(count=count, covered=covered), self.assertRaises(ValueError):
                self.run_check(value)

    def test_dependency_coverage_cannot_mask_application_gaps(self):
        value = report(covered_lines=9)
        dependency = copy.deepcopy(value["data"][0]["files"][0])
        dependency["filename"] = str(Path("vendor/library/src/lib.rs").resolve())
        dependency["summary"]["lines"] = {"count": 1000000, "covered": 1000000}
        value["data"][0]["files"].append(dependency)
        with self.assertRaisesRegex(ValueError, "lines: 1 uncovered"):
            self.run_check(value)

    def test_external_source_cannot_replace_application_coverage(self):
        value = report()
        value["data"][0]["files"][0]["filename"] = "/another/repo/src/main.rs"
        with self.assertRaisesRegex(ValueError, "No application source"):
            self.run_check(value)


if __name__ == "__main__":
    unittest.main()
