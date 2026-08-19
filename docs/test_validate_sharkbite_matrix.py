from __future__ import annotations

from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
import sys
import unittest


DOCS_DIR = Path(__file__).parent
sys.path.insert(0, str(DOCS_DIR))

import validate_sharkbite_matrix as validator


FIXTURE_DIR = DOCS_DIR / "testdata" / "validate_sharkbite_matrix"
CROSS_FILE_FIXTURE_PATHS = {
    "docs/testdata/validate_sharkbite_matrix/fixture_a.go",
    "docs/testdata/validate_sharkbite_matrix/fixture_b.go",
}
BOUNDARY_FIXTURE_PATHS = {
    "docs/testdata/validate_sharkbite_matrix/fixture_a.go",
    "docs/testdata/validate_sharkbite_matrix/fixture_identifier_old.go",
    "docs/testdata/validate_sharkbite_matrix/fixture_signature.h",
}


def load_fixture_lines(name: str) -> list[str]:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8").splitlines()


def fixture_evidence_cell(name: str) -> str:
    for line in load_fixture_lines(name):
        if line.startswith("| SB-"):
            return [cell.strip() for cell in line.split("|")[1:-1]][4]
    raise AssertionError(f"fixture {name} did not contain a matrix row")


class ValidateSharkbiteMatrixTests(unittest.TestCase):
    def assert_validation_fails(self, func, *expected_parts: str) -> str:
        stderr = StringIO()
        with redirect_stderr(stderr):
            with self.assertRaises(SystemExit):
                func()
        message = stderr.getvalue()
        for part in expected_parts:
            self.assertIn(part, message)
        return message

    def test_extract_path_anchor_bindings_keeps_adjacent_pairs(self) -> None:
        bindings = validator.extract_path_anchor_bindings(
            fixture_evidence_cell("multi_file_ok.md"),
            CROSS_FILE_FIXTURE_PATHS,
        )
        self.assertEqual(
            bindings["docs/testdata/validate_sharkbite_matrix/fixture_a.go"],
            ["TestAlpha"],
        )
        self.assertEqual(
            bindings["docs/testdata/validate_sharkbite_matrix/fixture_b.go"],
            ["TestBeta"],
        )

    def test_extract_path_anchor_bindings_preserves_ordered_multi_path_citations(self) -> None:
        bindings = validator.extract_path_anchor_bindings(
            "`TestAlpha` via `TestBeta` "
            "(`docs/testdata/validate_sharkbite_matrix/fixture_a.go`; "
            "`docs/testdata/validate_sharkbite_matrix/fixture_b.go`)",
            CROSS_FILE_FIXTURE_PATHS,
        )
        self.assertEqual(
            bindings["docs/testdata/validate_sharkbite_matrix/fixture_a.go"],
            ["TestAlpha"],
        )
        self.assertEqual(
            bindings["docs/testdata/validate_sharkbite_matrix/fixture_b.go"],
            ["TestBeta"],
        )

    def test_parse_rows_accepts_unique_row_ids(self) -> None:
        status_counts, prefix_counts, row_ids = validator.parse_rows(load_fixture_lines("unique_rows.md"))
        self.assertEqual(row_ids, {"SB-FIXTURE-101", "SB-FIXTURE-102"})
        self.assertEqual(status_counts["Covered"], 1)
        self.assertEqual(status_counts["Missing Go"], 1)
        self.assertEqual(prefix_counts["SB-FIXTURE"]["Covered"], 1)
        self.assertEqual(prefix_counts["SB-FIXTURE"]["Missing Go"], 1)

    def test_parse_rows_rejects_duplicate_ids_with_same_status(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_rows(load_fixture_lines("duplicate_same_status.md")),
            "duplicate accepted row id SB-FIXTURE-101",
            "Covered vs Covered",
        )

    def test_parse_rows_rejects_duplicate_ids_with_different_status(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_rows(load_fixture_lines("duplicate_different_status.md")),
            "duplicate accepted row id SB-FIXTURE-101",
            "Covered vs Missing Go",
        )

    def test_validate_targeted_symbol_anchors_rejects_cross_file_anchor_leakage(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_targeted_symbol_anchors(
                load_fixture_lines("multi_file_stale.md"),
                targeted_paths=CROSS_FILE_FIXTURE_PATHS,
            ),
            "StaleTest",
            "fixture_b.go",
        )

    def test_validate_targeted_symbol_anchors_accepts_adjacent_multi_file_fixture(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("multi_file_ok.md"),
            targeted_paths=CROSS_FILE_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_accepts_exact_identifier_boundaries(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("identifier_boundary_ok.md"),
            targeted_paths=BOUNDARY_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_rejects_identifier_substrings(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_targeted_symbol_anchors(
                load_fixture_lines("identifier_boundary_stale.md"),
                targeted_paths=BOUNDARY_FIXTURE_PATHS,
            ),
            "TestAlpha",
            "fixture_identifier_old.go",
        )

    def test_validate_targeted_symbol_anchors_accepts_multiline_signature_anchor(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("signature_anchor_ok.md"),
            targeted_paths=BOUNDARY_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_accepts_full_multiline_signature_anchor(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("signature_full_multiline_ok.md"),
            targeted_paths=BOUNDARY_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_rejects_partial_signature_with_stale_type(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_targeted_symbol_anchors(
                load_fixture_lines("signature_anchor_partial_stale.md"),
                targeted_paths=BOUNDARY_FIXTURE_PATHS,
            ),
            "fixture_signature(stale_type row, ...)",
            "fixture_signature.h",
        )

    def test_validate_targeted_symbol_anchors_resets_after_non_target_file_citation(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("non_target_separator.md"),
            targeted_paths=CROSS_FILE_FIXTURE_PATHS,
        )


if __name__ == "__main__":
    unittest.main()
