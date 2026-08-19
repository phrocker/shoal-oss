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
FIXTURE_PATHS = {
    "docs/testdata/validate_sharkbite_matrix/fixture_a.go",
    "docs/testdata/validate_sharkbite_matrix/fixture_b.go",
}


def load_fixture_lines(name: str) -> list[str]:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8").splitlines()


def fixture_evidence_cell(name: str) -> str:
    for line in load_fixture_lines(name):
        if line.startswith("| SB-"):
            return [cell.strip() for cell in line.split("|")[1:-1]][4]
    raise AssertionError(f"fixture {name} did not contain a matrix row")


class ValidateSharkbiteMatrixTests(unittest.TestCase):
    def test_extract_path_anchor_bindings_keeps_adjacent_pairs(self) -> None:
        bindings = validator.extract_path_anchor_bindings(
            fixture_evidence_cell("multi_file_ok.md"),
            FIXTURE_PATHS,
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
            FIXTURE_PATHS,
        )
        self.assertEqual(
            bindings["docs/testdata/validate_sharkbite_matrix/fixture_a.go"],
            ["TestAlpha"],
        )
        self.assertEqual(
            bindings["docs/testdata/validate_sharkbite_matrix/fixture_b.go"],
            ["TestBeta"],
        )

    def test_validate_targeted_symbol_anchors_rejects_cross_file_anchor_leakage(self) -> None:
        stderr = StringIO()
        with redirect_stderr(stderr):
            with self.assertRaises(SystemExit):
                validator.validate_targeted_symbol_anchors(
                    load_fixture_lines("multi_file_stale.md"),
                    targeted_paths=FIXTURE_PATHS,
                )
        self.assertIn("StaleTest", stderr.getvalue())
        self.assertIn("fixture_b.go", stderr.getvalue())

    def test_validate_targeted_symbol_anchors_accepts_adjacent_multi_file_fixture(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("multi_file_ok.md"),
            targeted_paths=FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_resets_after_non_target_file_citation(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("non_target_separator.md"),
            targeted_paths=FIXTURE_PATHS,
        )


if __name__ == "__main__":
    unittest.main()
