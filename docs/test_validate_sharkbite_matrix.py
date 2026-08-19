from __future__ import annotations

from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
import re
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
ANCHOR_FIXTURE_PATHS = {
    "docs/testdata/validate_sharkbite_matrix/fixture_a.go",
    "docs/testdata/validate_sharkbite_matrix/fixture_go_signature.go",
    "docs/testdata/validate_sharkbite_matrix/fixture_identifier_old.go",
    "docs/testdata/validate_sharkbite_matrix/fixture_main.c",
    "docs/testdata/validate_sharkbite_matrix/fixture_python_signature.py",
    "docs/testdata/validate_sharkbite_matrix/fixture_signature.h",
    "docs/testdata/validate_sharkbite_matrix/fixture_signature_elsewhere.h",
}


def load_fixture_lines(name: str) -> list[str]:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8").splitlines()


def load_document_text() -> str:
    return validator.DOC_PATH.read_text(encoding="utf-8")


def replace_pattern_once(text: str, pattern: str, replacement: str) -> str:
    rewritten, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise AssertionError(f"expected exactly one match for pattern {pattern!r}")
    return rewritten


def remove_line_starting_once(lines: list[str], prefix: str) -> list[str]:
    removed = False
    rewritten: list[str] = []
    for line in lines:
        if not removed and line.startswith(prefix):
            removed = True
            continue
        rewritten.append(line)
    if not removed:
        raise AssertionError(f"expected to remove a line starting with {prefix!r}")
    return rewritten


def insert_after_line_starting_once(lines: list[str], prefix: str, new_line: str) -> list[str]:
    inserted = False
    rewritten: list[str] = []
    for line in lines:
        rewritten.append(line)
        if not inserted and line.startswith(prefix):
            rewritten.append(new_line)
            inserted = True
    if not inserted:
        raise AssertionError(f"expected to insert after a line starting with {prefix!r}")
    return rewritten


def make_anchor_fixture_lines(evidence: str) -> list[str]:
    return [
        "| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |",
        "| --- | --- | --- | --- | --- | --- | --- |",
        f"| SB-FIXTURE-401 | Example | — | — | {evidence} | Missing Go | |",
    ]


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

    def test_parse_rows_rejects_truncated_rows(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_rows(load_fixture_lines("schema_truncated.md")),
            "malformed SB row SB-FIXTURE-301",
            "expected 7 cells, found 6",
        )

    def test_parse_rows_rejects_extra_cell_rows(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_rows(load_fixture_lines("schema_extra_cell.md")),
            "malformed SB row SB-FIXTURE-302",
            "expected 7 cells, found 8",
        )

    def test_parse_rows_rejects_malformed_delimiter_rows(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_rows(load_fixture_lines("schema_malformed.md")),
            "malformed SB row SB-FIXTURE-303",
            "expected 7 cells, found 5",
        )

    def test_parse_markdown_table_reads_rows_after_heading(self) -> None:
        headers, rows = validator.parse_markdown_table(load_fixture_lines("table_separator_ok.md"), "## Example heading")
        self.assertEqual(headers, ["Field", "Value"])
        self.assertEqual(rows, [["Alpha", "Beta"], ["Gamma", "Delta"]])

    def test_parse_markdown_table_requires_immediate_separator(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_markdown_table(
                load_fixture_lines("table_separator_missing.md"),
                "## Example heading",
            ),
            "missing separator row after header under ## Example heading",
        )

    def test_parse_markdown_table_rejects_malformed_separator(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_markdown_table(
                load_fixture_lines("table_separator_malformed.md"),
                "## Example heading",
            ),
            "malformed separator row under ## Example heading",
            "| nope | --- |",
        )

    def test_parse_markdown_table_rejects_wrong_width_separator(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_markdown_table(
                load_fixture_lines("table_separator_wrong_width.md"),
                "## Example heading",
            ),
            "malformed separator row under ## Example heading",
            "expected 2 cells, found 3",
        )

    def test_parse_rows_metadata_extracts_total_and_required_counts(self) -> None:
        self.assertEqual(
            validator.parse_rows_metadata("3203 (2811 required by the [§2.2](#sec-2) release gate)"),
            (3203, 2811),
        )

    def test_parse_count_accepts_supported_plain_and_bold_integer_formats(self) -> None:
        self.assertEqual(validator.parse_count("0"), 0)
        self.assertEqual(validator.parse_count("3203"), 3203)
        self.assertEqual(validator.parse_count("3,203"), 3203)
        self.assertEqual(validator.parse_count("**3,203**"), 3203)

    def test_parse_count_rejects_unsupported_integer_formats(self) -> None:
        for cell in ("24x47", "-1", "2,44,7", "161.0", "392 rows"):
            with self.subTest(cell=cell):
                self.assert_validation_fails(
                    lambda value=cell: validator.parse_count(value),
                    f"unsupported count format: {cell!r}",
                )

    def test_parse_status_summary_reads_declared_totals(self) -> None:
        declared_counts, total = validator.parse_status_summary(load_fixture_lines("status_summary_valid.md"))
        self.assertEqual(total, 3203)
        self.assertEqual(declared_counts["Covered"], 0)
        self.assertEqual(declared_counts["Missing Go"], 2447)
        self.assertEqual(declared_counts["Not required (rationale required)"], 392)

    def test_parse_status_summary_rejects_unsupported_count_cells(self) -> None:
        for fixture_name, expected_cell in (
            ("status_summary_invalid_alphanumeric.md", "'24x47'"),
            ("status_summary_invalid_negative.md", "'-1'"),
            ("status_summary_invalid_bad_commas.md", "'2,44,7'"),
            ("status_summary_invalid_decimal.md", "'161.0'"),
            ("status_summary_invalid_extra_text.md", "'392 rows'"),
        ):
            with self.subTest(fixture_name=fixture_name):
                self.assert_validation_fails(
                    lambda name=fixture_name: validator.parse_status_summary(load_fixture_lines(name)),
                    "unsupported count format",
                    expected_cell,
                )

    def test_parse_status_summary_rejects_duplicate_total_rows(self) -> None:
        for fixture_name in (
            "status_summary_duplicate_total_same.md",
            "status_summary_duplicate_total_conflict.md",
        ):
            with self.subTest(fixture_name=fixture_name):
                self.assert_validation_fails(
                    lambda name=fixture_name: validator.parse_status_summary(load_fixture_lines(name)),
                    "duplicate total row in status summary table",
                )

    def test_parse_category_summary_reads_prefix_rows_and_totals(self) -> None:
        (
            declared_prefix_counts,
            declared_prefix_totals,
            declared_total_status_counts,
            declared_total_rows,
        ) = validator.parse_category_summary(
            [
                "### 25.2 By category",
                "",
                "| Section | Prefix | Rows | Covered | Missing Go | Missing C ABI | Behavior mismatch | Intentional divergence | Not required |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
                "| [§5](#sec-5) Packaging and imports | `SB-PKG` | 14 | 0 | 0 | 10 | 1 | 1 | 2 |",
                "| **Total** | | **14** | **0** | **0** | **10** | **1** | **1** | **2** |",
            ]
        )
        self.assertEqual(declared_total_rows, 14)
        self.assertEqual(declared_prefix_totals["SB-PKG"], 14)
        self.assertEqual(declared_prefix_counts["SB-PKG"]["Missing C ABI"], 10)
        self.assertEqual(
            declared_total_status_counts["Intentional divergence (approval required)"],
            1,
        )

    def test_parse_category_summary_rejects_duplicate_total_rows(self) -> None:
        for fixture_name in (
            "category_summary_duplicate_total_same.md",
            "category_summary_duplicate_total_conflict.md",
        ):
            with self.subTest(fixture_name=fixture_name):
                self.assert_validation_fails(
                    lambda name=fixture_name: validator.parse_category_summary(load_fixture_lines(name)),
                    "duplicate total row in category summary table",
                )

    def test_validate_revision_16_inventory_rejects_same_prefix_row_id_substitution(self) -> None:
        rewritten_text = replace_pattern_once(
            load_document_text(),
            r"^\| SB-PKG-001 \|",
            "| SB-PKG-999 |",
        )
        self.assert_validation_fails(
            lambda: validator.validate_counts(rewritten_text.splitlines(), rewritten_text),
            "revision 16 inventory row ids changed",
            "missing [SB-PKG-001]",
            "unexpected [SB-PKG-999]",
        )

    def test_validate_revision_16_inventory_rejects_row_deletion(self) -> None:
        rewritten_text = "\n".join(remove_line_starting_once(load_document_text().splitlines(), "| SB-PKG-001 |"))
        self.assert_validation_fails(
            lambda: validator.validate_counts(rewritten_text.splitlines(), rewritten_text),
            "revision 16 inventory row ids changed",
            "missing [SB-PKG-001]",
        )

    def test_validate_revision_16_inventory_rejects_row_addition(self) -> None:
        original_line = (
            "| SB-PKG-001 | Distribution `sharkbite`, version `1.2.0.3` (`setup.py:34-35`) | — | — | — | Missing C ABI | "
            "No Python packaging exists in Shoal. `Makefile` has `build`, `capi`, `test`, `test-hdfs`, `vet`, `clean` only — no wheel/sdist target. |"
        )
        added_line = original_line.replace("| SB-PKG-001 |", "| SB-PKG-999 |", 1)
        rewritten_text = "\n".join(
            insert_after_line_starting_once(load_document_text().splitlines(), "| SB-PKG-001 |", added_line)
        )
        self.assert_validation_fails(
            lambda: validator.validate_counts(rewritten_text.splitlines(), rewritten_text),
            "revision 16 inventory row ids changed",
            "unexpected [SB-PKG-999]",
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
            targeted_paths=ANCHOR_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_rejects_identifier_substrings(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_targeted_symbol_anchors(
                load_fixture_lines("identifier_boundary_stale.md"),
                targeted_paths=ANCHOR_FIXTURE_PATHS,
            ),
            "TestAlpha",
            "fixture_identifier_old.go",
        )

    def test_validate_targeted_symbol_anchors_accepts_multiline_signature_anchor(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("signature_anchor_ok.md"),
            targeted_paths=ANCHOR_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_accepts_full_multiline_signature_anchor(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("signature_full_multiline_ok.md"),
            targeted_paths=ANCHOR_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_rejects_partial_signature_with_stale_type(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_targeted_symbol_anchors(
                load_fixture_lines("signature_anchor_partial_stale.md"),
                targeted_paths=ANCHOR_FIXTURE_PATHS,
            ),
            "fixture_signature(stale_type row, ...)",
            "fixture_signature.h",
        )

    def test_validate_targeted_symbol_anchors_rejects_stale_type_from_other_construct(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_targeted_symbol_anchors(
                load_fixture_lines("signature_anchor_stale_elsewhere.md"),
                targeted_paths=ANCHOR_FIXTURE_PATHS,
            ),
            "fixture_signature(stale_type row, ...)",
            "fixture_signature_elsewhere.h",
        )

    def test_validate_targeted_symbol_anchors_accepts_go_signature_shorthand(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("go_signature_anchor_ok.md"),
            targeted_paths=ANCHOR_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_accepts_python_signature_shorthand(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("python_signature_anchor_ok.md"),
            targeted_paths=ANCHOR_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_accepts_main_void_shorthand(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("main_anchor_ok.md"),
            targeted_paths=ANCHOR_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_resets_after_non_target_file_citation(self) -> None:
        validator.validate_targeted_symbol_anchors(
            load_fixture_lines("non_target_separator.md"),
            targeted_paths=CROSS_FILE_FIXTURE_PATHS,
        )

    def test_validate_targeted_symbol_anchors_explicit_optional_path_allows_bare_citation(self) -> None:
        validator.validate_targeted_symbol_anchors(
            make_anchor_fixture_lines("`capi/include/shoal.h`"),
            targeted_paths={"capi/include/shoal.h"},
        )

    def test_validate_targeted_symbol_anchors_custom_target_path_requires_anchor(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_targeted_symbol_anchors(
                make_anchor_fixture_lines("`docs/testdata/validate_sharkbite_matrix/fixture_a.go`"),
                targeted_paths={"docs/testdata/validate_sharkbite_matrix/fixture_a.go"},
            ),
            "SB-FIXTURE-401 cites docs/testdata/validate_sharkbite_matrix/fixture_a.go without an adjacent local symbol/test anchor",
        )

    def test_validate_targeted_symbol_anchors_custom_target_path_accepts_anchor(self) -> None:
        validator.validate_targeted_symbol_anchors(
            make_anchor_fixture_lines(
                "`TestAlpha` (`docs/testdata/validate_sharkbite_matrix/fixture_a.go`)"
            ),
            targeted_paths={"docs/testdata/validate_sharkbite_matrix/fixture_a.go"},
        )


if __name__ == "__main__":
    unittest.main()
