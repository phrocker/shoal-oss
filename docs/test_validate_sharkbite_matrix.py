from __future__ import annotations

from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from unittest import mock
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


def load_row_manifest_fixture(name: str) -> tuple[tuple[str, str], ...]:
    return validator.parse_row_manifest_lines(load_fixture_lines(name), source=name)


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


MATRIX_HEADER = "| ID | Sharkbite | Shoal Go | Shoal C ABI | Evidence | Status | Notes |"
MATRIX_SEPARATOR = "| --- | --- | --- | --- | --- | --- | --- |"
MATRIX_ROW = "| SB-FIXTURE-101 | — | — | — | — | Covered | fixture row |"


def matrix_table(header: str, separator: str | None, *rows: str) -> list[str]:
    lines = ["## Fixture heading", "", header]
    if separator is not None:
        lines.append(separator)
    lines.extend(rows)
    lines.append("")
    return lines


def replace_standalone_number(text: str, old: int, new: int) -> str:
    return re.sub(rf"(?<![\d,]){old}(?![\d,])", str(new), text)


def delete_matrix_row_consistently(text: str, row_id: str, prefix: str) -> str:
    """Delete one `Not required` row and restate every number the document derives.

    The result satisfies every internal cross-check — metadata, per-status
    summary, per-category summary and the narrative prose — while holding one
    row fewer than the audited inventory.
    """
    lines = text.splitlines()
    kept = [line for line in lines if not line.startswith(f"| {row_id} |")]
    assert len(kept) == len(lines) - 1, f"row {row_id} was not unique in the document"

    mutated = "\n".join(kept)
    mutated = replace_standalone_number(mutated, 3203, 3202)
    mutated = replace_standalone_number(mutated, 392, 391)

    adjusted: list[str] = []
    for line in mutated.splitlines():
        cells = [cell.strip() for cell in line.split("|")[1:-1]]
        if len(cells) == 9 and cells[1] == f"`{prefix}`":
            cells[2] = str(int(cells[2]) - 1)
            cells[8] = str(int(cells[8]) - 1)
            line = "| " + " | ".join(cells) + " |"
        adjusted.append(line)
    return "\n".join(adjusted) + "\n"


def pinned_constants_for_deleted_not_required_row(prefix: str, row_id: str) -> dict:
    status_counts = dict(validator.EXPECTED_STATUS_COUNTS)
    status_counts[validator.NOT_REQUIRED_STATUS] -= 1
    prefix_totals = dict(validator.EXPECTED_PREFIX_TOTALS)
    prefix_totals[prefix] -= 1
    prefix_counts = {
        name: dict(counts) for name, counts in validator.EXPECTED_PREFIX_COUNTS.items()
    }
    prefix_counts[prefix][validator.NOT_REQUIRED_STATUS] -= 1
    manifest = tuple(entry for entry in validator.load_expected_rows() if entry[0] != row_id)
    return {
        "EXPECTED_TOTAL_ROWS": validator.EXPECTED_TOTAL_ROWS - 1,
        "EXPECTED_REQUIRED_ROWS": validator.EXPECTED_REQUIRED_ROWS,
        "EXPECTED_STATUS_COUNTS": status_counts,
        "EXPECTED_PREFIX_TOTALS": prefix_totals,
        "EXPECTED_PREFIX_COUNTS": prefix_counts,
        "load_expected_rows": lambda: manifest,
    }


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
        self.assertEqual(row_ids, (("SB-FIXTURE-101", "Covered"), ("SB-FIXTURE-102", "Missing Go")))
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
        self.assertEqual(declared_counts["Covered"], 1)
        self.assertEqual(declared_counts["Missing Go"], 2420)
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

    def test_validate_expected_row_sequence_accepts_valid_fixture_sequence(self) -> None:
        validator.validate_expected_row_sequence(
            load_row_manifest_fixture("row_sequence_valid.txt"),
            load_row_manifest_fixture("row_sequence_expected.txt"),
        )

    def test_validate_expected_row_sequence_rejects_swapped_fixture_sequence(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_expected_row_sequence(
                load_row_manifest_fixture("row_sequence_swap.txt"),
                load_row_manifest_fixture("row_sequence_expected.txt"),
            ),
            "revision 18 inventory rows changed",
            "missing [none]",
            "unexpected [none]",
            "moved [SB-FIXTURE-102 expected 2 found 3, SB-FIXTURE-103 expected 3 found 2]",
        )

    def test_validate_expected_row_sequence_rejects_reordered_fixture_sequence(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_expected_row_sequence(
                load_row_manifest_fixture("row_sequence_reordered.txt"),
                load_row_manifest_fixture("row_sequence_expected.txt"),
            ),
            "revision 18 inventory rows changed",
            "missing [none]",
            "unexpected [none]",
            "moved [SB-FIXTURE-101 expected 1 found 2",
        )

    def test_validate_expected_row_sequence_rejects_missing_fixture_row(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_expected_row_sequence(
                load_row_manifest_fixture("row_sequence_missing.txt"),
                load_row_manifest_fixture("row_sequence_expected.txt"),
            ),
            "revision 18 inventory rows changed",
            "missing [SB-FIXTURE-103 (Behavior mismatch)]",
        )

    def test_validate_expected_row_sequence_rejects_added_fixture_row(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_expected_row_sequence(
                load_row_manifest_fixture("row_sequence_added.txt"),
                load_row_manifest_fixture("row_sequence_expected.txt"),
            ),
            "revision 18 inventory rows changed",
            "unexpected [SB-FIXTURE-999 (Missing Go)]",
        )

    def test_validate_expected_row_sequence_rejects_status_swap_within_a_section(self) -> None:
        self.assert_validation_fails(
            lambda: validator.validate_expected_row_sequence(
                load_row_manifest_fixture("row_sequence_reclassified.txt"),
                load_row_manifest_fixture("row_sequence_expected.txt"),
            ),
            "revision 18 inventory rows changed",
            "missing [none]",
            "unexpected [none]",
            "moved [none]",
            "reclassified [SB-FIXTURE-101 pinned Missing C ABI found Missing Go, "
            "SB-FIXTURE-102 pinned Missing Go found Missing C ABI]",
        )

    def test_parse_row_manifest_lines_requires_a_pinned_status(self) -> None:
        self.assert_validation_fails(
            lambda: load_row_manifest_fixture("row_sequence_missing_status.txt"),
            "invalid row manifest entry",
            "expected 'ROW-ID STATUS'",
        )

    def test_parse_row_manifest_lines_rejects_unknown_status(self) -> None:
        self.assert_validation_fails(
            lambda: load_row_manifest_fixture("row_sequence_invalid_status.txt"),
            "invalid status in row manifest entry",
            "'Totally Covered'",
        )

    def test_row_manifest_pins_the_status_of_every_audited_row(self) -> None:
        rows = validator.load_expected_rows()
        self.assertEqual(len(rows), validator.EXPECTED_TOTAL_ROWS)
        self.assertEqual(len({row_id for row_id, _status in rows}), validator.EXPECTED_TOTAL_ROWS)
        for _row_id, status in rows:
            self.assertIn(status, validator.STATUSES)
        document_rows = validator.parse_rows(load_document_text().splitlines())[2]
        self.assertEqual(rows, document_rows)

    def test_gap_completion_consistency_accepts_complete_pairs(self) -> None:
        lines = load_fixture_lines("gap_completion_valid.md")
        rows = validator.parse_rows(lines)[2]
        validator.validate_gap_completion_consistency(lines, rows)

    def test_gap_completion_consistency_rejects_completion_drift(self) -> None:
        lines = load_fixture_lines("gap_completion_drift.md")
        rows = validator.parse_rows(lines)[2]
        self.assert_validation_fails(
            lambda: validator.validate_gap_completion_consistency(lines, rows),
            "SB-GAP-C-001 claims completion, but referenced rows remain Missing C ABI",
            "SB-CPP-016 (Missing C ABI)",
        )

    def test_pinned_inventory_rejects_status_swap_between_rows_in_one_section(self) -> None:
        text = load_document_text()
        first = "| SB-PKG-001 |"
        second = "| SB-PKG-014 |"
        lines = text.splitlines()
        first_index = next(i for i, line in enumerate(lines) if line.startswith(first))
        second_index = next(i for i, line in enumerate(lines) if line.startswith(second))
        first_cells = lines[first_index].split("|")
        second_cells = lines[second_index].split("|")
        self.assertNotEqual(first_cells[-3].strip(), second_cells[-3].strip())
        first_cells[-3], second_cells[-3] = second_cells[-3], first_cells[-3]
        lines[first_index] = "|".join(first_cells)
        lines[second_index] = "|".join(second_cells)
        mutated = "\n".join(lines) + "\n"

        self.assert_validation_fails(
            lambda: validator.validate_counts(mutated.splitlines(), mutated),
            "revision 18 inventory rows changed",
            "reclassified [SB-PKG-001 pinned Missing C ABI found Intentional divergence "
            "(approval required), SB-PKG-014 pinned Intentional divergence (approval required) "
            "found Missing C ABI]",
        )

    def test_validate_revision_inventory_rejects_same_prefix_row_id_substitution(self) -> None:
        rewritten_text = replace_pattern_once(
            load_document_text(),
            r"^\| SB-PKG-001 \|",
            "| SB-PKG-999 |",
        )
        self.assert_validation_fails(
            lambda: validator.validate_counts(rewritten_text.splitlines(), rewritten_text),
            "revision 18 inventory rows changed",
            "missing [SB-PKG-001 (Missing C ABI)]",
            "unexpected [SB-PKG-999 (Missing C ABI)]",
        )

    def test_validate_revision_inventory_rejects_row_deletion(self) -> None:
        rewritten_text = "\n".join(remove_line_starting_once(load_document_text().splitlines(), "| SB-PKG-001 |"))
        self.assert_validation_fails(
            lambda: validator.validate_counts(rewritten_text.splitlines(), rewritten_text),
            "revision 18 inventory rows changed",
            "missing [SB-PKG-001 (Missing C ABI)]",
        )

    def test_validate_revision_inventory_rejects_row_addition(self) -> None:
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
            "revision 18 inventory rows changed",
            "unexpected [SB-PKG-999 (Missing C ABI)]",
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

    # ---- pinned audited inventory ------------------------------------------

    def test_pinned_inventory_constants_are_internally_consistent(self) -> None:
        validator.validate_pinned_inventory_constants()
        self.assertEqual(validator.EXPECTED_REVISION, 18)
        self.assertEqual(validator.EXPECTED_TOTAL_ROWS, 3203)
        self.assertEqual(validator.EXPECTED_REQUIRED_ROWS, 2811)
        self.assertEqual(
            validator.EXPECTED_STATUS_COUNTS,
            {
                "Covered": 1,
                "Missing Go": 2420,
                "Missing C ABI": 99,
                "Behavior mismatch": 204,
                validator.INTENTIONAL_DIVERGENCE_STATUS: 87,
                validator.NOT_REQUIRED_STATUS: 392,
            },
        )

    def test_pinned_inventory_constants_reject_incoherent_edit(self) -> None:
        with mock.patch.object(validator, "EXPECTED_TOTAL_ROWS", 3202):
            self.assert_validation_fails(
                validator.validate_pinned_inventory_constants,
                "pinned per-status counts sum to 3203",
                "EXPECTED_TOTAL_ROWS is 3202",
            )

    def test_pinned_inventory_constants_reject_section_count_drift(self) -> None:
        drifted = {
            prefix: dict(counts) for prefix, counts in validator.EXPECTED_PREFIX_COUNTS.items()
        }
        drifted["SB-EMB"][validator.NOT_REQUIRED_STATUS] -= 1
        with mock.patch.object(validator, "EXPECTED_PREFIX_COUNTS", drifted):
            self.assert_validation_fails(
                validator.validate_pinned_inventory_constants,
                "pinned status counts for SB-EMB sum to 34",
                "EXPECTED_PREFIX_TOTALS pins 35",
            )

    def test_pinned_inventory_rejects_row_deletion_with_consistent_prose(self) -> None:
        mutated = delete_matrix_row_consistently(load_document_text(), "SB-EMB-035", "SB-EMB")
        message = self.assert_validation_fails(
            lambda: validator.validate_counts(mutated.splitlines(), mutated),
            "revision 18 inventory rows changed: missing [SB-EMB-035 (Not required (rationale required))]",
        )
        self.assertIn("must update EXPECTED_REVISION", message)
        self.assertIn("row manifest", message)

    def test_pinned_counts_reject_row_deletion_when_the_manifest_is_relaxed(self) -> None:
        mutated = delete_matrix_row_consistently(load_document_text(), "SB-EMB-035", "SB-EMB")
        manifest = tuple(
            entry
            for entry in validator.load_expected_rows()
            if entry[0] != "SB-EMB-035"
        )
        with mock.patch.object(
            validator, "load_expected_rows", lambda: manifest
        ):
            self.assert_validation_fails(
                lambda: validator.validate_counts(mutated.splitlines(), mutated),
                "revision 18 inventory expects 3203 rows, found 3202",
            )

    def test_row_deletion_with_consistent_prose_satisfies_only_internal_cross_checks(self) -> None:
        mutated = delete_matrix_row_consistently(load_document_text(), "SB-EMB-035", "SB-EMB")
        with mock.patch.multiple(
            validator,
            **pinned_constants_for_deleted_not_required_row("SB-EMB", "SB-EMB-035"),
        ):
            validator.validate_counts(mutated.splitlines(), mutated)

    def test_pinned_inventory_rejects_section_total_shift(self) -> None:
        status_counts, prefix_counts, row_ids = validator.parse_rows(
            load_document_text().splitlines()
        )
        shifted = {prefix: counts.copy() for prefix, counts in prefix_counts.items()}
        shifted["SB-EMB"]["Missing Go"] += 1
        shifted["SB-XCUT"]["Missing Go"] -= 1
        self.assert_validation_fails(
            lambda: validator.validate_revision_inventory(row_ids, status_counts, shifted),
            "revision 18 inventory expects 35 rows for SB-EMB, found 36",
        )

    def test_pinned_inventory_rejects_status_reclassification(self) -> None:
        status_counts, prefix_counts, row_ids = validator.parse_rows(
            load_document_text().splitlines()
        )
        reclassified = status_counts.copy()
        reclassified["Missing Go"] -= 1
        reclassified["Covered"] += 1
        self.assert_validation_fails(
            lambda: validator.validate_revision_inventory(
                row_ids, reclassified, prefix_counts
            ),
            "revision 18 inventory expects 1 rows for Covered, found 2",
        )

    def test_declared_count_edit_still_fails_internal_cross_check(self) -> None:
        text = load_document_text()
        mutated = replace_pattern_once(
            text, re.escape("| Missing Go | 2420 |"), "| Missing Go | 2419 |"
        )
        self.assert_validation_fails(
            lambda: validator.validate_counts(mutated.splitlines(), mutated),
            "status summary says 2419 rows for Missing Go, but parsed 2420",
        )

    def test_revision_bump_requires_validator_constant_update(self) -> None:
        text = load_document_text()
        mutated = text.replace(
            f"Revision {validator.EXPECTED_REVISION} — applies the seventeenth independent audit",
            f"Revision {validator.EXPECTED_REVISION + 1} — applies the eighteenth independent audit",
        ).replace(
            f"As of revision {validator.EXPECTED_REVISION} that is",
            f"As of revision {validator.EXPECTED_REVISION + 1} that is",
        )
        self.assertNotEqual(mutated, text)
        self.assert_validation_fails(
            lambda: validator.validate_counts(mutated.splitlines(), mutated),
            "document status is missing expected detail: Revision 18 — applies the seventeenth",
        )

    # ---- matrix table separators -------------------------------------------

    def test_parse_rows_accepts_matrix_table_with_separator(self) -> None:
        status_counts, prefix_counts, row_ids = validator.parse_rows(
            matrix_table(MATRIX_HEADER, MATRIX_SEPARATOR, MATRIX_ROW)
        )
        self.assertEqual(row_ids, (("SB-FIXTURE-101", "Covered"),))
        self.assertEqual(status_counts["Covered"], 1)
        self.assertEqual(prefix_counts["SB-FIXTURE"]["Covered"], 1)

    def test_parse_rows_requires_separator_after_matrix_header(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_rows(matrix_table(MATRIX_HEADER, None, MATRIX_ROW)),
            "missing separator row after the matrix table header on line 3",
        )

    def test_parse_rows_requires_separator_when_matrix_header_ends_the_document(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_rows([MATRIX_HEADER]),
            "missing separator row after the matrix table header on line 1",
        )

    def test_parse_rows_rejects_malformed_separator_after_matrix_header(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_rows(
                matrix_table(
                    MATRIX_HEADER,
                    "| --- | --- | -- | --- | --- | --- | --- |",
                    MATRIX_ROW,
                )
            ),
            "malformed separator row after the matrix table header on line 3",
            "column 3: invalid separator cell '--'",
        )

    def test_parse_rows_rejects_wrong_width_separator_after_matrix_header(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_rows(
                matrix_table(MATRIX_HEADER, "| --- | --- | --- |", MATRIX_ROW)
            ),
            "malformed separator row after the matrix table header on line 3",
            "expected 7 cells, found 3",
        )

    def test_parse_rows_rejects_separator_inside_matrix_table(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_rows(
                matrix_table(MATRIX_HEADER, MATRIX_SEPARATOR, MATRIX_ROW, MATRIX_SEPARATOR)
            ),
            "unexpected separator row inside a matrix table on line 6",
        )

    def test_matrix_rows_are_scoped_to_their_own_table(self) -> None:
        _status_counts, _prefix_counts, row_ids = validator.parse_rows(
            [
                MATRIX_HEADER,
                MATRIX_SEPARATOR,
                MATRIX_ROW,
                "",
                "| Other | Table |",
                "| --- | --- |",
                "| value | value |",
            ]
        )
        self.assertEqual(row_ids, (("SB-FIXTURE-101", "Covered"),))

    def test_parse_markdown_table_accepts_alignment_separators(self) -> None:
        headers, rows = validator.parse_markdown_table(
            [
                "## Example heading",
                "",
                "| Field | Value | Extra |",
                "| :--- | ---: | :---: |",
                "| Alpha | Beta | Gamma |",
            ],
            "## Example heading",
        )
        self.assertEqual(headers, ["Field", "Value", "Extra"])
        self.assertEqual(rows, [["Alpha", "Beta", "Gamma"]])

    def test_parse_markdown_table_rejects_extra_separator_row(self) -> None:
        self.assert_validation_fails(
            lambda: validator.parse_markdown_table(
                [
                    "## Example heading",
                    "",
                    "| Field | Value |",
                    "| --- | --- |",
                    "| Alpha | Beta |",
                    "| --- | --- |",
                    "| Gamma | Delta |",
                ],
                "## Example heading",
            ),
            "unexpected extra separator row under ## Example heading on line 6",
        )


if __name__ == "__main__":
    unittest.main()
