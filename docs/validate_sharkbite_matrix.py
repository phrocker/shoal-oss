from __future__ import annotations

from collections.abc import Iterator, Sequence
from collections import Counter, defaultdict
from functools import lru_cache
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile


DOC_PATH = Path(__file__).with_name("sharkbite-compatibility.md")
EXPECTED_REVISION = 38
# Update this manifest only when the independently audited inventory itself
# changes; review every added/removed or reclassified ID in code review.
EXPECTED_ROW_MANIFEST = DOC_PATH.with_name(
    f"sharkbite-compatibility-revision{EXPECTED_REVISION}-rows.txt"
)
EXPECTED_C_ABI_SYMBOL_MANIFEST = DOC_PATH.with_name(
    f"sharkbite-compatibility-revision{EXPECTED_REVISION}-cabi-symbols.txt"
)

STATUSES = {
    "Covered",
    "Missing Go",
    "Missing C ABI",
    "Behavior mismatch",
    "Intentional divergence (approval required)",
    "Not required (rationale required)",
}

NOT_REQUIRED_STATUS = "Not required (rationale required)"
INTENTIONAL_DIVERGENCE_STATUS = "Intentional divergence (approval required)"
EXPECTED_TOTAL_ROWS = 3203
EXPECTED_REQUIRED_ROWS = 2811


def status_count_map(
    *,
    covered: int = 0,
    missing_go: int = 0,
    missing_c_abi: int = 0,
    behavior_mismatch: int = 0,
    intentional_divergence: int = 0,
    not_required: int = 0,
) -> dict[str, int]:
    return {
        "Covered": covered,
        "Missing Go": missing_go,
        "Missing C ABI": missing_c_abi,
        "Behavior mismatch": behavior_mismatch,
        "Intentional divergence (approval required)": intentional_divergence,
        "Not required (rationale required)": not_required,
    }


EXPECTED_STATUS_COUNTS = {
    "Covered": 131,
    "Missing Go": 2290,
    "Missing C ABI": 84,
    "Behavior mismatch": 219,
    "Intentional divergence (approval required)": 87,
    "Not required (rationale required)": 392,
}

EXPECTED_PREFIX_COUNTS = {
    "SB-BASE": status_count_map(covered=18, not_required=2),
    "SB-CFG": status_count_map(
        covered=12,
        behavior_mismatch=16,
        intentional_divergence=2,
        not_required=6,
    ),
    "SB-CONN": status_count_map(
        covered=1,
        missing_go=1,
        behavior_mismatch=10,
        intentional_divergence=1,
    ),
    "SB-CPP": status_count_map(
        missing_go=11,
        missing_c_abi=1,
        behavior_mismatch=50,
        not_required=8,
    ),
    "SB-CXX": status_count_map(
        missing_go=2230,
        covered=41,
        behavior_mismatch=15,
        not_required=304,
        missing_c_abi=36,
    ),
    "SB-DATA": status_count_map(
        covered=9,
        missing_go=9,
        missing_c_abi=1,
        behavior_mismatch=50,
        not_required=6,
    ),
    "SB-EMB": status_count_map(not_required=35),
    "SB-ERR": status_count_map(
        covered=5,
        missing_go=1,
        behavior_mismatch=6,
        intentional_divergence=1,
        not_required=3,
    ),
    "SB-HDFS": status_count_map(missing_go=26),
    "SB-LOG": status_count_map(missing_go=2, behavior_mismatch=1),
    "SB-NS": status_count_map(behavior_mismatch=8),
    "SB-PANDA": status_count_map(missing_c_abi=20, not_required=1),
    "SB-PKG": status_count_map(
        missing_c_abi=10,
        behavior_mismatch=1,
        intentional_divergence=1,
        not_required=2,
    ),
    "SB-RFILE": status_count_map(
        missing_go=1,
        covered=31,
        behavior_mismatch=1,
        not_required=3,
    ),
    "SB-SCAN": status_count_map(
        covered=5,
        missing_go=5,
        missing_c_abi=3,
        behavior_mismatch=10,
        not_required=5,
    ),
    "SB-SEC": status_count_map(behavior_mismatch=18, not_required=1),
    "SB-STAT": status_count_map(
        missing_go=1,
        intentional_divergence=82,
        not_required=1,
    ),
    "SB-TABLE": status_count_map(
        missing_go=1,
        covered=2,
        behavior_mismatch=13,
        not_required=6,
    ),
    "SB-TORCH": status_count_map(missing_c_abi=9),
    "SB-WRITE": status_count_map(
        covered=4,
        missing_go=1,
        missing_c_abi=2,
        behavior_mismatch=7,
        not_required=9,
    ),
    "SB-XCUT": status_count_map(
        covered=3,
        missing_go=1,
        missing_c_abi=2,
        behavior_mismatch=13,
    ),
}

EXPECTED_PREFIX_TOTALS = {
    "SB-BASE": 20,
    "SB-CFG": 36,
    "SB-CONN": 13,
    "SB-CPP": 70,
    "SB-CXX": 2626,
    "SB-DATA": 75,
    "SB-EMB": 35,
    "SB-ERR": 16,
    "SB-HDFS": 26,
    "SB-LOG": 3,
    "SB-NS": 8,
    "SB-PANDA": 21,
    "SB-PKG": 14,
    "SB-RFILE": 36,
    "SB-SCAN": 28,
    "SB-SEC": 19,
    "SB-STAT": 84,
    "SB-TABLE": 22,
    "SB-TORCH": 9,
    "SB-WRITE": 23,
    "SB-XCUT": 19,
}

EXPECTED_METADATA_FIELDS = {
    "Tracking issue": (
        'Shoal [#81](https://github.com/phrocker/shoal-oss/issues/81) — "docs: define and audit '
        'complete Sharkbite compatibility matrix" (parent '
        "[#59](https://github.com/phrocker/shoal-oss/issues/59); upstream target "
        "[phrocker/sharkbite#108](https://github.com/phrocker/sharkbite/issues/108))"
    ),
    "Sharkbite reference": (
        "`phrocker/sharkbite` @ `7f2625f74331b0cd4a75dc0484949c40f1409686` "
        "(\"Bump accumulo-core from 2.0.0 to 2.0.1 in /native-iterators-jni (#100)\", 2022-07-22)"
    ),
    "Sharkbite release line": "`sharkbite` 1.2.0.3 on PyPI (`setup.py:34-35`)",
    "Shoal reference": (
        "`phrocker/shoal-oss` exact audited baseline for revision 38 "
        "`3b28b8ea4364cbafc0f357727a75413dd68995c2` "
        "(\"Merge PR #151: complete the public key value API\") "
        "plus the column-visibility C ABI introduced in this revision"
    ),
    "Shoal C ABI version": "`SHOAL_ABI_VERSION 1u` (`capi/include/shoal_types.h`)",
}

EXPECTED_DOCUMENT_STATUS_SNIPPETS = (
    "Normative gate. Binding on all Sharkbite-compatibility work.",
    f"Revision {EXPECTED_REVISION} — completes the 31-row column-visibility expression tranche",
    "Revision 36 — completes the twelve-row streaming cursor tranche",
    "Revision 34 — completes the four-row compatibility-error tranche",
    "Revision 32 — completes the five-row high-level scanner facade",
    "Revision 26 — completes the 17-row data-model value C ABI",
    "Revision 23 — records the public data-model value types",
    "Revision 22 — reclassifies the thirty-one RFile and stream rows of [§15](#sec-15)",
    "Revision 21 — adds owned range and iterator-setting descriptor ABIs",
    "Revision 20 reclassifies the twelve client-configuration and instance-topology rows",
    "Revision 19 added the merged connector-identity C ABI",
    "Revision 18 applied the seventeenth independent audit",
    "Revision 9 applied the eighth audit",
)

# Every pin above describes one audited revision. Changing the inventory is
# therefore an explicit, reviewable edit of this file: the revision number, the
# totals, the per-status counts and the per-section counts must move together,
# in the same commit as the document change and the audit that justifies it.
INVENTORY_CHANGE_HINT = (
    "the audited inventory is pinned in docs/validate_sharkbite_matrix.py; an intentional "
    "inventory revision must update EXPECTED_REVISION, EXPECTED_TOTAL_ROWS, "
    "EXPECTED_REQUIRED_ROWS, EXPECTED_STATUS_COUNTS, EXPECTED_PREFIX_TOTALS, "
    "EXPECTED_PREFIX_COUNTS and the row manifest "
    f"docs/{EXPECTED_ROW_MANIFEST.name} (row ids, order and pinned statuses) "
    "in the same commit, together with the "
    "audit evidence that justifies the new inventory"
)

GAP_COMPLETION_RULES: dict[str, tuple[str, ...]] = {
    "SB-GAP-GO-001": ("Missing Go",),
    "SB-GAP-GO-002": ("Missing Go",),
    "SB-GAP-C-001": ("Missing Go", "Missing C ABI"),
    "SB-GAP-C-002": ("Missing Go", "Missing C ABI"),
    "SB-GAP-C-004": ("Missing Go", "Missing C ABI"),
    "SB-GAP-C-011": ("Missing Go", "Missing C ABI"),
    "SB-GAP-GO-011": ("Missing Go",),
    "SB-GAP-GO-012": ("Missing Go",),
    "SB-GAP-C-008": ("Missing Go", "Missing C ABI"),
    "SB-GAP-C-009": ("Missing Go", "Missing C ABI"),
}

EXPECTED_ROW_MANIFEST_HEADER = (
    f"# Revision-{EXPECTED_REVISION} accepted matrix rows for docs/sharkbite-compatibility.md.",
    "# One entry per row: ROW-ID followed by the audited status, in document order.",
    "# Pinning the status next to the id means a swap of two rows' statuses inside one",
    "# section cannot pass the aggregate count checks undetected.",
    f"# Update only when the independently audited revision-{EXPECTED_REVISION} inventory itself changes.",
    "# Regenerate from the audited document, review every added/removed/reclassified row",
    "# in code review, and keep the list in document order for human auditability.",
)

C_ABI_EXPORT_HEADER_PATH = Path("capi/include/shoal.h")
C_ABI_REFERENCE_PATHS = (
    Path("capi/tests/lifecycle.c"),
    Path("capi/tests/shared_library_query.c"),
    Path("capi/tests/header_cpp_test.cpp"),
)
DEFAULT_C_ABI_INCLUDE_PATHS = (
    Path("capi/include"),
    Path("capi/tests"),
)
EXPECTED_C_ABI_DECLARED_EXPORTS = 264
EXPECTED_C_ABI_REFERENCED_EXPORTS = 259
EXPECTED_C_ABI_UNREFERENCED_EXPORTS = (
    "shoal_scanner_scan",
    "shoal_batch_scanner_scan",
    "shoal_write_failure_get_constraint",
    "shoal_write_failure_get_authorization",
    "shoal_write_failure_get_cleanup",
)
EXPECTED_C_ABI_SYMBOL_MANIFEST_HEADER = (
    f"# Revision-{EXPECTED_REVISION} compiled C ABI symbol inventory for docs/sharkbite-compatibility.md.",
    "# Generated from the undefined/imported shoal_* references emitted by compiling the exact C/C++",
    "# sources that cmd/shoal-capi/cabi_test.go links in TestSharedLibraryCABI.",
    f"# Update only when the independently audited revision-{EXPECTED_REVISION} compiled inventory changes.",
)

CATEGORY_STATUS_COLUMNS = {
    "Covered": "Covered",
    "Missing Go": "Missing Go",
    "Missing C ABI": "Missing C ABI",
    "Behavior mismatch": "Behavior mismatch",
    "Intentional divergence": INTENTIONAL_DIVERGENCE_STATUS,
    "Not required": NOT_REQUIRED_STATUS,
}

TARGETED_LOCAL_CITATIONS = {
    "capi/include/shoal.h",
    "capi/include/shoal_types.h",
    "capi/tests/lifecycle.c",
    "capi/tests/result_bridge.c",
    "capi/tests/shared_library_query.c",
    "capi/tests/header_cpp_test.cpp",
    "cmd/shoal-capi/export.go",
    "cmd/shoal-capi/abi_export_test.go",
    "cmd/shoal-capi/connector_control_export_test.go",
    "cmd/shoal-capi/table_admin_export_test.go",
    "cmd/shoal-capi/cabi_test.go",
    "cmd/shoal-capi/state_test.go",
    "cmd/shoal-capi/writer_export_test.go",
}
# Implementation files behind the section 6 client-configuration and instance
# topology rows. Their citations are anchor-checked exactly like the C ABI
# ones so a rename or a moved declaration cannot leave the normative evidence
# pointing at the wrong symbol.
TARGETED_SB_CFG_CITATIONS = {
    "accumulo/config.go",
    "accumulo/configuration.go",
    "accumulo/configuration_test.go",
    "accumulo/instance.go",
    "accumulo/topology.go",
    "accumulo/topology_test.go",
    "internal/zk/locator.go",
    "internal/zk/manager.go",
    "internal/zk/topology_test.go",
}

OPTIONAL_ANCHOR_CITATIONS = {
    "capi/include/shoal.h",
    "capi/include/shoal_types.h",
}

# Implementation files behind the section 15 RFile rows. Anchor-checked for the
# same reason: the matrix claims an exact public Go surface, so a rename or a
# deleted method must fail the document, not just the build.
# Implementation files behind the section 11 table-maintenance rows.
TARGETED_SB_KEY_CITATIONS = {
    "accumulo/key.go",
    "accumulo/key_test.go",
}

TARGETED_SB_VIS_CITATIONS = {
    "accumulo/column_visibility.go",
    "accumulo/column_visibility_test.go",
}

TARGETED_SB_SCAN_CITATIONS = {
    "accumulo/scan_stream.go",
    "accumulo/scan_stream_test.go",
}

TARGETED_SB_TABLE_CITATIONS = {
    "accumulo/table_constraints.go",
    "accumulo/table_constraints_test.go",
    "accumulo/table_flush.go",
}

# Implementation files behind the section 8 data-model value types.
TARGETED_SB_DATA_CITATIONS = {
    "accumulo/authorizations.go",
    "accumulo/key_string.go",
    "accumulo/range.go",
    "accumulo/value_types_test.go",
}

TARGETED_SB_RFILE_CITATIONS = {
    "rfile/close_internal_test.go",
    "rfile/entry.go",
    "rfile/multigroup_internal_test.go",
    "rfile/errors.go",
    "rfile/reader.go",
    "rfile/rfile_test.go",
    "rfile/seekable.go",
    "rfile/writer.go",
}

# Every path whose citations are anchor-checked: the C ABI surface plus the
# section 6 and section 15 implementation files.
ANCHOR_CHECKED_CITATIONS = (
    TARGETED_LOCAL_CITATIONS
    | TARGETED_SB_CFG_CITATIONS
    | TARGETED_SB_RFILE_CITATIONS
    | TARGETED_SB_DATA_CITATIONS
    | TARGETED_SB_TABLE_CITATIONS
    | TARGETED_SB_SCAN_CITATIONS
    | TARGETED_SB_VIS_CITATIONS
    | TARGETED_SB_KEY_CITATIONS
)
COUNT_RE = re.compile(
    r"^(?P<bold>\*\*)?(?P<number>0|[1-9]\d*|[1-9]\d{0,2}(?:,\d{3})+)(?(bold)\*\*|)$"
)
CODE_SPAN_RE = re.compile(r"`([^`]+)`")
IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
ANCHOR_PART_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|\.{3}|\s+|.")
FILE_CITATION_SUFFIXES = {
    ".c",
    ".cc",
    ".cpp",
    ".go",
    ".h",
    ".hpp",
    ".java",
    ".json",
    ".md",
    ".proto",
    ".py",
    ".sh",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
}
EXPORT_SYMBOL_RE = re.compile(
    r"SHOAL_API\s+[^;]*?\b(?:SHOAL_CALL\s+)?(?P<name>shoal_[A-Za-z0-9_]+)\s*\(",
    re.S,
)
FREE_SYMBOL_RE = re.compile(
    r"SHOAL_API\s+void\s+SHOAL_CALL\s+(?P<name>shoal_[A-Za-z0-9_]+_free)\s*\(",
    re.S,
)
FILE_CITATION_BASENAMES = {"ARCHITECTURE.md", "CMakeLists.txt", "Dockerfile", "Makefile", "README.md"}
IDENTIFIER_BOUNDARY_CLASS = r"A-Za-z0-9_"
IGNORED_ANCHOR_TOKENS = {
    "ABI",
    "C",
    "Go",
    "and",
    "by",
    "char",
    "const",
    "double",
    "enum",
    "extern",
    "float",
    "full",
    "in",
    "inline",
    "int",
    "interface",
    "long",
    "map",
    "on",
    "plus",
    "read",
    "short",
    "signed",
    "static",
    "struct",
    "typedef",
    "union",
    "under",
    "unsigned",
    "var",
    "void",
    "volatile",
    "with",
}
WHITESPACE_TOLERANT_PUNCTUATION = frozenset("(),*&[]")
DECLARATION_PATTERNS = (
    re.compile(
        r"(?ms)^[ \t]*(?:typedef\s+)?(?:struct|union|enum|class|interface)\b[\s\S]*?"
        r"^[ \t]*\}[ \t]*[A-Za-z_][A-Za-z0-9_]*[ \t]*;"
    ),
    re.compile(
        r"(?ms)^[ \t]*(?:struct|union|enum|class|interface)\s+[A-Za-z_][A-Za-z0-9_]*"
        r"\s*\{[\s\S]*?^[ \t]*\}[ \t]*;?"
    ),
    re.compile(
        r"(?ms)^[ \t]*type\s+[A-Za-z_][A-Za-z0-9_]*\s+(?:struct|interface)\s*\{"
        r"[\s\S]*?^[ \t]*\}[ \t]*"
    ),
    re.compile(
        r"(?ms)^[ \t]*(?:async\s+def|def)\s+[A-Za-z_][A-Za-z0-9_]*\s*\([^)]*?\)"
        r"(?:\s*->\s*[^:]+)?\s*:"
    ),
    re.compile(r"(?m)^[ \t]*class\s+[A-Za-z_][A-Za-z0-9_]*(?:\s*\([^)]*?\))?\s*:"),
    re.compile(
        r"(?ms)^[ \t]*func\s+(?:\([^)]+\)\s*)?[A-Za-z_][A-Za-z0-9_]*\s*\([^)]*?\)"
        r"(?:\s*\([^)]*?\)|\s+[^{\n]+)?\s*\{"
    ),
    re.compile(
        r"(?ms)^[ \t]*(?:[A-Za-z_][A-Za-z0-9_\s\*]*?\s+)?[A-Za-z_][A-Za-z0-9_]*"
        r"\s*\([^;{}]*?\)\s*(?:;|\{)"
    ),
)


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def split_matrix_cells(line: str) -> list[str]:
    return [part.strip() for part in line.split("|")[1:-1]]


def separator_cell_problem(cell: str) -> str | None:
    if not cell:
        return "empty separator cell"
    if not re.fullmatch(r":?-{3,}:?", cell):
        return f"invalid separator cell {cell!r} (expected ---, :---, ---: or :---:)"
    return None


def is_markdown_separator_row(cells: list[str]) -> bool:
    return bool(cells) and all(separator_cell_problem(cell) is None for cell in cells)


def require_matrix_separator(line: str | None, expected_cells: int, header_line_number: int) -> None:
    context = f"the matrix table header on line {header_line_number}"
    cells = split_matrix_cells(line) if line is not None and line.startswith("|") else []
    require(
        any(separator_cell_problem(cell) is None for cell in cells),
        (
            f"missing separator row after {context}"
            + (f": found {line.strip()!r}" if line is not None else "")
        ),
    )
    problems = [
        f"column {index}: {problem}"
        for index, problem in enumerate((separator_cell_problem(cell) for cell in cells), start=1)
        if problem is not None
    ]
    require(
        not problems,
        f"malformed separator row after {context}: {'; '.join(problems)}",
    )
    require(
        len(cells) == expected_cells,
        (
            f"malformed separator row after {context}: expected {expected_cells} cells, "
            f"found {len(cells)}"
        ),
    )


def parse_markdown_table(lines: list[str], heading: str) -> tuple[list[str], list[list[str]]]:
    try:
        heading_index = lines.index(heading)
    except ValueError:
        fail(f"missing heading: {heading}")

    line_index = heading_index + 1
    while line_index < len(lines) and not lines[line_index].startswith("|"):
        line_index += 1
    require(line_index < len(lines), f"missing markdown table after {heading}")

    header_cells = split_matrix_cells(lines[line_index])
    separator_index = line_index + 1
    require(
        separator_index < len(lines) and lines[separator_index].startswith("|"),
        f"missing separator row after header under {heading}",
    )
    separator_cells = split_matrix_cells(lines[separator_index])
    require(
        len(separator_cells) == len(header_cells),
        (
            f"malformed separator row under {heading}: expected {len(header_cells)} cells, "
            f"found {len(separator_cells)}"
        ),
    )
    require(
        is_markdown_separator_row(separator_cells),
        f"malformed separator row under {heading}: {lines[separator_index]}",
    )

    rows: list[list[str]] = []
    line_index = separator_index + 1
    while line_index < len(lines) and lines[line_index].startswith("|"):
        cells = split_matrix_cells(lines[line_index])
        require(
            not is_markdown_separator_row(cells),
            (
                f"unexpected extra separator row under {heading} on line "
                f"{line_index + 1}"
            ),
        )
        require(
            len(cells) == len(header_cells),
            (
                f"malformed table row under {heading}: expected {len(header_cells)} cells, "
                f"found {len(cells)}"
            ),
        )
        rows.append(cells)
        line_index += 1
    return header_cells, rows


def parse_metadata(lines: list[str]) -> dict[str, str]:
    headers, rows = parse_markdown_table(lines, "## 1. Status of this document")
    require(headers == ["Field", "Value"], f"unexpected metadata headers: {headers}")
    metadata: dict[str, str] = {}
    for field, value in rows:
        require(field not in metadata, f"duplicate metadata field: {field}")
        metadata[field] = value
    return metadata


def parse_count(cell: str) -> int:
    value = cell.strip()
    match = COUNT_RE.fullmatch(value)
    require(match is not None, f"unsupported count format: {cell!r}")
    return int(match.group("number").replace(",", ""))


def strip_backticks(cell: str) -> str:
    return cell[1:-1] if cell.startswith("`") and cell.endswith("`") else cell


def parse_rows_metadata(value: str) -> tuple[int, int]:
    match = re.fullmatch(
        r"(?P<total>\d[\d,]*) \((?P<required>\d[\d,]*) required by the \[§2\.2\]\(#sec-2\) release gate\)",
        value,
    )
    require(match is not None, f"malformed Rows metadata: {value}")
    return parse_count(match.group("total")), parse_count(match.group("required"))


def parse_covered_rows_metadata(value: str) -> int:
    match = re.match(r"\*\*(?P<count>\d[\d,]*)\*\*", value)
    require(match is not None, f"malformed Covered rows metadata: {value}")
    return parse_count(match.group("count"))


def parse_status_summary(lines: list[str]) -> tuple[dict[str, int], int]:
    headers, rows = parse_markdown_table(lines, "### 25.1 By status")
    require(headers == ["Status", "Rows"], f"unexpected status-summary headers: {headers}")

    declared_counts: dict[str, int] = {}
    total_rows: int | None = None
    for status, count_cell in rows:
        if status == "**Total**":
            require(total_rows is None, "duplicate total row in status summary table")
            total_rows = parse_count(count_cell)
            continue
        require(status in STATUSES, f"unknown status in summary table: {status}")
        require(status not in declared_counts, f"duplicate status summary row: {status}")
        declared_counts[status] = parse_count(count_cell)

    require(total_rows is not None, "missing total row in status summary table")
    require(
        set(declared_counts) == STATUSES,
        f"status summary does not cover expected statuses: {sorted(declared_counts)}",
    )
    return declared_counts, total_rows


def parse_category_summary(
    lines: list[str],
) -> tuple[dict[str, dict[str, int]], dict[str, int], dict[str, int], int]:
    headers, rows = parse_markdown_table(lines, "### 25.2 By category")
    expected_headers = [
        "Section",
        "Prefix",
        "Rows",
        "Covered",
        "Missing Go",
        "Missing C ABI",
        "Behavior mismatch",
        "Intentional divergence",
        "Not required",
    ]
    require(headers == expected_headers, f"unexpected category-summary headers: {headers}")

    declared_prefix_counts: dict[str, dict[str, int]] = {}
    declared_prefix_totals: dict[str, int] = {}
    declared_total_status_counts: dict[str, int] | None = None
    declared_total_rows: int | None = None

    for row in rows:
        section, prefix_cell, row_total_cell = row[:3]
        status_cells = row[3:]
        row_status_counts = {
            CATEGORY_STATUS_COLUMNS[header]: parse_count(cell)
            for header, cell in zip(headers[3:], status_cells)
        }

        if section == "**Total**":
            require(
                declared_total_rows is None and declared_total_status_counts is None,
                "duplicate total row in category summary table",
            )
            declared_total_rows = parse_count(row_total_cell)
            declared_total_status_counts = row_status_counts
            continue

        prefix = strip_backticks(prefix_cell)
        require(prefix, f"missing prefix in category summary row: {row}")
        require(prefix not in declared_prefix_counts, f"duplicate category summary row: {prefix}")
        declared_prefix_counts[prefix] = row_status_counts
        declared_prefix_totals[prefix] = parse_count(row_total_cell)

    require(declared_total_rows is not None, "missing total row in category summary table")
    require(
        declared_total_status_counts is not None,
        "missing status totals in category summary table",
    )
    return (
        declared_prefix_counts,
        declared_prefix_totals,
        declared_total_status_counts,
        declared_total_rows,
    )


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def row_identifier(cells: list[str], line: str) -> str:
    if cells:
        return cells[0]
    match = re.match(r"\|\s*(SB-[^|`\s]+)", line)
    return match.group(1) if match else "<unknown-row>"


def iter_matrix_rows(lines: list[str]) -> Iterator[tuple[int, str, list[str]]]:
    current_header_cells: int | None = None
    pending_separator: tuple[int, int] | None = None
    for line_number, line in enumerate(lines, start=1):
        if pending_separator is not None:
            header_line_number, header_cell_count = pending_separator
            pending_separator = None
            require_matrix_separator(line, header_cell_count, header_line_number)
            continue
        if not line.startswith("|"):
            current_header_cells = None
            continue
        if line.startswith("| ID |"):
            header_cells = split_matrix_cells(line)
            if "Status" in header_cells:
                current_header_cells = len(header_cells)
                pending_separator = (line_number, len(header_cells))
            else:
                current_header_cells = None
            continue
        if current_header_cells is not None and is_markdown_separator_row(split_matrix_cells(line)):
            fail(f"unexpected separator row inside a matrix table on line {line_number}")
        if not line.startswith("| SB-"):
            continue
        cells = split_matrix_cells(line)
        row_id = row_identifier(cells, line)
        if current_header_cells is None or row_id.startswith("SB-GAP"):
            continue
        expected_cells = current_header_cells or len(cells)
        require(
            len(cells) == expected_cells,
            (
                f"malformed SB row {row_id} on line {line_number}: expected "
                f"{expected_cells} cells, found {len(cells)}"
            ),
        )
        yield line_number, row_id, cells
    if pending_separator is not None:
        require_matrix_separator(None, pending_separator[1], pending_separator[0])


def parse_rows(
    lines: list[str],
) -> tuple[Counter[str], dict[str, Counter[str]], tuple[tuple[str, str], ...]]:
    status_counts: Counter[str] = Counter()
    prefix_counts: dict[str, Counter[str]] = defaultdict(Counter)
    accepted_row_ids: dict[str, tuple[int, str]] = {}
    row_sequence: list[tuple[str, str]] = []
    for line_number, row_id, cells in iter_matrix_rows(lines):
        status = cells[-2]
        require(
            status in STATUSES,
            f"unknown status for row {row_id} on line {line_number}: {status}",
        )
        previous = accepted_row_ids.get(row_id)
        if previous is not None:
            fail(
                f"duplicate accepted row id {row_id} on lines {previous[0]} and {line_number} "
                f"({previous[1]} vs {status})"
            )
        accepted_row_ids[row_id] = (line_number, status)
        row_sequence.append((row_id, status))
        prefix = "-".join(row_id.split("-")[:2])
        status_counts[status] += 1
        prefix_counts[prefix][status] += 1
    return status_counts, prefix_counts, tuple(row_sequence)


def parse_row_manifest_lines(
    lines: Sequence[str], *, source: str
) -> tuple[tuple[str, str], ...]:
    """Parse a manifest of audited rows: one `ROW-ID STATUS` entry per line."""
    entries: list[tuple[str, str]] = []
    seen: set[str] = set()
    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"(?P<row_id>SB-[A-Z0-9-]+)[ \t]+(?P<status>\S.*)", line)
        require(
            match is not None,
            (
                f"invalid row manifest entry in {source} on line {line_number}: {raw_line!r} "
                "(expected 'ROW-ID STATUS')"
            ),
        )
        assert match is not None
        row_id = match.group("row_id")
        status = match.group("status").strip()
        require(
            status in STATUSES,
            (
                f"invalid status in row manifest entry in {source} on line {line_number}: "
                f"{status!r}"
            ),
        )
        require(row_id not in seen, f"duplicate row manifest entry in {source}: {row_id}")
        seen.add(row_id)
        entries.append((row_id, status))
    return tuple(entries)


def validate_expected_row_manifest_provenance(lines: Sequence[str], *, source: str) -> None:
    filename_match = re.fullmatch(
        r"sharkbite-compatibility-revision(?P<revision>\d+)-rows\.txt", source
    )
    require(
        filename_match is not None,
        f"unexpected row manifest filename {source!r}",
    )
    assert filename_match is not None
    require(
        int(filename_match.group("revision")) == EXPECTED_REVISION,
        (
            f"row manifest filename {source!r} does not match EXPECTED_REVISION "
            f"{EXPECTED_REVISION}"
        ),
    )
    comment_lines = tuple(raw_line.rstrip() for raw_line in lines if raw_line.startswith("#"))
    require(
        comment_lines[: len(EXPECTED_ROW_MANIFEST_HEADER)] == EXPECTED_ROW_MANIFEST_HEADER,
        (
            f"row manifest header in {source} does not match revision {EXPECTED_REVISION}: "
            f"expected {EXPECTED_ROW_MANIFEST_HEADER[0]!r}"
        ),
    )


@lru_cache(maxsize=1)
def load_expected_rows() -> tuple[tuple[str, str], ...]:
    manifest_lines = EXPECTED_ROW_MANIFEST.read_text(encoding="utf-8").splitlines()
    validate_expected_row_manifest_provenance(
        manifest_lines, source=str(EXPECTED_ROW_MANIFEST.name)
    )
    rows = parse_row_manifest_lines(
        manifest_lines,
        source=str(EXPECTED_ROW_MANIFEST.name),
    )
    require(
        len(rows) == EXPECTED_TOTAL_ROWS,
        (
            f"revision {EXPECTED_REVISION} row manifest expects {EXPECTED_TOTAL_ROWS} entries, "
            f"found {len(rows)}"
        ),
    )
    manifest_status_counts = Counter(status for _row_id, status in rows)
    for status, expected in EXPECTED_STATUS_COUNTS.items():
        require(
            manifest_status_counts[status] == expected,
            (
                f"revision {EXPECTED_REVISION} row manifest pins {manifest_status_counts[status]} "
                f"rows for {status}, but EXPECTED_STATUS_COUNTS pins {expected}"
            ),
        )
    return rows


def format_row_entry(entry: tuple[str, str] | str) -> str:
    if isinstance(entry, tuple):
        row_id, status = entry
        return f"{row_id} ({status})"
    return entry


def preview_row_ids(row_ids: Sequence[tuple[str, str] | str], *, limit: int = 5) -> str:
    preview = [format_row_entry(entry) for entry in row_ids[:limit]]
    suffix = " ..." if len(row_ids) > limit else ""
    return (", ".join(preview) + suffix) if preview else "none"


def validate_pinned_inventory_constants() -> None:
    require(
        set(EXPECTED_STATUS_COUNTS) == STATUSES,
        f"EXPECTED_STATUS_COUNTS does not cover the six statuses: {sorted(EXPECTED_STATUS_COUNTS)}",
    )
    pinned_status_total = sum(EXPECTED_STATUS_COUNTS.values())
    require(
        pinned_status_total == EXPECTED_TOTAL_ROWS,
        (
            f"pinned per-status counts sum to {pinned_status_total}, but EXPECTED_TOTAL_ROWS is "
            f"{EXPECTED_TOTAL_ROWS}"
        ),
    )
    pinned_required = EXPECTED_TOTAL_ROWS - EXPECTED_STATUS_COUNTS[NOT_REQUIRED_STATUS]
    require(
        pinned_required == EXPECTED_REQUIRED_ROWS,
        (
            f"pinned required rows should be {pinned_required}, but EXPECTED_REQUIRED_ROWS is "
            f"{EXPECTED_REQUIRED_ROWS}"
        ),
    )
    require(
        set(EXPECTED_PREFIX_TOTALS) == set(EXPECTED_PREFIX_COUNTS),
        (
            "pinned section totals and pinned section status counts describe different sections: "
            f"{sorted(EXPECTED_PREFIX_TOTALS)} vs {sorted(EXPECTED_PREFIX_COUNTS)}"
        ),
    )
    pinned_prefix_total = sum(EXPECTED_PREFIX_TOTALS.values())
    require(
        pinned_prefix_total == EXPECTED_TOTAL_ROWS,
        (
            f"pinned section totals sum to {pinned_prefix_total}, but EXPECTED_TOTAL_ROWS is "
            f"{EXPECTED_TOTAL_ROWS}"
        ),
    )
    for prefix, expected_counts in EXPECTED_PREFIX_COUNTS.items():
        pinned_section_total = sum(expected_counts.values())
        require(
            pinned_section_total == EXPECTED_PREFIX_TOTALS[prefix],
            (
                f"pinned status counts for {prefix} sum to {pinned_section_total}, but "
                f"EXPECTED_PREFIX_TOTALS pins {EXPECTED_PREFIX_TOTALS[prefix]}"
            ),
        )
    for status in STATUSES:
        pinned_by_section = sum(counts[status] for counts in EXPECTED_PREFIX_COUNTS.values())
        require(
            pinned_by_section == EXPECTED_STATUS_COUNTS[status],
            (
                f"pinned section counts sum to {pinned_by_section} rows for {status}, but "
                f"EXPECTED_STATUS_COUNTS pins {EXPECTED_STATUS_COUNTS[status]}"
            ),
        )
    require(
        len(EXPECTED_C_ABI_UNREFERENCED_EXPORTS)
        == EXPECTED_C_ABI_DECLARED_EXPORTS - EXPECTED_C_ABI_REFERENCED_EXPORTS,
        (
            "pinned C ABI symbol counts drifted: "
            f"{len(EXPECTED_C_ABI_UNREFERENCED_EXPORTS)} missing symbols vs "
            f"{EXPECTED_C_ABI_DECLARED_EXPORTS} declared and {EXPECTED_C_ABI_REFERENCED_EXPORTS} referenced"
        ),
    )


def parse_command_text(command_text: str) -> list[str]:
    return shlex.split(command_text, posix=os.name != "nt")


@lru_cache(maxsize=None)
def go_env(name: str) -> str:
    output = subprocess.run(
        ["go", "env", name],
        check=True,
        capture_output=True,
        text=True,
    )
    return output.stdout.strip()


def compiler_command(name: str) -> list[str] | None:
    command_text = go_env(name)
    if not command_text:
        return None
    fields = parse_command_text(command_text)
    return fields if fields else None


def symbol_tool_command() -> list[str] | None:
    for candidate in ("nm", "llvm-nm"):
        path = shutil.which(candidate)
        if path is not None:
            return [path]
    cc = compiler_command("CC")
    if cc:
        compiler_name = Path(cc[0]).name.lower()
        for suffix in ("gcc", "cc", "clang"):
            if compiler_name.endswith(suffix):
                stem = Path(cc[0]).name[: -len(suffix)]
                candidate = stem + "nm"
                path = shutil.which(candidate)
                if path is not None:
                    return [path]
    return None


def load_c_abi_symbol_manifest_lines(path: Path | None = None) -> list[str]:
    manifest_path = EXPECTED_C_ABI_SYMBOL_MANIFEST if path is None else path
    return manifest_path.read_text(encoding="utf-8").splitlines()


def parse_named_symbol_sections(
    lines: Sequence[str], *, source: str
) -> dict[str, tuple[str, ...]]:
    sections: dict[str, list[str]] = {}
    current_section: str | None = None
    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        section_match = re.fullmatch(r"\[(?P<section>[a-z_]+)\]", line)
        if section_match is not None:
            current_section = section_match.group("section")
            require(
                current_section not in sections,
                f"duplicate symbol section [{current_section}] in {source}",
            )
            sections[current_section] = []
            continue
        require(
            current_section is not None,
            f"unexpected symbol entry before a section header in {source} on line {line_number}: {raw_line!r}",
        )
        require(
            re.fullmatch(r"shoal_[A-Za-z0-9_]+", line) is not None,
            f"invalid symbol entry in {source} on line {line_number}: {raw_line!r}",
        )
        sections[current_section].append(line)
    return {section: tuple(entries) for section, entries in sections.items()}


def validate_expected_c_abi_symbol_manifest_provenance(
    lines: Sequence[str], *, source: str
) -> None:
    filename_match = re.fullmatch(
        r"sharkbite-compatibility-revision(?P<revision>\d+)-cabi-symbols\.txt",
        source,
    )
    require(filename_match is not None, f"unexpected C ABI symbol manifest filename {source!r}")
    assert filename_match is not None
    require(
        int(filename_match.group("revision")) == EXPECTED_REVISION,
        (
            f"C ABI symbol manifest filename {source!r} does not match EXPECTED_REVISION "
            f"{EXPECTED_REVISION}"
        ),
    )
    comment_lines = tuple(raw_line.rstrip() for raw_line in lines if raw_line.startswith("#"))
    require(
        comment_lines[: len(EXPECTED_C_ABI_SYMBOL_MANIFEST_HEADER)]
        == EXPECTED_C_ABI_SYMBOL_MANIFEST_HEADER,
        (
            f"C ABI symbol manifest header in {source} does not match revision "
            f"{EXPECTED_REVISION}: expected {EXPECTED_C_ABI_SYMBOL_MANIFEST_HEADER[0]!r}"
        ),
    )


@lru_cache(maxsize=1)
def load_expected_c_abi_symbol_manifest() -> dict[str, tuple[str, ...]]:
    lines = load_c_abi_symbol_manifest_lines()
    validate_expected_c_abi_symbol_manifest_provenance(
        lines, source=EXPECTED_C_ABI_SYMBOL_MANIFEST.name
    )
    sections = parse_named_symbol_sections(lines, source=EXPECTED_C_ABI_SYMBOL_MANIFEST.name)
    require(
        set(sections) == {"declared", "referenced", "unreferenced"},
        (
            "C ABI symbol manifest sections do not match the audited inventory: "
            f"{sorted(sections)}"
        ),
    )
    declared = sections["declared"]
    referenced = sections["referenced"]
    unreferenced = sections["unreferenced"]
    require(
        len(declared) == EXPECTED_C_ABI_DECLARED_EXPORTS,
        (
            f"C ABI symbol manifest pins {len(declared)} declared exports, but "
            f"EXPECTED_C_ABI_DECLARED_EXPORTS pins {EXPECTED_C_ABI_DECLARED_EXPORTS}"
        ),
    )
    require(
        len(referenced) == EXPECTED_C_ABI_REFERENCED_EXPORTS,
        (
            f"C ABI symbol manifest pins {len(referenced)} referenced exports, but "
            f"EXPECTED_C_ABI_REFERENCED_EXPORTS pins {EXPECTED_C_ABI_REFERENCED_EXPORTS}"
        ),
    )
    require(
        unreferenced == EXPECTED_C_ABI_UNREFERENCED_EXPORTS,
        (
            "C ABI symbol manifest pins a different unreferenced export inventory: "
            f"{unreferenced}"
        ),
    )
    return sections


def collect_c_abi_declared_exports(repo_root: Path | None = None) -> tuple[str, ...]:
    root = repo_root or DOC_PATH.parent.parent
    exports = tuple(
        match.group("name")
        for match in EXPORT_SYMBOL_RE.finditer(
            (root / C_ABI_EXPORT_HEADER_PATH).read_text(encoding="utf-8")
        )
    )
    require(exports, f"no SHOAL_API exports found in {C_ABI_EXPORT_HEADER_PATH}")
    require(
        len(exports) == len(set(exports)),
        f"duplicate SHOAL_API export declarations found in {C_ABI_EXPORT_HEADER_PATH}",
    )
    return exports


def compiler_for_source(source_path: Path) -> list[str] | None:
    suffix = source_path.suffix.lower()
    if suffix in {".cpp", ".cc", ".cxx"}:
        return compiler_command("CXX")
    return compiler_command("CC")


def compile_source_to_object(
    source_path: Path,
    object_path: Path,
    *,
    include_paths: Sequence[Path],
    repo_root: Path,
) -> None:
    compiler = compiler_for_source(source_path)
    require(compiler is not None, f"no compiler configured for {source_path}")
    args = list(compiler)
    suffix = source_path.suffix.lower()
    if suffix in {".cpp", ".cc", ".cxx"}:
        args.extend(["-std=c++11", "-Wall", "-Wextra", "-Werror"])
    else:
        args.extend(["-std=c11", "-Wall", "-Wextra", "-Werror"])
    for include_path in include_paths:
        args.extend(["-I", str(repo_root / include_path)])
    args.extend(["-c", str(repo_root / source_path), "-o", str(object_path)])
    subprocess.run(args, cwd=repo_root, check=True, capture_output=True, text=True)


def extract_undefined_shoal_symbols(
    object_path: Path, *, symbol_tool: Sequence[str]
) -> tuple[str, ...]:
    output = subprocess.run(
        [*symbol_tool, "-u", str(object_path)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    symbols: set[str] = set()
    for raw_line in output.splitlines():
        for token in re.findall(r"(?:__imp__?|_)?shoal_[A-Za-z0-9_]+", raw_line):
            normalized = token
            while normalized.startswith("_"):
                normalized = normalized[1:]
            if normalized.startswith("imp_"):
                normalized = normalized[len("imp_") :]
            if normalized.startswith("_"):
                normalized = normalized[1:]
            if normalized.startswith("shoal_"):
                symbols.add(normalized)
    return tuple(sorted(symbols))


def compiled_c_abi_reference_inventory(
    source_paths: Sequence[Path] = C_ABI_REFERENCE_PATHS,
    *,
    include_paths: Sequence[Path] = DEFAULT_C_ABI_INCLUDE_PATHS,
    repo_root: Path | None = None,
) -> tuple[str, ...] | None:
    root = repo_root or DOC_PATH.parent.parent
    symbol_tool = symbol_tool_command()
    if symbol_tool is None:
        return None
    if any(compiler_for_source(path) is None for path in source_paths):
        return None
    with tempfile.TemporaryDirectory(dir=root) as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        collected: set[str] = set()
        for index, source_path in enumerate(source_paths):
            object_path = temp_dir / f"symbol_inventory_{index}{source_path.suffix}.o"
            compile_source_to_object(
                source_path,
                object_path,
                include_paths=include_paths,
                repo_root=root,
            )
            collected.update(
                extract_undefined_shoal_symbols(object_path, symbol_tool=symbol_tool)
            )
    return tuple(sorted(collected))


def collect_c_abi_symbol_inventory(
    repo_root: Path | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    exports = collect_c_abi_declared_exports(repo_root)
    manifest = load_expected_c_abi_symbol_manifest()
    compiled_references = compiled_c_abi_reference_inventory(repo_root=repo_root)
    if compiled_references is None:
        referenced = manifest["referenced"]
    else:
        export_set = set(exports)
        referenced = tuple(symbol for symbol in compiled_references if symbol in export_set)
    unreferenced = tuple(symbol for symbol in exports if symbol not in referenced)
    return exports, referenced, unreferenced


def collect_c_abi_free_function_inventory(repo_root: Path | None = None) -> tuple[str, ...]:
    root = repo_root or DOC_PATH.parent.parent
    header_text = (root / C_ABI_EXPORT_HEADER_PATH).read_text(encoding="utf-8")
    free_functions = tuple(match.group("name") for match in FREE_SYMBOL_RE.finditer(header_text))
    require(free_functions, f"no free functions found in {C_ABI_EXPORT_HEADER_PATH}")
    require(
        len(free_functions) == len(set(free_functions)),
        f"duplicate free function declarations found in {C_ABI_EXPORT_HEADER_PATH}",
    )
    return free_functions


def format_named_symbol_manifest(
    sections: dict[str, Sequence[str]], *, header: Sequence[str]
) -> str:
    lines = [*header, ""]
    for section_name in ("declared", "referenced", "unreferenced"):
        lines.append(f"[{section_name}]")
        lines.extend(sections[section_name])
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_c_abi_symbol_manifest(repo_root: Path | None = None) -> str:
    exports = collect_c_abi_declared_exports(repo_root)
    compiled_references = compiled_c_abi_reference_inventory(repo_root=repo_root)
    require(
        compiled_references is not None,
        "standard C/C++ toolchain with nm/llvm-nm is required to regenerate the C ABI symbol manifest",
    )
    export_set = set(exports)
    referenced = tuple(symbol for symbol in compiled_references if symbol in export_set)
    unreferenced = tuple(symbol for symbol in exports if symbol not in referenced)
    manifest_text = format_named_symbol_manifest(
        {
            "declared": exports,
            "referenced": referenced,
            "unreferenced": unreferenced,
        },
        header=EXPECTED_C_ABI_SYMBOL_MANIFEST_HEADER,
    )
    EXPECTED_C_ABI_SYMBOL_MANIFEST.write_text(manifest_text, encoding="utf-8")
    load_expected_c_abi_symbol_manifest.cache_clear()
    return manifest_text


def validate_c_abi_symbol_inventory(full_text: str, repo_root: Path | None = None) -> None:
    exports, referenced, unreferenced = collect_c_abi_symbol_inventory(repo_root)
    manifest = load_expected_c_abi_symbol_manifest()
    require(
        len(exports) == EXPECTED_C_ABI_DECLARED_EXPORTS,
        (
            f"expected {EXPECTED_C_ABI_DECLARED_EXPORTS} SHOAL_API exports in "
            f"{C_ABI_EXPORT_HEADER_PATH}, found {len(exports)}"
        ),
    )
    require(
        len(referenced) == EXPECTED_C_ABI_REFERENCED_EXPORTS,
        (
            f"expected {EXPECTED_C_ABI_REFERENCED_EXPORTS} C/C++ test-referenced exports, "
            f"found {len(referenced)}"
        ),
    )
    require(
        tuple(exports) == manifest["declared"],
        (
            "stale declared C ABI export manifest inventory: "
            f"expected {manifest['declared'][:3]}..., found {tuple(exports)[:3]}..."
        ),
    )
    require(
        tuple(referenced) == manifest["referenced"],
        "stale compiled C ABI referenced-export manifest inventory",
    )
    require(
        tuple(unreferenced) == manifest["unreferenced"],
        "stale compiled C ABI unreferenced-export manifest inventory",
    )
    require(
        unreferenced == EXPECTED_C_ABI_UNREFERENCED_EXPORTS,
        (
            "stale unreferenced C ABI export inventory: "
            f"expected {EXPECTED_C_ABI_UNREFERENCED_EXPORTS}, found {unreferenced}"
        ),
    )
    normalized = normalize_whitespace(full_text)
    require(
        (
            f"applied to {len(exports)} declared exports in `{C_ABI_EXPORT_HEADER_PATH.as_posix()}`"
            in normalized
        ),
        "missing or stale C ABI export-total narrative for SB-XCUT-013",
    )
    require(
        f"reference **{len(referenced)} of the {len(exports)}** exports" in normalized,
        "missing or stale C ABI export-reference narrative for SB-XCUT-013",
    )
    missing_list = ", ".join(f"`{symbol}`" for symbol in unreferenced)
    require(
        missing_list in normalized,
        "missing or stale C ABI unreferenced-export list for SB-XCUT-013",
    )


def validate_c_abi_free_inventory(full_text: str, repo_root: Path | None = None) -> None:
    free_functions = collect_c_abi_free_function_inventory(repo_root)
    exports, referenced, _unreferenced = collect_c_abi_symbol_inventory(repo_root)
    require(
        all(symbol in exports for symbol in free_functions),
        "C ABI free-function inventory contains symbols not declared in shoal.h",
    )
    missing_references = [symbol for symbol in free_functions if symbol not in referenced]
    require(
        not missing_references,
        (
            "typed C ABI free functions are missing from the compiled reference inventory: "
            f"{', '.join(missing_references)}"
        ),
    )
    normalized = normalize_whitespace(full_text)
    free_list = ", ".join(f"`{symbol}`" for symbol in free_functions)
    require(
        f"{len(free_functions)} typed free functions — {free_list}" in normalized,
        "missing or stale typed free-function inventory for SB-XCUT-002",
    )


def moved_row_diagnostics(
    current_rows: Sequence[tuple[str, str]],
    expected_rows: Sequence[tuple[str, str]],
    *,
    limit: int = 5,
) -> list[str]:
    expected_positions = {row_id: index + 1 for index, (row_id, _status) in enumerate(expected_rows)}
    current_positions = {row_id: index + 1 for index, (row_id, _status) in enumerate(current_rows)}
    moved: list[str] = []
    for row_id, _status in expected_rows:
        expected_position = expected_positions[row_id]
        current_position = current_positions.get(row_id)
        if current_position is None or current_position == expected_position:
            continue
        moved.append(f"{row_id} expected {expected_position} found {current_position}")
        if len(moved) >= limit:
            break
    return moved


def reclassified_row_diagnostics(
    current_rows: Sequence[tuple[str, str]],
    expected_rows: Sequence[tuple[str, str]],
    *,
    limit: int = 5,
) -> list[str]:
    current_statuses = dict(current_rows)
    reclassified: list[str] = []
    for row_id, expected_status in expected_rows:
        current_status = current_statuses.get(row_id)
        if current_status is None or current_status == expected_status:
            continue
        reclassified.append(f"{row_id} pinned {expected_status} found {current_status}")
        if len(reclassified) >= limit:
            break
    return reclassified


def validate_expected_row_sequence(
    current_rows: Sequence[tuple[str, str]],
    expected_rows: Sequence[tuple[str, str]],
) -> None:
    current_row_ids = {row_id for row_id, _status in current_rows}
    expected_row_ids = {row_id for row_id, _status in expected_rows}
    missing_rows = [entry for entry in expected_rows if entry[0] not in current_row_ids]
    unexpected_rows = [entry for entry in current_rows if entry[0] not in expected_row_ids]
    moved_rows = moved_row_diagnostics(current_rows, expected_rows)
    reclassified_rows = reclassified_row_diagnostics(current_rows, expected_rows)
    require(
        tuple(current_rows) == tuple(expected_rows),
        (
            f"revision {EXPECTED_REVISION} inventory rows changed: "
            f"missing [{preview_row_ids(missing_rows)}]; "
            f"unexpected [{preview_row_ids(unexpected_rows)}]; "
            f"moved [{preview_row_ids(moved_rows)}]; "
            f"reclassified [{preview_row_ids(reclassified_rows)}]; "
            f"{INVENTORY_CHANGE_HINT}"
        ),
    )


def validate_revision_inventory(
    rows: Sequence[tuple[str, str]],
    status_counts: Counter[str],
    prefix_counts: dict[str, Counter[str]],
) -> None:
    validate_pinned_inventory_constants()
    expected_rows = load_expected_rows()
    validate_expected_row_sequence(rows, expected_rows)

    total_rows = sum(status_counts.values())
    require(
        total_rows == EXPECTED_TOTAL_ROWS,
        (
            f"revision {EXPECTED_REVISION} inventory expects {EXPECTED_TOTAL_ROWS} rows, found "
            f"{total_rows}; {INVENTORY_CHANGE_HINT}"
        ),
    )
    required_rows = total_rows - status_counts[NOT_REQUIRED_STATUS]
    require(
        required_rows == EXPECTED_REQUIRED_ROWS,
        (
            f"revision {EXPECTED_REVISION} inventory expects {EXPECTED_REQUIRED_ROWS} required "
            f"rows, found {required_rows}; {INVENTORY_CHANGE_HINT}"
        ),
    )

    for status, expected in EXPECTED_STATUS_COUNTS.items():
        actual = status_counts[status]
        require(
            actual == expected,
            (
                f"revision {EXPECTED_REVISION} inventory expects {expected} rows for {status}, "
                f"found {actual}; {INVENTORY_CHANGE_HINT}"
            ),
        )

    require(
        set(prefix_counts) == set(EXPECTED_PREFIX_COUNTS),
        (
            f"revision {EXPECTED_REVISION} inventory prefixes do not match: "
            f"{sorted(prefix_counts)} vs {sorted(EXPECTED_PREFIX_COUNTS)}; {INVENTORY_CHANGE_HINT}"
        ),
    )
    for prefix, expected_counts in EXPECTED_PREFIX_COUNTS.items():
        actual_total = sum(prefix_counts[prefix].values())
        require(
            actual_total == EXPECTED_PREFIX_TOTALS[prefix],
            (
                f"revision {EXPECTED_REVISION} inventory expects "
                f"{EXPECTED_PREFIX_TOTALS[prefix]} rows for {prefix}, found {actual_total}; "
                f"{INVENTORY_CHANGE_HINT}"
            ),
        )
        for status, expected in expected_counts.items():
            actual = prefix_counts[prefix][status]
            require(
                actual == expected,
                (
                    f"revision {EXPECTED_REVISION} inventory expects {expected} rows for "
                    f"{prefix} / {status}, found {actual}; {INVENTORY_CHANGE_HINT}"
                ),
            )


def parse_gap_completion_tables(lines: list[str]) -> dict[str, tuple[str, str]]:
    gap_rows: dict[str, tuple[str, str]] = {}
    for heading in (
        "### 23.1 Stage 1 — Go parity (blocks everything)",
        "### 23.2 Stage 2 — C ABI parity (blocked by Stage 1 per row)",
    ):
        headers, rows = parse_markdown_table(lines, heading)
        require(
            headers == ["ID", "Gap", "Matrix rows", "Existing issue/PR", "Notes"],
            f"unexpected headers under {heading}: {headers}",
        )
        for row in rows:
            row_id, _gap, matrix_rows, _existing, notes = row
            if row_id.startswith("SB-GAP-"):
                require(
                    row_id not in gap_rows,
                    f"duplicate audited gap row {row_id}",
                )
                gap_rows[row_id] = (matrix_rows, notes)
    return gap_rows


def expand_gap_row_references(
    matrix_rows_cell: str, row_statuses: dict[str, str], *, gap_id: str
) -> tuple[str, ...]:
    expanded: list[str] = []
    seen: set[str] = set()
    for token in matrix_rows_cell.split(","):
        entry = token.strip()
        if not entry:
            continue
        if entry.endswith("-*"):
            prefix = entry[:-1]
            matches = sorted(row_id for row_id in row_statuses if row_id.startswith(prefix))
            require(matches, f"{gap_id} references no audited rows for wildcard {entry}")
            candidates = matches
        elif "…" in entry:
            start, end = [part.strip() for part in entry.split("…", 1)]
            require(start and end, f"{gap_id} contains an empty range boundary in {entry!r}")
            require("-" in start and "-" in end, f"{gap_id} contains a malformed range {entry}")
            start_prefix, start_number = start.rsplit("-", 1)
            end_prefix, end_number = end.rsplit("-", 1)
            require(
                start_prefix == end_prefix,
                f"{gap_id} mixes prefixes in range {entry}",
            )
            require(
                start_number.isdigit() and end_number.isdigit(),
                f"{gap_id} contains a non-numeric range {entry}",
            )
            require(
                int(start_number) <= int(end_number),
                f"{gap_id} contains a descending range {entry}",
            )
            width = max(len(start_number), len(end_number))
            candidates = [
                f"{start_prefix}-{value:0{width}d}"
                for value in range(int(start_number), int(end_number) + 1)
            ]
        else:
            candidates = [entry]
        for row_id in candidates:
            require(row_id in row_statuses, f"{gap_id} references unknown row {row_id}")
            if row_id not in seen:
                seen.add(row_id)
                expanded.append(row_id)
    require(expanded, f"{gap_id} claims completion without referencing any matrix rows")
    return tuple(expanded)


def validate_gap_completion_consistency(
    lines: list[str],
    rows: Sequence[tuple[str, str]],
    *,
    rules: dict[str, tuple[str, ...]] | None = None,
) -> None:
    active_rules = GAP_COMPLETION_RULES if rules is None else rules
    gap_rows = parse_gap_completion_tables(lines)
    row_statuses = dict(rows)
    for gap_id, forbidden_statuses in active_rules.items():
        require(gap_id in gap_rows, f"missing audited gap row {gap_id}")
        matrix_rows_cell, _notes = gap_rows[gap_id]
        referenced_rows = expand_gap_row_references(
            matrix_rows_cell, row_statuses, gap_id=gap_id
        )
        require(
            referenced_rows,
            f"{gap_id} claims completion without referencing any matrix rows",
        )
        contradicting = [
            (row_id, row_statuses[row_id])
            for row_id in referenced_rows
            if row_statuses[row_id] in forbidden_statuses
        ]
        require(
            not contradicting,
            (
                f"{gap_id} claims completion, but referenced rows remain one of "
                f"{', '.join(forbidden_statuses)}: "
                f"{preview_row_ids(contradicting)}"
            ),
        )


def validate_counts(lines: list[str], full_text: str) -> None:
    metadata = parse_metadata(lines)
    for field, expected_value in EXPECTED_METADATA_FIELDS.items():
        actual = metadata.get(field)
        require(actual == expected_value, f"unexpected metadata value for {field}: {actual!r}")
    document_status = metadata.get("Document status", "")
    for snippet in EXPECTED_DOCUMENT_STATUS_SNIPPETS:
        require(
            snippet in document_status,
            f"document status is missing expected detail: {snippet}",
        )

    status_counts, prefix_counts, rows = parse_rows(lines)
    total_rows = len(rows)
    require(sum(status_counts.values()) == total_rows, f"expected {total_rows} rows, found {sum(status_counts.values())}")
    validate_revision_inventory(rows, status_counts, prefix_counts)
    validate_gap_completion_consistency(lines, rows)

    metadata_total_rows, metadata_required_rows = parse_rows_metadata(metadata.get("Rows", ""))
    require(
        metadata_total_rows == total_rows,
        f"Rows metadata says {metadata_total_rows}, but parsed {total_rows} unique rows",
    )
    covered_rows = parse_covered_rows_metadata(metadata.get("Covered rows", ""))
    require(
        covered_rows == status_counts["Covered"],
        f"Covered rows metadata says {covered_rows}, but parsed {status_counts['Covered']}",
    )
    required_rows = total_rows - status_counts[NOT_REQUIRED_STATUS]
    require(
        metadata_required_rows == required_rows,
        f"Rows metadata says {metadata_required_rows} required rows, but parsed {required_rows}",
    )

    declared_status_counts, declared_status_total = parse_status_summary(lines)
    require(
        declared_status_total == total_rows,
        f"status summary total says {declared_status_total}, but parsed {total_rows}",
    )
    for status in sorted(STATUSES):
        actual = status_counts[status]
        declared = declared_status_counts[status]
        require(
            declared == actual,
            f"status summary says {declared} rows for {status}, but parsed {actual}",
        )

    (
        declared_prefix_counts,
        declared_prefix_totals,
        declared_category_status_totals,
        declared_category_total,
    ) = parse_category_summary(lines)
    require(
        declared_category_total == total_rows,
        f"category summary total says {declared_category_total}, but parsed {total_rows}",
    )
    require(
        set(declared_prefix_counts) == set(prefix_counts),
        (
            "category summary prefixes do not match parsed prefixes: "
            f"{sorted(declared_prefix_counts)} vs {sorted(prefix_counts)}"
        ),
    )
    for prefix in sorted(prefix_counts):
        parsed_total = sum(prefix_counts[prefix].values())
        require(
            declared_prefix_totals[prefix] == parsed_total,
            f"category summary says {declared_prefix_totals[prefix]} rows for {prefix}, but parsed {parsed_total}",
        )
        for status in sorted(STATUSES):
            actual = prefix_counts[prefix][status]
            declared = declared_prefix_counts[prefix][status]
            require(
                declared == actual,
                f"category summary says {declared} rows for {prefix} / {status}, but parsed {actual}",
            )
    for status in sorted(STATUSES):
        declared = declared_category_status_totals[status]
        actual = status_counts[status]
        require(
            declared == actual,
            f"category summary total says {declared} rows for {status}, but parsed {actual}",
        )

    validate_status_narratives(full_text, status_counts, prefix_counts)
    validate_c_abi_symbol_inventory(full_text)
    validate_c_abi_free_inventory(full_text)


def validate_status_narratives(
    full_text: str,
    status_counts: Counter[str],
    prefix_counts: dict[str, Counter[str]],
) -> None:
    normalized = normalize_whitespace(full_text)
    total_rows = sum(status_counts.values())
    required_rows = total_rows - status_counts[NOT_REQUIRED_STATUS]
    python_visible_behavior = status_counts["Behavior mismatch"] - prefix_counts["SB-CXX"]["Behavior mismatch"]

    expected_phrases = [
        f"As of revision {EXPECTED_REVISION} that is {required_rows} of {total_rows} rows, and **only {status_counts['Covered']} are satisfied** ([SB-XCUT-012](#sec-20), the twelve configuration/topology rows in [§6](#sec-6), the 31 RFile/stream rows in [§15](#sec-15), the 17 data-model value rows in [§8](#sec-8) and [§19.2](#sec-19-2), the four buffered-writer rows in [§10](#sec-10), the four row-bounded flush/constraint rows in [§11](#sec-11) and [§19.2](#sec-19-2), the connector invalidation/cancellation rows in [§7](#sec-7) and [§20](#sec-20), the eight high-level client rows in [§10.1](#sec-10-1), the five high-level scanner rows in [§10.1](#sec-10-1), the four compatibility-error rows in [§18](#sec-18), the twelve streaming cursor rows in [§9](#sec-9), [§10.1](#sec-10-1), [§19.2](#sec-19-2), and [§20](#sec-20), and the 31 column-visibility rows in [§18](#sec-18) and [§19.2](#sec-19-2))",
        f"{required_rows} rows are **required** by the final release gate ([§2.2](#sec-2)); the {status_counts[NOT_REQUIRED_STATUS]} `Not required` rows are excluded by construction, and {prefix_counts['SB-CXX'][NOT_REQUIRED_STATUS]} of those are the evidence-proved duplicates described in [§19.1](#sec-19-1).",
        "**Exactly 131 rows are `Covered`: [SB-XCUT-012](#sec-20), the twelve configuration/topology rows completed in revision 24, the 31 RFile/stream rows completed in revision 25, the 17 data-model value rows completed in revision 26, the four buffered-writer rows completed in revision 28, the four row-bounded flush/constraint rows completed in revision 29, the connector invalidation/cancellation rows completed in revision 30, the eight high-level client rows completed in revision 31, the five high-level scanner rows completed in revision 32, the four compatibility-error rows completed in revision 34, the twelve streaming cursor rows completed in revision 36, and the 31 column-visibility rows completed in revision 38.**",
        f"The shape of the work is visible in the {status_counts['Missing Go']} `Missing Go` rows, of which {prefix_counts['SB-CXX']['Missing Go']} are the C++ members in [§19.2](#sec-19-2) that no Shoal layer exports.",
        f"`Behavior mismatch` ({status_counts['Behavior mismatch']}) is the bucket that sets the schedule: {python_visible_behavior} rows on the Python-visible and curated C++ surface each need a differential test against a live cluster or the exported ABI, and {prefix_counts['SB-CXX']['Behavior mismatch']} are destructors of classes bound into Python, where the destruction point is user-observable and the model differs from Go finalisation ([§19.1](#sec-19-1)).",
        f"`Intentional divergence` ({status_counts[INTENTIONAL_DIVERGENCE_STATUS]}) is dominated by one upstream fact: {prefix_counts['SB-STAT'][INTENTIONAL_DIVERGENCE_STATUS]} rows are cluster-status accessors Accumulo itself deleted ([§14](#sec-14), [SB-DIV-016](#sec-26)).",
        f"`Missing C ABI` ({status_counts['Missing C ABI']}) is now led by the key value API (36), pandas ({prefix_counts['SB-PANDA']['Missing C ABI']}), packaging/import scaffolding ({prefix_counts['SB-PKG']['Missing C ABI']}), PyTorch ({prefix_counts['SB-TORCH']['Missing C ABI']}), the remaining scanner rows ({prefix_counts['SB-SCAN']['Missing C ABI']}), and the remaining data-model row ({prefix_counts['SB-DATA']['Missing C ABI']}).",
    ]
    for phrase in expected_phrases:
        require(phrase in normalized, f"missing or stale status narrative: {phrase}")


def validate_local_line_number_removal(full_text: str) -> None:
    for path in TARGETED_LOCAL_CITATIONS:
        require(
            re.search(rf"`{re.escape(path)}:\d", full_text) is None,
            f"line-number citation remains for {path}",
        )


def normalize_file_citation(span: str) -> str:
    return span.split(":", 1)[0]


def is_file_citation(span: str) -> bool:
    path = normalize_file_citation(span)
    if path in FILE_CITATION_BASENAMES:
        return True
    if path.endswith("/**") or any(marker in path for marker in ("{", "}", "*", "?")):
        return "/" in path
    suffix = Path(path).suffix.lower()
    return suffix in FILE_CITATION_SUFFIXES


def load_targeted_contents(
    targeted_paths: set[str],
    repo_root: Path | None = None,
) -> dict[str, str]:
    root = repo_root or DOC_PATH.parent.parent
    return {
        path: (root / path).read_text(encoding="utf-8", errors="ignore")
        for path in targeted_paths
    }


def is_anchor_glue(text: str) -> bool:
    words = re.findall(r"[A-Za-z]+", text)
    if any(word not in {"and", "or", "s"} for word in words):
        return False
    remainder = re.sub(r"[A-Za-z]+", "", text)
    return all(character in " \t`,;/+&()-" for character in remainder)


def is_file_group_glue(text: str) -> bool:
    return all(character in " \t`,;()" for character in text)


def preceding_anchor_groups(
    spans: list[tuple[str, int, int]],
    start_index: int,
    cell: str,
) -> list[list[str]]:
    candidate_indices: list[int] = []
    previous_index = start_index - 1
    while previous_index >= 0:
        previous_span, _previous_start, _previous_end = spans[previous_index]
        if previous_span.startswith("SB-") or previous_span in {"n/a", "—"}:
            break
        if is_file_citation(previous_span):
            break
        candidate_indices.insert(0, previous_index)
        previous_index -= 1

    if not candidate_indices:
        return []

    groups: list[list[str]] = []
    current_group = [spans[candidate_indices[0]][0]]
    for earlier_index, later_index in zip(candidate_indices, candidate_indices[1:]):
        glue = cell[spans[earlier_index][2]:spans[later_index][1]]
        if is_anchor_glue(glue):
            current_group.append(spans[later_index][0])
            continue
        groups.append(current_group)
        current_group = [spans[later_index][0]]
    groups.append(current_group)
    return groups


def extract_path_anchor_bindings(cell: str, targeted_paths: set[str]) -> dict[str, list[str]]:
    bindings: dict[str, list[str]] = defaultdict(list)
    spans = [
        (match.group(1), match.start(1), match.end(1))
        for match in CODE_SPAN_RE.finditer(cell)
    ]

    index = 0
    while index < len(spans):
        span, _start, _end = spans[index]
        if not is_file_citation(span):
            index += 1
            continue

        group_indices = [index]
        lookahead = index + 1
        while (
            lookahead < len(spans)
            and is_file_citation(spans[lookahead][0])
            and is_file_group_glue(cell[spans[lookahead - 1][2]:spans[lookahead][1]])
        ):
            group_indices.append(lookahead)
            lookahead += 1

        anchor_groups = preceding_anchor_groups(spans, group_indices[0], cell)
        assignments: list[list[str]]
        if not anchor_groups:
            assignments = [[] for _ in group_indices]
        elif len(group_indices) == 1:
            assignments = [anchor_groups[-1]]
        elif len(anchor_groups) == 1:
            assignments = anchor_groups * len(group_indices)
        elif len(anchor_groups) >= len(group_indices):
            assignments = anchor_groups[-len(group_indices):]
        else:
            assignments = [[] for _ in group_indices]

        for group_index, anchors in zip(group_indices, assignments):
            path = normalize_file_citation(spans[group_index][0])
            if path in targeted_paths:
                bindings[path].extend(anchors)

        index = lookahead

    return dict(bindings)


@lru_cache(maxsize=None)
def identifier_boundary_pattern(token: str) -> re.Pattern[str]:
    return re.compile(
        rf"(?<![{IDENTIFIER_BOUNDARY_CLASS}]){re.escape(token)}(?![{IDENTIFIER_BOUNDARY_CLASS}])",
        re.DOTALL,
    )


@lru_cache(maxsize=None)
def anchor_boundary_pattern(anchor: str) -> re.Pattern[str]:
    pieces: list[str] = []
    for part in ANCHOR_PART_RE.findall(anchor):
        if part == "...":
            pieces.append(r"[\s\S]*?")
            continue
        if part.isspace():
            pieces.append(r"\s+")
            continue
        if IDENT_RE.fullmatch(part):
            pieces.append(identifier_boundary_pattern(part).pattern)
            continue
        pieces.append(re.escape(part))
        if part in WHITESPACE_TOLERANT_PUNCTUATION:
            pieces.append(r"\s*")
    return re.compile("".join(pieces), re.DOTALL)


def significant_anchor_identifiers(anchor: str, ignored_tokens: set[str]) -> list[str]:
    seen: set[str] = set()
    significant: list[str] = []
    for token in IDENT_RE.findall(anchor):
        if token in ignored_tokens or token in seen:
            continue
        seen.add(token)
        significant.append(token)
    return significant


@lru_cache(maxsize=None)
def declaration_constructs(content: str) -> tuple[str, ...]:
    constructs: list[str] = []
    seen: set[str] = set()
    for pattern in DECLARATION_PATTERNS:
        for match in pattern.finditer(content):
            construct = match.group(0).strip()
            if construct and construct not in seen:
                constructs.append(construct)
                seen.add(construct)
    if not constructs:
        constructs = [line.strip() for line in content.splitlines() if line.strip()]
    return tuple(constructs)


@lru_cache(maxsize=None)
def ordered_identifier_pattern(tokens: tuple[str, ...]) -> re.Pattern[str]:
    pieces = [identifier_boundary_pattern(tokens[0]).pattern]
    for token in tokens[1:]:
        pieces.append(r"[\s\S]*?")
        pieces.append(identifier_boundary_pattern(token).pattern)
    return re.compile("".join(pieces), re.DOTALL)


def compound_anchor_matches_construct(anchor: str, content: str, ignored_tokens: set[str]) -> bool:
    tokens = significant_anchor_identifiers(anchor, ignored_tokens)
    if len(tokens) <= 1:
        return False
    pattern = ordered_identifier_pattern(tuple(tokens))
    return any(pattern.search(construct) for construct in declaration_constructs(content))


def anchor_matches_content(anchor: str, content: str, ignored_tokens: set[str]) -> bool:
    if anchor_boundary_pattern(anchor).search(content):
        return True
    tokens = significant_anchor_identifiers(anchor, ignored_tokens)
    if not tokens:
        return False
    if len(tokens) == 1:
        return identifier_boundary_pattern(tokens[0]).search(content) is not None
    return compound_anchor_matches_construct(anchor, content, ignored_tokens)


def filtered_local_anchors(ref: str, anchors: list[str]) -> list[str]:
    if ref in OPTIONAL_ANCHOR_CITATIONS:
        return [anchor for anchor in anchors if "shoal_" in anchor or "SHOAL_" in anchor]
    return anchors


def validate_targeted_symbol_anchors(
    lines: list[str],
    *,
    targeted_paths: set[str] = ANCHOR_CHECKED_CITATIONS,
    repo_root: Path | None = None,
) -> None:
    contents = load_targeted_contents(targeted_paths, repo_root=repo_root)

    for _line_number, row_id, cells in iter_matrix_rows(lines):
        for cell in cells[1:]:
            path_anchor_bindings = extract_path_anchor_bindings(cell, targeted_paths)
            if not path_anchor_bindings:
                continue
            for ref, anchors in path_anchor_bindings.items():
                anchors = filtered_local_anchors(ref, anchors)
                if ref not in OPTIONAL_ANCHOR_CITATIONS:
                    require(
                        anchors,
                        f"{row_id} cites {ref} without an adjacent local symbol/test anchor",
                    )
                elif not anchors:
                    continue
                content = contents[ref]
                missing = [
                    anchor
                    for anchor in anchors
                    if not anchor_matches_content(anchor, content, IGNORED_ANCHOR_TOKENS)
                ]
                require(
                    not missing,
                    f"{row_id} cites {ref} with stale adjacent anchors: {', '.join(missing)}",
                )


def main(argv: Sequence[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    if args == ["--rewrite-cabi-symbol-manifest"]:
        write_c_abi_symbol_manifest()
        print("matrix-cabi-symbol-manifest-ok")
        return
    require(not args, f"unexpected arguments: {args}")
    full_text = DOC_PATH.read_text(encoding="utf-8")
    lines = full_text.splitlines()
    validate_counts(lines, full_text)
    validate_local_line_number_removal(full_text)
    validate_targeted_symbol_anchors(lines)
    print("matrix-counts-ok")
    print("matrix-citations-ok")


if __name__ == "__main__":
    main()
