from __future__ import annotations

from collections.abc import Iterator, Sequence
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
import re
import sys


DOC_PATH = Path(__file__).with_name("sharkbite-compatibility.md")
# The manifest pins the current inventory: one "ROW-ID STATUS" entry per row, in
# document order. Update it whenever that inventory legitimately changes — an
# audit that reclassifies rows, or an implementation change that moves rows
# between statuses under the decision procedure in section 4.2 — together with
# EXPECTED_REVISION and the pinned counts below, and review every added,
# removed, or reclassified row in code review.
ROW_MANIFEST = DOC_PATH.with_name("sharkbite-compatibility-rows.txt")

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
# Revision of the independently audited inventory pinned below. A document
# revision bump cannot land without updating this constant, because the
# expected document-status snippet and the narrative phrasing are derived from
# it.
EXPECTED_REVISION = 17
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
    "Covered": 0,
    "Missing Go": 2436,
    "Missing C ABI": 128,
    "Behavior mismatch": 160,
    "Intentional divergence (approval required)": 87,
    "Not required (rationale required)": 392,
}

EXPECTED_PREFIX_COUNTS = {
    "SB-BASE": status_count_map(missing_c_abi=18, not_required=2),
    "SB-CFG": status_count_map(
        missing_c_abi=20,
        behavior_mismatch=8,
        intentional_divergence=2,
        not_required=6,
    ),
    "SB-CONN": status_count_map(
        missing_go=3,
        missing_c_abi=2,
        behavior_mismatch=7,
        intentional_divergence=1,
    ),
    "SB-CPP": status_count_map(
        missing_go=11,
        missing_c_abi=4,
        behavior_mismatch=47,
        not_required=8,
    ),
    "SB-CXX": status_count_map(
        missing_go=2307,
        behavior_mismatch=15,
        not_required=304,
    ),
    "SB-DATA": status_count_map(
        missing_go=15,
        missing_c_abi=15,
        behavior_mismatch=39,
        not_required=6,
    ),
    "SB-EMB": status_count_map(not_required=35),
    "SB-ERR": status_count_map(
        missing_go=2,
        missing_c_abi=5,
        behavior_mismatch=5,
        intentional_divergence=1,
        not_required=3,
    ),
    "SB-HDFS": status_count_map(missing_go=26),
    "SB-LOG": status_count_map(missing_go=2, behavior_mismatch=1),
    "SB-NS": status_count_map(missing_go=7, behavior_mismatch=1),
    "SB-PANDA": status_count_map(missing_c_abi=20, not_required=1),
    "SB-PKG": status_count_map(
        missing_c_abi=10,
        behavior_mismatch=1,
        intentional_divergence=1,
        not_required=2,
    ),
    "SB-RFILE": status_count_map(
        missing_go=32,
        behavior_mismatch=1,
        not_required=3,
    ),
    "SB-SCAN": status_count_map(
        missing_go=8,
        missing_c_abi=4,
        behavior_mismatch=11,
        not_required=5,
    ),
    "SB-SEC": status_count_map(missing_go=17, behavior_mismatch=1, not_required=1),
    "SB-STAT": status_count_map(
        missing_go=1,
        intentional_divergence=82,
        not_required=1,
    ),
    "SB-TABLE": status_count_map(
        missing_go=3,
        missing_c_abi=11,
        behavior_mismatch=2,
        not_required=6,
    ),
    "SB-TORCH": status_count_map(missing_c_abi=9),
    "SB-WRITE": status_count_map(
        missing_go=1,
        missing_c_abi=6,
        behavior_mismatch=7,
        not_required=9,
    ),
    "SB-XCUT": status_count_map(
        missing_go=1,
        missing_c_abi=4,
        behavior_mismatch=14,
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
        "`phrocker/shoal-oss` exact audited baseline for revision 17 "
        "`298a036d32247a85941724798c85033cb939cde7` "
        "(\"Merge pull request #101 from phrocker/rewrite/108-compatibility-matrix-followup\"), "
        "plus the `accumulo` client-configuration and instance-topology additions "
        "introduced in the same change as this revision"
    ),
    "Shoal C ABI version": "`SHOAL_ABI_VERSION 1u` (`capi/include/shoal_types.h`)",
}

EXPECTED_DOCUMENT_STATUS_SNIPPETS = (
    "Normative gate. Binding on all Sharkbite-compatibility work.",
    f"Revision {EXPECTED_REVISION} — the first implementation-driven revision",
    "Revision 16 applied the fifteenth independent audit",
    "Revision 15 applied the fourteenth audit",
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
    "docs/sharkbite-compatibility-rows.txt (row ids, order and pinned statuses) "
    "in the same commit, together with the "
    "audit evidence that justifies the new inventory"
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
    "cmd/shoal-capi/table_admin_export_test.go",
    "cmd/shoal-capi/cabi_test.go",
    "cmd/shoal-capi/state_test.go",
    "cmd/shoal-capi/writer_export_test.go",
}
TARGETED_SB_CFG_CITATIONS = {
    "accumulo/configuration.go",
    "accumulo/configuration_test.go",
    "accumulo/instance.go",
    "accumulo/topology.go",
    "accumulo/topology_test.go",
    "internal/zk/manager.go",
    "internal/zk/topology_test.go",
}
OPTIONAL_ANCHOR_CITATIONS = {
    "capi/include/shoal.h",
    "capi/include/shoal_types.h",
}
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
FILE_CITATION_BASENAMES = {"ARCHITECTURE.md", "CMakeLists.txt", "Dockerfile", "Makefile", "README.md"}
IDENTIFIER_BOUNDARY_CLASS = r"A-Za-z0-9_"
IGNORED_ANCHOR_TOKENS = {
    "ABI",
    "C",
    "Go",
    "and",
    "accumulo",
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
    "internal",
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
    "zk",
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


@lru_cache(maxsize=1)
def load_expected_rows() -> tuple[tuple[str, str], ...]:
    rows = parse_row_manifest_lines(
        ROW_MANIFEST.read_text(encoding="utf-8").splitlines(),
        source=str(ROW_MANIFEST.name),
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


def validate_pinned_inventory(
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
    validate_pinned_inventory(rows, status_counts, prefix_counts)

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
        f"As of revision {EXPECTED_REVISION} that is {required_rows} of {total_rows} rows, and **none of them is satisfied**",
        f"{required_rows} rows are **required** by the final release gate ([§2.2](#sec-2)); the {status_counts[NOT_REQUIRED_STATUS]} `Not required` rows are excluded by construction, and {prefix_counts['SB-CXX'][NOT_REQUIRED_STATUS]} of those are the evidence-proved duplicates described in [§19.1](#sec-19-1).",
        "No row is `Covered`.",
        f"The shape of the work is visible in the {status_counts['Missing Go']} `Missing Go` rows, of which {prefix_counts['SB-CXX']['Missing Go']} are the C++ members in [§19.2](#sec-19-2) that no Shoal layer exports.",
        f"`Behavior mismatch` ({status_counts['Behavior mismatch']}) is the bucket that sets the schedule: {python_visible_behavior} rows on the Python-visible and curated C++ surface each need a differential test against a live cluster or the exported ABI, and {prefix_counts['SB-CXX']['Behavior mismatch']} are destructors of classes bound into Python, where the destruction point is user-observable and the model differs from Go finalisation ([§19.1](#sec-19-1)).",
        f"`Intentional divergence` ({status_counts[INTENTIONAL_DIVERGENCE_STATUS]}) is dominated by one upstream fact: {prefix_counts['SB-STAT'][INTENTIONAL_DIVERGENCE_STATUS]} rows are cluster-status accessors Accumulo itself deleted ([§14](#sec-14), [SB-DIV-016](#sec-26)).",
        f"`Missing C ABI` ({status_counts['Missing C ABI']}) is concentrated in the Python layers — pandas ({prefix_counts['SB-PANDA']['Missing C ABI']}), high-level helpers ({prefix_counts['SB-BASE']['Missing C ABI']}), PyTorch ({prefix_counts['SB-TORCH']['Missing C ABI']}).",
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
    targeted_paths: set[str] = TARGETED_LOCAL_CITATIONS,
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


def main() -> None:
    full_text = DOC_PATH.read_text(encoding="utf-8")
    lines = full_text.splitlines()
    validate_counts(lines, full_text)
    validate_local_line_number_removal(full_text)
    validate_targeted_symbol_anchors(lines)
    validate_targeted_symbol_anchors(lines, targeted_paths=TARGETED_SB_CFG_CITATIONS)
    print("matrix-counts-ok")
    print("matrix-citations-ok")


if __name__ == "__main__":
    main()
