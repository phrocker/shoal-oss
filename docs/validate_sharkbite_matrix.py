from __future__ import annotations

from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
import re
import sys


DOC_PATH = Path(__file__).with_name("sharkbite-compatibility.md")

STATUSES = {
    "Covered",
    "Missing Go",
    "Missing C ABI",
    "Behavior mismatch",
    "Intentional divergence (approval required)",
    "Not required (rationale required)",
}

EXPECTED_STATUS_COUNTS = {
    "Covered": 51,
    "Missing Go": 133,
    "Missing C ABI": 51,
    "Behavior mismatch": 66,
    "Intentional divergence (approval required)": 4,
    "Not required (rationale required)": 45,
}

EXPECTED_PREFIX_COUNTS = {
    "SB-CONN": {
        "Covered": 3,
        "Missing Go": 3,
        "Missing C ABI": 2,
        "Behavior mismatch": 5,
        "Intentional divergence (approval required)": 1,
        "Not required (rationale required)": 0,
    },
    "SB-ERR": {
        "Covered": 2,
        "Missing Go": 2,
        "Missing C ABI": 5,
        "Behavior mismatch": 2,
        "Intentional divergence (approval required)": 1,
        "Not required (rationale required)": 3,
    },
    "SB-TABLE": {
        "Covered": 8,
        "Missing Go": 3,
        "Missing C ABI": 0,
        "Behavior mismatch": 5,
        "Intentional divergence (approval required)": 0,
        "Not required (rationale required)": 6,
    },
    "SB-CPP": {
        "Covered": 8,
        "Missing Go": 3,
        "Missing C ABI": 0,
        "Behavior mismatch": 2,
        "Intentional divergence (approval required)": 0,
        "Not required (rationale required)": 1,
    },
    "SB-XCUT": {
        "Covered": 10,
        "Missing Go": 1,
        "Missing C ABI": 2,
        "Behavior mismatch": 5,
        "Intentional divergence (approval required)": 0,
        "Not required (rationale required)": 0,
    },
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

EXPECTED_METADATA_LINES = [
    "| Shoal review scope | `phrocker/shoal-oss` PR #84 source branch `rewrite/108-capi-table-admin` |",
    "| Shoal reviewed code baseline | exact-head review parent `ed237d06eea9ce707dd9e6d9715ce380d7818e3e` -> head `ab6717610a155637106ffa8136e2d8d997b341e1` (`ab67176`, code baseline for the 51 covered rows) |",
    "| Rows | 350 |",
    "| Covered rows | 51 (14.6 percent) |",
]

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
    "by",
    "full",
    "in",
    "on",
    "plus",
    "read",
    "struct",
    "under",
    "with",
}


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def parse_rows(lines: list[str]) -> tuple[Counter[str], dict[str, Counter[str]], set[str]]:
    status_counts: Counter[str] = Counter()
    prefix_counts: dict[str, Counter[str]] = defaultdict(Counter)
    accepted_row_ids: dict[str, tuple[int, str]] = {}
    for line_number, line in enumerate(lines, start=1):
        if not line.startswith("| SB-") or line.startswith("| SB-GAP"):
            continue
        parts = [part.strip() for part in line.split("|")[1:-1]]
        if len(parts) < 3:
            continue
        row_id = parts[0]
        status = parts[-2]
        if status not in STATUSES:
            continue
        previous = accepted_row_ids.get(row_id)
        if previous is not None:
            fail(
                f"duplicate accepted row id {row_id} on lines {previous[0]} and {line_number} "
                f"({previous[1]} vs {status})"
            )
        accepted_row_ids[row_id] = (line_number, status)
        prefix = "-".join(row_id.split("-")[:2])
        status_counts[status] += 1
        prefix_counts[prefix][status] += 1
    return status_counts, prefix_counts, set(accepted_row_ids)


def validate_counts(lines: list[str], full_text: str) -> None:
    for metadata_line in EXPECTED_METADATA_LINES:
        require(metadata_line in full_text, f"missing metadata line: {metadata_line}")

    status_counts, prefix_counts, row_ids = parse_rows(lines)
    require(len(row_ids) == 350, f"expected 350 unique rows, found {len(row_ids)}")
    require(sum(status_counts.values()) == 350, f"expected 350 rows, found {sum(status_counts.values())}")

    for status, expected in EXPECTED_STATUS_COUNTS.items():
        actual = status_counts[status]
        require(actual == expected, f"expected {expected} rows for {status}, found {actual}")

    for prefix, expected_counts in EXPECTED_PREFIX_COUNTS.items():
        for status, expected in expected_counts.items():
            actual = prefix_counts[prefix][status]
            require(
                actual == expected,
                f"expected {expected} rows for {prefix} / {status}, found {actual}",
            )


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
    return re.compile("".join(pieces), re.DOTALL)


def anchor_matches_content(anchor: str, content: str, ignored_tokens: set[str]) -> bool:
    if anchor_boundary_pattern(anchor).search(content):
        return True
    for token in IDENT_RE.findall(anchor):
        if token in ignored_tokens or len(token) < 3:
            continue
        if identifier_boundary_pattern(token).search(content):
            return True
    return False


def validate_targeted_symbol_anchors(
    lines: list[str],
    *,
    targeted_paths: set[str] = TARGETED_LOCAL_CITATIONS,
    repo_root: Path | None = None,
) -> None:
    contents = load_targeted_contents(targeted_paths, repo_root=repo_root)

    for line in lines:
        if not line.startswith("| SB-"):
            continue
        cells = [cell.strip() for cell in line.split("|")[1:-1]]
        row_id = cells[0]
        for cell in cells[1:]:
            path_anchor_bindings = extract_path_anchor_bindings(cell, targeted_paths)
            if not path_anchor_bindings:
                continue
            for ref, anchors in path_anchor_bindings.items():
                require(anchors, f"{row_id} cites {ref} without an adjacent local symbol/test anchor")
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
    print("matrix-counts-ok")
    print("matrix-citations-ok")


if __name__ == "__main__":
    main()
