from __future__ import annotations

from collections import Counter, defaultdict
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


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def parse_rows(lines: list[str]) -> tuple[Counter[str], dict[str, Counter[str]]]:
    status_counts: Counter[str] = Counter()
    prefix_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for line in lines:
        if not line.startswith("| SB-") or line.startswith("| SB-GAP"):
            continue
        parts = [part.strip() for part in line.split("|")[1:-1]]
        if len(parts) < 3:
            continue
        row_id = parts[0]
        status = parts[-2]
        if status not in STATUSES:
            continue
        prefix = "-".join(row_id.split("-")[:2])
        status_counts[status] += 1
        prefix_counts[prefix][status] += 1
    return status_counts, prefix_counts


def validate_counts(lines: list[str], full_text: str) -> None:
    for metadata_line in EXPECTED_METADATA_LINES:
        require(metadata_line in full_text, f"missing metadata line: {metadata_line}")

    status_counts, prefix_counts = parse_rows(lines)
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


def validate_targeted_symbol_anchors(lines: list[str]) -> None:
    contents = {
        path: (DOC_PATH.parent.parent / path).read_text(encoding="utf-8", errors="ignore")
        for path in TARGETED_LOCAL_CITATIONS
    }
    ignored_tokens = {
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

    for line in lines:
        if not line.startswith("| SB-"):
            continue
        cells = [cell.strip() for cell in line.split("|")[1:-1]]
        row_id = cells[0]
        for cell in cells[1:]:
            spans = [match.group(1) for match in CODE_SPAN_RE.finditer(cell)]
            target_refs = [span for span in spans if span in TARGETED_LOCAL_CITATIONS]
            if not target_refs:
                continue
            anchors = [
                span
                for span in spans
                if span not in TARGETED_LOCAL_CITATIONS and not span.startswith("SB-") and span not in {"n/a", "—"}
            ]
            if "main()" in cell and "main()" not in anchors:
                anchors.append("main()")
            if "test_v1_initializers()" in cell and "test_v1_initializers()" not in anchors:
                anchors.append("test_v1_initializers()")
            if "static_assert" in cell and "static_assert" not in anchors:
                anchors.append("static_assert")

            for ref in target_refs:
                content = contents[ref]
                found = False
                for anchor in anchors:
                    if anchor in content:
                        found = True
                        break
                    for token in IDENT_RE.findall(anchor):
                        if token in ignored_tokens or len(token) < 3:
                            continue
                        if token in content:
                            found = True
                            break
                    if found:
                        break
                require(found, f"{row_id} cites {ref} without a matching local symbol/test anchor")


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
