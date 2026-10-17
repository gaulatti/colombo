"""Require exact, nonempty LLVM line and branch coverage for application sources."""

import json
from pathlib import Path
import sys


def check(report):
    if report.get("type") != "llvm.coverage.json.export":
        raise ValueError("Expected an LLVM coverage export")
    data = report.get("data")
    if not isinstance(data, list) or not data:
        raise ValueError("Coverage data is missing")
    counts = {metric: [0, 0] for metric in ("lines", "branches")}
    source_files = 0
    for group in data:
        files = group.get("files")
        if not isinstance(files, list) or not files:
            raise ValueError("Per-file coverage is missing")
        for file in files:
            filename = file.get("filename", "")
            parts = Path(filename).parts
            if "src" not in parts or "vendor" in parts or ".cargo" in parts:
                continue
            try:
                Path(filename).resolve().relative_to(Path("src").resolve())
            except ValueError:
                continue
            source_files += 1
            for metric in counts:
                summary = file.get("summary", {}).get(metric, {})
                count, covered = summary.get("count"), summary.get("covered")
                if type(count) is not int or type(covered) is not int:
                    raise ValueError(f"Missing integer {metric} counts: {filename}")
                if count < 0 or not 0 <= covered <= count:
                    raise ValueError(f"Invalid {metric} counts: {filename}")
                counts[metric][0] += count
                counts[metric][1] += covered
    if not source_files:
        raise ValueError("No application source coverage was measured")
    failures = []
    for metric, (count, covered) in counts.items():
        print(f"{metric}: {covered}/{count}")
        if count == 0:
            failures.append(f"No {metric} measured")
        elif count != covered:
            failures.append(f"{metric}: {count - covered} uncovered; require exactly 100%")
    if failures:
        raise ValueError("; ".join(failures))
    return counts


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            raise ValueError("Usage: check_coverage.py coverage.json")
        check(json.loads(Path(sys.argv[1]).read_text()))
    except (OSError, ValueError, TypeError, AttributeError) as error:
        print(f"Coverage gate failed: {error}", file=sys.stderr)
        sys.exit(1)
