"""Dependency-free repository hygiene checks."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {".py", ".md", ".json", ".yml", ".yaml", ".toml"}


def main() -> int:
    failures: list[str] = []
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or path.suffix not in TEXT_SUFFIXES:
            continue
        if any(part in {".git", ".venv", "__pycache__", "reports"} for part in path.parts):
            continue
        text = path.read_text(encoding="utf-8")
        if not text.endswith("\n"):
            failures.append(f"{path.relative_to(ROOT)}: missing final newline")
        for number, line in enumerate(text.splitlines(), 1):
            if line.rstrip() != line:
                failures.append(
                    f"{path.relative_to(ROOT)}:{number}: trailing whitespace"
                )
            if path.suffix == ".py" and "\t" in line:
                failures.append(f"{path.relative_to(ROOT)}:{number}: tab in Python")
        if path.suffix == ".json":
            try:
                json.loads(text)
            except json.JSONDecodeError as exc:
                failures.append(f"{path.relative_to(ROOT)}: invalid JSON: {exc}")
    if failures:
        print("\n".join(failures))
        return 1
    print("lint: repository text and JSON checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

