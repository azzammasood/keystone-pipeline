"""Schema check for CI: `python -m keystone.export.schemas`.

Every `contracts/*.schema.json` must be a valid JSON Schema 2020-12, and every example under
`tests/fixtures/schemas/<schema stem>/` must validate (`valid_*.json`) or be rejected
(`invalid_*.json`) by it. Exit 1 on any failure.
"""

import json
import sys
from pathlib import Path

import jsonschema
from jsonschema.validators import Draft202012Validator

from keystone.paths import CONTRACTS_DIR, REPO_ROOT

EXAMPLES_DIR = REPO_ROOT / "tests" / "fixtures" / "schemas"


def _stem(path: Path) -> str:
    return path.name.removesuffix(".schema.json")


def check(contracts: Path = CONTRACTS_DIR, examples: Path = EXAMPLES_DIR) -> list[str]:
    """Problems found; empty when all schemas and examples pass."""
    problems: list[str] = []
    schemas = sorted(contracts.glob("*.schema.json"))
    if not schemas:
        return [f"no *.schema.json under {contracts}"]
    for path in schemas:
        schema = json.loads(path.read_text("utf-8"))
        try:
            Draft202012Validator.check_schema(schema)
        except jsonschema.SchemaError as exc:
            problems.append(f"{path.name}: not a valid 2020-12 schema: {exc.message}")
            continue
        validator = Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)
        cases = sorted((examples / _stem(path)).glob("*.json"))
        if not any(c.name.startswith("valid_") for c in cases):
            problems.append(f"{path.name}: no valid_*.json example in {examples / _stem(path)}")
        for case in cases:
            errors = list(validator.iter_errors(json.loads(case.read_text("utf-8"))))
            if case.name.startswith("valid_") and errors:
                problems.append(f"{case.name}: rejected: {errors[0].message[:200]}")
            elif case.name.startswith("invalid_") and not errors:
                problems.append(f"{case.name}: accepted but should be rejected")
    return problems


def main() -> int:
    problems = check()
    for p in problems:
        print(p, file=sys.stderr)
    n = len(list(CONTRACTS_DIR.glob("*.schema.json")))
    print(f"schemas={n} problems={len(problems)}")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
