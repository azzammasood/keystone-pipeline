"""Schema-drift detection per era: unknown fields or JSON types fail the file loudly.

The registry (`contracts/drift/gharchive.yml`) lists, per era and event type, every key path up
to `MAX_DEPTH` levels below the record root (array elements are `path[]`) with the JSON types
seen there. Paths outside the payload are shared by all types of an era (`_envelope`). It is
learned from the Phase 3 sample hours and days (`keystone parse learn-drift`) and reviewed in
git. Only key paths and type names are stored, never values, so it holds no personal data.

`null` is accepted anywhere: it changes no type, and the typed parser decides what it means.
"""

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from keystone.paths import CONTRACTS_DIR

MAX_DEPTH = 3
ENVELOPE = "_envelope"
REGISTRY_PATH = CONTRACTS_DIR / "drift" / "gharchive.yml"
_TYPES: dict[type, str] = {
    type(None): "null",
    str: "str",
    int: "int",
    float: "float",
    bool: "bool",
    dict: "dict",
    list: "list",
}

# era -> scope (event type or _envelope) -> path -> JSON types
Registry = dict[str, dict[str, dict[str, list[str]]]]


class DriftError(RuntimeError):
    """A file holds fields or types its era's registry does not know."""


@dataclass(frozen=True)
class DriftFinding:
    kind: str  # unknown_event_type | unknown_field | unknown_type | era_shape
    scope: str
    path: str
    detail: str

    def line(self) -> str:
        return f"{self.kind} {self.scope} {self.path} {self.detail}".strip()


def json_type(value: Any) -> str:
    return _TYPES.get(type(value)) or type(value).__name__


def load_registry(path: Path = REGISTRY_PATH) -> Registry:
    if not path.exists():
        return {}
    data: Registry = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return data


def save_registry(registry: Registry, path: Path = REGISTRY_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clean = {
        era: {
            scope: {p: sorted(set(t)) for p, t in sorted(paths.items())}
            for scope, paths in sorted(scopes.items())
        }
        for era, scopes in sorted(registry.items())
    }
    header = (
        "# Known key paths and JSON types per schema era and event type (see\n"
        "# src/keystone/parsers/drift.py). Learned from sample hours with\n"
        "# `keystone parse learn-drift`; review every diff. Paths and type names only.\n"
    )
    body = yaml.safe_dump(clean, sort_keys=False, default_flow_style=None, width=100)
    path.write_text(header + body, encoding="utf-8")


@dataclass
class ShapeObserver:
    """Collects (scope, path, type) counts over one file's records."""

    seen: Counter[tuple[str, str, str]] = field(default_factory=Counter)
    event_types: Counter[str] = field(default_factory=Counter)

    def observe(self, record: dict[str, Any]) -> None:
        etype = record.get("type")
        scope = etype if isinstance(etype, str) else "?"
        self.event_types[scope] += 1
        for key, value in record.items():
            target = scope if key == "payload" else ENVELOPE
            self._walk(target, key, value, 1)

    def _walk(self, scope: str, path: str, value: Any, depth: int) -> None:
        self.seen[(scope, path, json_type(value))] += 1
        if depth >= MAX_DEPTH:
            return
        if isinstance(value, dict):
            for key, child in value.items():
                self._walk(scope, f"{path}.{key}", child, depth + 1)
        elif isinstance(value, list):
            for child in value:
                self._walk(scope, f"{path}[]", child, depth)

    def findings(self, registry: Registry, era: str) -> list[DriftFinding]:
        known = registry.get(era, {})
        out: list[DriftFinding] = []
        for scope, n in sorted(self.event_types.items()):
            if scope not in known:
                out.append(DriftFinding("unknown_event_type", scope, "", f"{n} events"))
        for (scope, path, jtype), n in sorted(self.seen.items()):
            paths = known.get(scope)
            if paths is None:
                continue  # already reported as an unknown event type
            types = paths.get(path)
            if types is None:
                out.append(DriftFinding("unknown_field", scope, path, f"{jtype} x{n}"))
            elif jtype != "null" and jtype not in types:
                out.append(DriftFinding("unknown_type", scope, path, f"{jtype} not in {types}"))
        return out

    def merge_into(self, registry: Registry, era: str) -> int:
        """Add everything observed to `registry[era]`; returns the number of new entries."""
        scopes = registry.setdefault(era, {})
        added = 0
        for scope in self.event_types:
            scopes.setdefault(scope, {})
        for scope, path, jtype in self.seen:
            types = scopes.setdefault(scope, {}).setdefault(path, [])
            if jtype not in types:
                types.append(jtype)
                added += 1
        return added
