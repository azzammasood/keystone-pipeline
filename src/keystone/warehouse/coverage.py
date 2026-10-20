"""Docs coverage of the dbt project: models and their real columns that carry a description.

Columns come from `catalog.json` (what the warehouse holds after `dbt docs generate`), so an
undocumented column cannot hide by being absent from the YAML. Gate: >= 95% (CLAUDE.md).
"""

import json
from dataclasses import dataclass, field
from pathlib import Path

GATE = 0.95


@dataclass
class Coverage:
    models: int = 0
    models_documented: int = 0
    columns: int = 0
    columns_documented: int = 0
    missing: list[str] = field(default_factory=list)

    @property
    def ratio(self) -> float:
        total = self.models + self.columns
        return (self.models_documented + self.columns_documented) / total if total else 0.0

    @property
    def ok(self) -> bool:
        return self.models > 0 and self.ratio >= GATE


def measure(target_dir: Path) -> Coverage:
    manifest = json.loads((target_dir / "manifest.json").read_text("utf-8"))
    catalog = json.loads((target_dir / "catalog.json").read_text("utf-8"))
    cov = Coverage()
    for uid, node in manifest["nodes"].items():
        if node["resource_type"] not in ("model", "seed") or node["package_name"] != "keystone":
            continue
        cov.models += 1
        if node.get("description", "").strip():
            cov.models_documented += 1
        else:
            cov.missing.append(node["name"])
        described = {
            name.lower() for name, col in node.get("columns", {}).items() if col.get("description")
        }
        real = catalog["nodes"].get(uid, {}).get("columns", {})
        for name in real:
            cov.columns += 1
            if name.lower() in described:
                cov.columns_documented += 1
            else:
                cov.missing.append(f"{node['name']}.{name}")
    return cov
