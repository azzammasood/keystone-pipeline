"""Catalog metadata: `config/metadata.yml` for datasets outside dbt, dbt `meta` for models.

A dataset id is `<namespace>/<name>` in the naming scheme of lineage/naming.py. Config ids may
hold one template part (`{hour}`, `{ecosystem}`, `{namespace}`), which matches any value.
"""

import json
import re
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any

import yaml

from keystone.paths import CONFIG_DIR

METADATA_PATH = CONFIG_DIR / "metadata.yml"
_TEMPLATE = re.compile(r"\{[a-z_]+\}")


@dataclass(frozen=True)
class DatasetMeta:
    id: str
    title: str
    layer: str
    domain: str
    classification: str
    description: str
    contract: str
    freshness_sla_hours: int
    owner: str
    published: bool = False

    def missing(self) -> list[str]:
        """Required catalog fields that are empty (brief 9b: every dataset has them)."""
        fields = ("owner", "description", "domain", "contract", "classification")
        out = [f for f in fields if not getattr(self, f)]
        if not self.freshness_sla_hours:
            out.append("freshness_sla_hours")
        return out


class Metadata:
    def __init__(self, path: Path = METADATA_PATH) -> None:
        self.raw: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
        self._dbt: dict[str, DatasetMeta] = {}

    @property
    def owner_id(self) -> str:
        return str(self.raw["owner"]["id"])

    @property
    def glossary(self) -> dict[str, dict[str, str]]:
        return dict(self.raw["glossary"])

    @property
    def domains(self) -> dict[str, str]:
        return dict(self.raw["domains"])

    @property
    def classifications(self) -> dict[str, str]:
        return dict(self.raw["classifications"])

    @cached_property
    def _config_datasets(self) -> list[tuple[re.Pattern[str], DatasetMeta]]:
        sla = self.raw["freshness_sla_hours"]
        out = []
        for d in self.raw["datasets"]:
            layer = d["layer"]
            meta = DatasetMeta(
                id=d["id"],
                title=d["title"],
                layer=layer,
                domain=d["domain"],
                classification=d["classification"],
                description=" ".join(str(d["description"]).split()),
                contract=d.get("contract", ""),
                freshness_sla_hours=int(d.get("freshness_sla_hours", sla.get(layer, 0))),
                owner=d.get("owner", self.owner_id),
            )
            pieces = []  # the id with each template part as a wildcard
            last = 0
            for m in _TEMPLATE.finditer(d["id"]):
                pieces.append(re.escape(d["id"][last : m.start()]))
                pieces.append(r"[^/]+")
                last = m.end()
            pieces.append(re.escape(d["id"][last:]))
            pattern = "".join(pieces)
            out.append((re.compile(pattern), meta))
        return out

    def config_datasets(self) -> list[DatasetMeta]:
        return [m for _, m in self._config_datasets]

    def register_dbt(self, target_dir: Path) -> list[DatasetMeta]:
        """Catalog entries of every dbt model and seed (from manifest meta and descriptions)."""
        manifest = json.loads((target_dir / "manifest.json").read_text("utf-8"))
        out = []
        for node in manifest["nodes"].values():
            if node["resource_type"] not in ("model", "seed"):
                continue
            m = node.get("config", {}).get("meta", {}) or node.get("meta", {})
            schema, name = node["schema"], node["name"]
            ds = DatasetMeta(
                id=f"duckdb://keystone/keystone.{schema}.{name}",
                title=f"{schema}.{name}",
                layer=str(m.get("layer", "")),
                domain=str(m.get("domain", "")),
                classification=str(m.get("classification", "")),
                description=" ".join(str(node.get("description", "")).split()),
                contract=f"metadata/contracts/dbt_{schema}_{name}.odcs.yaml",
                freshness_sla_hours=int(m.get("freshness_sla_hours", 0) or 0),
                owner=str(m.get("owner", "")),
                published="publish" in node.get("tags", []),
            )
            self._dbt[ds.id] = ds
            out.append(ds)
        return sorted(out, key=lambda d: d.id)

    def dataset(self, dataset_id: str) -> DatasetMeta:
        if dataset_id in self._dbt:
            return self._dbt[dataset_id]
        for pattern, meta in self._config_datasets:
            if pattern.fullmatch(dataset_id):
                return meta
        raise KeyError(f"no catalog metadata for {dataset_id}")

    def column_doc(self, table: str, column: str) -> str:
        docs = self.raw["column_docs"]
        return str(docs.get(table, {}).get(column) or docs["_lineage"].get(column) or "")

    def column_terms(self, column: str) -> list[str]:
        return list(self.raw["column_terms"].get(column, []))

    def layer_terms(self, layer: str) -> list[str]:
        return list(self.raw["term_by_layer"].get(layer, []))
