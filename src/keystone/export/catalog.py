"""The static metadata export the site renders in both profiles (brief 9b):

- `metadata/catalog.json`: every dataset with owner, description, domain, layer, freshness SLA,
  contract, classification and its columns (described, typed, glossary terms);
- `metadata/glossary.json`: the glossary terms and where they are attached;
- `metadata/lineage.json`: the lineage graph from the OpenLineage event log, the tools that
  produced it, and the path from a source hourly file to a published field;
- `metadata/contracts/*.yaml`: every ODCS contract (hand-written ones copied from contracts/).

Completeness is measured here and gated: at least 95% of models and of columns documented,
every dataset with all catalog fields, every owner set.
"""

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from keystone.export.meta import DatasetMeta, Metadata
from keystone.lineage import graph as lgraph
from keystone.lineage import naming
from keystone.paths import CONTRACTS_DIR, REPO_ROOT

GATE = 0.95
PUBLISHED_NAMESPACES = ("marts", "metrics")


def _contract_columns(path: Path) -> list[dict[str, Any]]:
    if path.suffix == ".avsc":
        schema = json.loads(path.read_text("utf-8"))
        return [
            {"name": f["name"], "type": json.dumps(f["type"]), "description": f.get("doc", "")}
            for f in schema["fields"]
        ]
    if not path.name.endswith(".odcs.yaml"):
        return []
    spec = yaml.safe_load(path.read_text("utf-8"))
    return [
        {
            "name": p["name"],
            "type": p.get("physicalType") or p.get("logicalType", ""),
            "description": " ".join(str(p.get("description", "")).split()),
        }
        for obj in spec["schema"]
        for p in obj.get("properties", [])
    ]


def published_copies(models: list[DatasetMeta]) -> list[DatasetMeta]:
    """The Iceberg tables `warehouse publish` writes: one per published dbt model."""
    out = []
    for m in models:
        if not m.published:
            continue
        _, _, rest = m.id.partition("duckdb://keystone/keystone.")
        schema, name = rest.split(".", 1)
        out.append(
            DatasetMeta(
                id=f"{naming.ICEBERG_NS}/{schema}.{name}",
                title=f"Published {schema}.{name} (Iceberg)",
                layer="published",
                domain=m.domain,
                classification=m.classification,
                description=f"Published copy of {schema}.{name}: {m.description}",
                contract=m.contract,
                freshness_sla_hours=m.freshness_sla_hours,
                owner=m.owner,
                published=True,
            )
        )
    return out


def build_catalog(meta: Metadata, target_dir: Path) -> dict[str, Any]:
    models = meta.register_dbt(target_dir)
    entries = []
    for ds in [*meta.config_datasets(), *models, *published_copies(models)]:
        columns = _contract_columns(REPO_ROOT / ds.contract) if ds.contract else []
        for c in columns:
            terms = meta.column_terms(c["name"])
            if terms:
                c["glossary_terms"] = terms
        entry = asdict(ds)
        entry["glossary_terms"] = meta.layer_terms(ds.layer)
        entry["columns"] = columns
        entry["missing"] = ds.missing()
        entries.append(entry)
    return {
        "owner": meta.raw["owner"],
        "domains": meta.domains,
        "classifications": meta.classifications,
        "datasets": entries,
    }


def build_glossary(meta: Metadata, catalog: dict[str, Any]) -> dict[str, Any]:
    used: dict[str, list[str]] = {t: [] for t in meta.glossary}
    for ds in catalog["datasets"]:
        for t in ds["glossary_terms"]:
            used[t].append(ds["id"])
        for c in ds["columns"]:
            for t in c.get("glossary_terms", []):
                used[t].append(f"{ds['id']}#{c['name']}")
    return {
        "terms": [
            {
                "term": term,
                "definition": " ".join(str(v["definition"]).split()),
                "status": v["status"],
                "attached_to": sorted(set(used[term])),
            }
            for term, v in meta.glossary.items()
        ]
    }


@dataclass(frozen=True)
class Completeness:
    models: int
    models_documented: int
    columns: int
    columns_documented: int
    datasets: int
    datasets_complete: int
    owners_missing: int

    @property
    def models_pct(self) -> float:
        return self.models_documented / self.models if self.models else 0.0

    @property
    def columns_pct(self) -> float:
        return self.columns_documented / self.columns if self.columns else 0.0

    @property
    def documented_pct(self) -> float:
        """Models and columns together: the public completeness metric."""
        total = self.models + self.columns
        return (self.models_documented + self.columns_documented) / total if total else 0.0

    @property
    def ok(self) -> bool:
        return (
            self.models_pct >= GATE
            and self.columns_pct >= GATE
            and self.owners_missing == 0
            and self.datasets_complete == self.datasets
        )


def completeness(catalog: dict[str, Any]) -> Completeness:
    ds = catalog["datasets"]
    models = [d for d in ds if d["id"].startswith("duckdb://keystone/keystone.")]
    cols = [c for d in ds for c in d["columns"]]
    return Completeness(
        models=len(models),
        models_documented=sum(bool(d["description"]) for d in models),
        columns=len(cols),
        columns_documented=sum(bool(c["description"]) for c in cols),
        datasets=len(ds),
        datasets_complete=sum(not d["missing"] for d in ds),
        owners_missing=sum(not d["owner"] for d in ds),
    )


def build_lineage(root: Path, meta: Metadata, catalog: dict[str, Any]) -> dict[str, Any]:
    g = lgraph.build(lgraph.read_events(root))
    known = {d["id"]: d for d in catalog["datasets"]}

    def describe(did: str) -> dict[str, Any]:
        node = dict(g.datasets[did])
        try:
            m = meta.dataset(did)
            node.update(title=m.title, layer=m.layer, domain=m.domain)
        except KeyError:
            m = None
        if did in known:
            node.update(title=known[did]["title"], layer=known[did]["layer"])
        node["kind"] = "dataset"
        node["fields"] = sorted(g.fields.get(did, set()))
        node["cataloged"] = m is not None or did in known
        return node

    published = {
        d
        for d in g.datasets
        if d.startswith(naming.ICEBERG_NS + "/")
        and d.rsplit("/", 1)[1].split(".")[0] in PUBLISHED_NAMESPACES
    }
    hours = {d for d in g.datasets if lgraph.is_hour_file(d)}
    # The gate path: an hourly file to a published table (table level), and the published
    # field whose column lineage reaches furthest back along that same path.
    path: list[str] = []
    field_path: list[str] = []
    best_key = (-1, 0)
    for target in sorted(published):
        tpath = lgraph.shortest_path(g, hours, {target})
        if not tpath:
            continue
        on_path = set(tpath)
        for col in sorted(g.fields.get(target, set())):
            chain = [f"{target}#{col}", *lgraph.field_ancestry(g, f"{target}#{col}")]
            reach = sum(1 for f in chain if f.split("#", 1)[0] in on_path)
            key = (reach, -len(tpath))
            if key > best_key:
                best_key, path, field_path = key, tpath, chain
    # The full engine's branch (Kafka topic, Flink job, Iceberg rollup), recorded beside it.
    full = {
        d
        for d in g.datasets
        if d.startswith(naming.ICEBERG_NS + "/") and d not in published and d.endswith(".repo_hour")
    }
    full_path = lgraph.shortest_path(g, hours, full)
    nodes = len(g.datasets) + len(g.jobs)
    return {
        "datasets": [describe(d) for d in sorted(g.datasets)],
        "jobs": sorted(g.jobs.values(), key=lambda j: j["id"]),
        "edges": [{"from": a, "to": b, "kind": k} for a, b, k in sorted(g.edges)],
        "field_edges": [{"from": a, "to": b, "job": j} for a, b, j in sorted(g.field_edges)],
        "tools": {t: dict(c) for t, c in sorted(g.tools.items())},
        "unresolved_column_inputs": g.unresolved,
        "hour_files_kept": len(hours),
        "gate_path": {"tables": path, "field": field_path},
        "full_engine_path": full_path,
        "summary": {
            "nodes": nodes,
            "edges": len(g.edges),
            "field_edges": len(g.field_edges),
            "datasets": len(g.datasets),
            "jobs": len(g.jobs),
        },
    }


def write_export(root: Path, out_dir: Path, generated_contracts: dict[str, str]) -> dict[str, Any]:
    """Write every export file; return a summary (counts, completeness, gate path)."""
    from keystone.warehouse.dbt import WarehousePaths

    meta = Metadata()
    catalog = build_catalog(meta, WarehousePaths(root).target_dir)
    comp = completeness(catalog)
    lineage = build_lineage(root, meta, catalog)
    now = datetime.now(UTC).isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = {"generated_at": now, "generator": "keystone metadata export"}
    catalog_doc = {
        **stamp,
        "completeness": {**asdict(comp), "documented_pct": round(comp.documented_pct * 100, 2)},
        **catalog,
    }
    files = {
        "catalog.json": catalog_doc,
        "glossary.json": {**stamp, **build_glossary(meta, catalog)},
        "lineage.json": {**stamp, **lineage},
    }
    for name, doc in files.items():
        (out_dir / name).write_text(json.dumps(doc, indent=1, sort_keys=False) + "\n", "utf-8")
    cdir = out_dir / "contracts"
    cdir.mkdir(exist_ok=True)
    for name, text in generated_contracts.items():
        (cdir / name).write_text(text, "utf-8", newline="\n")
    for path in sorted(CONTRACTS_DIR.glob("*.odcs.yaml")):
        shutil.copyfile(path, cdir / path.name)
    shutil.copyfile(CONTRACTS_DIR / "avro" / "gharchive_event.avsc", cdir / "gharchive_event.avsc")
    return {"completeness": comp, "lineage": lineage["summary"], "gate_path": lineage["gate_path"]}
