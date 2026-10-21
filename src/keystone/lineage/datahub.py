"""DataHub: catalog, glossary, owners, domains, tags, assertions and lineage (brief 9b).

Two parts, both with the `acryl-datahub` library (the `metadata` dependency group):

1. Source recipes (DataHub's own ingestion): `dbt` (models, sources, column lineage parsed
   from the compiled SQL, tests and their last results as assertions), `iceberg` (the
   Lakekeeper REST catalog) and `kafka` (the Redpanda topic and its registry schema).
2. Keystone's own metadata, emitted as aspects: the domains, the owner group, the glossary
   (config/metadata.yml), tags for layer, classification and freshness SLA, glossary terms on
   columns, a link to each dataset's contract, and every non-dbt job of the lineage graph
   (CLI, Flink) as a DataJob with its inputs, outputs and column lineage (the rollup's from
   sqlglot, lineage/sqllineage.py).

Dataset URNs follow what the sources create, so both parts land on the same entities.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from keystone.export.meta import Metadata
from keystone.lineage import naming

GMS = "http://127.0.0.1:8080"
FLOW_ID = "keystone"
GLOSSARY_NODE = "keystone"
GITHUB = "https://github.com/azzammasood/keystone-pipeline/blob/main/"


def dataset_urn(did: str) -> str:
    """Keystone dataset id -> the DataHub URN the sources use for it."""
    from datahub.emitter.mce_builder import make_dataset_urn

    def ds(platform: str, name: str) -> str:
        return make_dataset_urn(platform, name, "PROD")

    for prefix, platform in (
        (naming.DUCKDB_NS + "/", "duckdb"),
        (naming.KAFKA_NS + "/", "kafka"),
        (naming.ICEBERG_NS + "/", "iceberg"),
    ):
        if did.startswith(prefix):
            return ds(platform, did.removeprefix(prefix))
    if did.startswith(naming.FILE_NS + "/"):
        rel = did.removeprefix(naming.FILE_NS + "/")
        for source, path in naming.SILVER_SOURCES.items():
            if rel == path:  # the dbt source over this silver table
                return ds("duckdb", f"keystone.silver.{source}")
        return ds("file", f"keystone/{rel}")
    for ns in (naming.GHARCHIVE_NS, naming.OSV_NS):
        if did.startswith(ns):
            return ds("file", did.removeprefix("https://"))
    raise ValueError(f"no DataHub mapping for {did}")


def recipes(root: Path, s3: dict[str, str], kafka: bool) -> dict[str, dict[str, Any]]:
    from keystone.warehouse.dbt import WarehousePaths

    t = WarehousePaths(root).target_dir
    out: dict[str, dict[str, Any]] = {
        "dbt": {
            "type": "dbt",
            "config": {
                "manifest_path": str(t / "manifest.json"),
                "catalog_path": str(t / "catalog.json"),
                "run_results_paths": [str(t / "run_results.json")],
                "target_platform": "duckdb",
                "include_column_lineage": True,
                "env": "PROD",
            },
        },
        "iceberg": {
            "type": "iceberg",
            "config": {
                "catalog": {
                    "ice": {
                        "type": "rest",
                        "uri": "http://127.0.0.1:8181/catalog",
                        "warehouse": "keystone",
                        "s3.endpoint": "http://127.0.0.1:9000",
                        "s3.region": "local-01",
                        "s3.path-style-access": "true",
                        **s3,
                    }
                }
            },
        },
    }
    if kafka:
        out["kafka"] = {
            "type": "kafka",
            "config": {
                "connection": {
                    "bootstrap": "127.0.0.1:19092",
                    "schema_registry_url": "http://127.0.0.1:18081",
                },
                "topic_patterns": {"allow": [r"keystone\..*"]},
            },
        }
    return out


@dataclass
class IngestReport:
    events: dict[str, int] = field(default_factory=dict)
    failures: dict[str, int] = field(default_factory=dict)


def run_sources(root: Path, s3: dict[str, str], kafka: bool, gms: str = GMS) -> IngestReport:
    from datahub.ingestion.run.pipeline import Pipeline

    report = IngestReport()
    sink = {"type": "datahub-rest", "config": {"server": gms}}
    for name, source in recipes(root, s3, kafka).items():
        p = Pipeline.create({"source": source, "sink": sink})
        p.run()
        src = p.source.get_report()
        report.events[name] = int(src.events_produced)
        report.failures[name] = len(src.failures)
    return report


def _audit() -> Any:
    from datahub.metadata.schema_classes import AuditStampClass

    return AuditStampClass(time=0, actor="urn:li:corpuser:datahub")


def _term_urn(term: str) -> str:
    from datahub.emitter.mce_builder import make_term_urn

    return make_term_urn(f"{GLOSSARY_NODE}.{term.replace(' ', '_')}")


def emit(
    catalog: dict[str, Any], lineage: dict[str, Any], meta: Metadata, gms: str = GMS
) -> dict[str, int]:
    """Emit Keystone's metadata and non-dbt lineage; return counts of what was written."""
    from datahub.emitter.mce_builder import (
        make_data_flow_urn,
        make_data_job_urn,
        make_domain_urn,
        make_group_urn,
        make_schema_field_urn,
        make_tag_urn,
    )
    from datahub.emitter.mcp import MetadataChangeProposalWrapper as Mcp
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata import schema_classes as s

    emitter = DatahubRestEmitter(gms)
    counts: dict[str, int] = defaultdict(int)

    def put(urn: str, aspect: Any, kind: str) -> None:
        emitter.emit(Mcp(entityUrn=urn, aspect=aspect))
        counts[kind] += 1

    owner = make_group_urn(meta.owner_id)
    put(
        owner,
        s.CorpGroupInfoClass(
            displayName=meta.owner_id,
            description=meta.raw["owner"]["description"],
            admins=[],
            members=[],
            groups=[],
        ),
        "owner",
    )
    for key, desc in meta.domains.items():
        put(make_domain_urn(key), s.DomainPropertiesClass(name=key, description=desc), "domains")
    node = f"urn:li:glossaryNode:{GLOSSARY_NODE}"
    put(node, s.GlossaryNodeInfoClass(definition="Keystone terms", name="Keystone"), "glossary")
    for term, v in meta.glossary.items():
        put(
            _term_urn(term),
            s.GlossaryTermInfoClass(
                definition=" ".join(str(v["definition"]).split()),
                name=term,
                termSource="INTERNAL",
                parentNode=node,
                customProperties={"status": str(v["status"])},
            ),
            "glossary",
        )

    # Datasets: the catalog's concrete ids plus the graph's (templates match graph ids).
    ids = {d["id"] for d in catalog["datasets"] if "{" not in d["id"]}
    ids |= {d["id"] for d in lineage["datasets"]}
    columns = {d["id"]: d["columns"] for d in catalog["datasets"]}
    for did in sorted(ids):
        try:
            m = meta.dataset(did)
            urn = dataset_urn(did)
        except (KeyError, ValueError):
            counts["datasets_uncataloged"] += 1
            continue
        put(urn, s.StatusClass(removed=False), "datasets")
        put(
            urn,
            s.OwnershipClass(
                owners=[s.OwnerClass(owner=owner, type=s.OwnershipTypeClass.TECHNICAL_OWNER)]
            ),
            "ownership",
        )
        put(urn, s.DomainsClass(domains=[make_domain_urn(m.domain)]), "domain_links")
        tags = [f"layer:{m.layer}", f"classification:{m.classification}"]
        tags.append(f"freshness-sla:{m.freshness_sla_hours}h")
        put(
            urn,
            s.GlobalTagsClass(tags=[s.TagAssociationClass(tag=make_tag_urn(t)) for t in tags]),
            "tags",
        )
        terms = meta.layer_terms(m.layer)
        if terms:
            put(
                urn,
                s.GlossaryTermsClass(
                    terms=[s.GlossaryTermAssociationClass(urn=_term_urn(t)) for t in terms],
                    auditStamp=_audit(),
                ),
                "terms",
            )
        if not urn.startswith(("urn:li:dataset:(urn:li:dataPlatform:duckdb,keystone.",)):
            put(urn, s.EditableDatasetPropertiesClass(description=m.description), "descriptions")
        if m.contract:
            put(
                urn,
                s.InstitutionalMemoryClass(
                    elements=[
                        s.InstitutionalMemoryMetadataClass(
                            url=GITHUB + m.contract,
                            description="Data contract",
                            createStamp=_audit(),
                        )
                    ]
                ),
                "contracts",
            )
        fields = [
            s.EditableSchemaFieldInfoClass(
                fieldPath=c["name"],
                glossaryTerms=s.GlossaryTermsClass(
                    terms=[
                        s.GlossaryTermAssociationClass(urn=_term_urn(t))
                        for t in c["glossary_terms"]
                    ],
                    auditStamp=_audit(),
                ),
            )
            for c in columns.get(did, [])
            if c.get("glossary_terms")
        ]
        if fields:
            put(urn, s.EditableSchemaMetadataClass(editableSchemaFieldInfo=fields), "column_terms")

    # Non-dbt jobs (dbt's model lineage comes from the dbt source) as DataJobs.
    flow = make_data_flow_urn("keystone", FLOW_ID, "PROD")
    put(flow, s.DataFlowInfoClass(name="keystone pipeline"), "jobs")
    reads: dict[str, list[str]] = defaultdict(list)
    writes: dict[str, list[str]] = defaultdict(list)
    for e in lineage["edges"]:
        if e["kind"] == "reads":
            reads[e["to"]].append(e["from"])
        else:
            writes[e["from"]].append(e["to"])
    field_edges: dict[str, list[dict[str, str]]] = defaultdict(list)
    for fe in lineage["field_edges"]:
        field_edges[fe["job"]].append(fe)
    for job in lineage["jobs"]:
        if job["tool"] == "dbt (dbt-ol)":
            continue
        jurn = make_data_job_urn("keystone", FLOW_ID, job["name"], "PROD")
        try:
            ins = [dataset_urn(d) for d in reads[job["id"]]]
            outs = [dataset_urn(d) for d in writes[job["id"]]]
        except ValueError:
            counts["jobs_unmapped"] += 1
            continue
        fine = []
        for fe in field_edges[job["id"]]:
            (src, scol), (dst, dcol) = fe["from"].split("#", 1), fe["to"].split("#", 1)
            fine.append((dataset_urn(src), scol, dataset_urn(dst), dcol))
        put(
            jurn,
            s.DataJobInfoClass(
                name=job["name"],
                type="COMMAND",
                flowUrn=flow,
                customProperties={"tool": job["tool"]},
            ),
            "jobs",
        )
        put(
            jurn,
            s.DataJobInputOutputClass(
                inputDatasets=sorted(set(ins)),
                outputDatasets=sorted(set(outs)),
                fineGrainedLineages=[
                    s.FineGrainedLineageClass(
                        upstreamType=s.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[make_schema_field_urn(su, sc)],
                        downstreamType=s.FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[make_schema_field_urn(du, dc)],
                    )
                    for su, sc, du, dc in sorted(set(fine))
                ],
            ),
            "job_lineage",
        )
        counts["column_edges"] += len(set(fine))
    emitter.close()
    return dict(counts)


def job_urns(lineage: dict[str, Any]) -> set[str]:
    """DataJob URNs `emit` writes: every non-dbt job of the lineage graph."""
    from datahub.emitter.mce_builder import make_data_job_urn

    return {
        make_data_job_urn("keystone", FLOW_ID, j["name"], "PROD")
        for j in lineage["jobs"]
        if j["tool"] != "dbt (dbt-ol)"
    }


def _gql(gms: str, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    import httpx

    r = httpx.post(f"{gms}/api/graphql", json={"query": query, "variables": variables}, timeout=60)
    r.raise_for_status()
    body: dict[str, Any] = r.json()
    if body.get("errors"):
        raise RuntimeError(str(body["errors"])[:500])
    data: dict[str, Any] = body["data"]
    return data


def upstream(gms: str, urn: str) -> set[str]:
    """URNs of everything upstream of `urn` (datasets and jobs, any distance)."""
    q = """query($u:String!){ searchAcrossLineage(input:{urn:$u, direction:UPSTREAM, query:"*",
      start:0, count:1000, orFilters:[{and:[{field:"degree", values:["1","2","3+"]}]}]})
      { total searchResults { entity { urn } } } }"""
    data = _gql(gms, q, {"u": urn})["searchAcrossLineage"]
    return {r["entity"]["urn"] for r in data["searchResults"]}


def _aliases(urn: str) -> set[str]:
    """A dbt node is two sibling entities in DataHub: the dbt one and the target-platform one;
    lineage search may return either."""
    return {urn, urn.replace("urn:li:dataPlatform:duckdb,", "urn:li:dataPlatform:dbt,")}


def gate_missing(gms: str, path: list[str], wait_s: float = 120.0) -> list[str]:
    """Datasets of the gate path not (yet) upstream of its last dataset in DataHub. Lineage is
    indexed asynchronously, so the check retries until `wait_s` has passed."""
    import time

    target = dataset_urn(path[-1])
    deadline = time.monotonic() + wait_s
    while True:
        ups = upstream(gms, target)
        missing = [d for d in path[:-1] if not _aliases(dataset_urn(d)) & ups]
        if not missing or time.monotonic() > deadline:
            return missing
        time.sleep(5)


def field_urn(field_id: str) -> str:
    """`<dataset id>#<column>` -> the DataHub schemaField URN."""
    from datahub.emitter.mce_builder import make_schema_field_urn

    did, col = field_id.split("#", 1)
    return make_schema_field_urn(dataset_urn(did), col)


def _fine_grained_edges(gms: str, datasets: list[str], jobs: list[str]) -> set[tuple[str, str]]:
    """(upstream field URN, downstream field URN) stored in DataHub: fine-grained lineage of
    the datasets' UpstreamLineage (dbt source, both siblings) and of the jobs' inputs/outputs."""
    from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
    from datahub.metadata.schema_classes import DataJobInputOutputClass, UpstreamLineageClass

    graph = DataHubGraph(DatahubClientConfig(server=gms))
    fgl = []
    for urn in datasets:
        for alias in _aliases(urn):
            ul = graph.get_aspect(alias, UpstreamLineageClass)
            fgl += (ul.fineGrainedLineages or []) if ul else []
    for urn in jobs:
        io = graph.get_aspect(urn, DataJobInputOutputClass)
        fgl += (io.fineGrainedLineages or []) if io else []
    return {(u, d) for f in fgl for u in (f.upstreams or []) for d in (f.downstreams or [])}


def field_gate_missing(gms: str, chain: list[str], jobs: list[str]) -> list[str]:
    """Hops of the gate's column chain (`downstream <- upstream`) with no fine-grained lineage
    edge in DataHub (either sibling of a dbt node counts)."""
    datasets = sorted({dataset_urn(f.split("#", 1)[0]) for f in chain})
    edges = _fine_grained_edges(gms, datasets, jobs)
    missing = []
    for down, up in zip(chain, chain[1:], strict=False):
        ups, downs = _aliases(field_urn(up)), _aliases(field_urn(down))
        if not any((u, d) in edges for u in ups for d in downs):
            missing.append(f"{down} <- {up}")
    return missing


def remove_stale_jobs(gms: str, current: set[str]) -> list[str]:
    """Soft-delete DataJobs of the keystone flow that the current lineage no longer has (a
    renamed job would otherwise keep its old edges in DataHub)."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper as Mcp
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import StatusClass

    q = """query{ searchAcrossEntities(input:{types:[DATA_JOB], query:"*", start:0, count:1000})
      { searchResults { entity { urn } } } }"""
    found = {r["entity"]["urn"] for r in _gql(gms, q, {})["searchAcrossEntities"]["searchResults"]}
    stale = sorted(u for u in found if f"({FLOW_ID},{FLOW_ID}," in u and u not in current)
    emitter = DatahubRestEmitter(gms)
    for urn in stale:
        emitter.emit(Mcp(entityUrn=urn, aspect=StatusClass(removed=True)))
    emitter.close()
    return stale


def count(gms: str, entity_type: str) -> int:
    q = """query($t:EntityType!){ searchAcrossEntities(input:{types:[$t], query:"*", start:0,
      count:1}) { total } }"""
    return int(_gql(gms, q, {"t": entity_type})["searchAcrossEntities"]["total"])


def version(gms: str = GMS) -> str:
    import httpx

    try:
        cfg = httpx.get(f"{gms}/config", timeout=10).json()
        return str(cfg["versions"]["acryldata/datahub"]["version"])
    except (httpx.HTTPError, KeyError, ValueError):
        return "unknown"
