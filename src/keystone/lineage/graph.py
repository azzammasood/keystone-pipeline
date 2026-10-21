"""The lineage graph, built from the OpenLineage event log (CLI, dbt-ol, Spark, Flink).

Nodes are datasets, jobs and fields (`<dataset id>#<column>`); edges are `reads` (dataset to
job), `writes` (job to dataset) and `derives` (field to field, through a job). Edges come from
every event except FAIL and ABORT (openlineage-flink reports its datasets on START and sent no
COMPLETE in our batch runs); a job's run count counts COMPLETE events only. A dataset reported
with a symlink into the Keystone scheme is merged into the symlinked id. Column inputs that
name no known dataset (a CTE or a table function the SQL parser could not resolve) are dropped
and counted, never guessed.

Hourly source files: a backfill touches tens of thousands, so the graph keeps the newest
`MAX_HOUR_FILES` as nodes and reports the total.
"""

import gzip
from collections import Counter, defaultdict, deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson

from keystone.lineage import naming
from keystone.lineage.emit import events_dir

MAX_HOUR_FILES = 200

# Producer URL fragment -> tool name (record of which tool emitted what).
TOOLS = {
    "keystone-pipeline": "keystone CLI",
    "openlineage-dbt": "dbt (dbt-ol)",
    "integration/dbt": "dbt (dbt-ol)",
    "integration/spark": "Spark (openlineage-spark)",
    "integration/flink": "Flink (openlineage-flink)",
    "airflow": "Airflow",
}


def tool_of(producer: str) -> str:
    for frag, name in TOOLS.items():
        if frag in producer:
            return name
    return producer


def read_events(root: Path) -> Iterator[dict[str, Any]]:
    d = events_dir(root)
    for path in sorted(d.glob("*.jsonl")) + sorted(d.glob("*.jsonl.gz")):
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rb") as fh:
            for line in fh:
                if line.strip():
                    yield orjson.loads(line)


def ds_id(namespace: str, name: str) -> str:
    """`<namespace>/<name>`; a name that is a path (`/2026-10-19-12.json.gz`) keeps one slash."""
    return f"{namespace}{name}" if name.startswith("/") else f"{namespace}/{name}"


@dataclass
class Graph:
    datasets: dict[str, dict[str, Any]] = field(default_factory=dict)
    jobs: dict[str, dict[str, Any]] = field(default_factory=dict)
    fields: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    edges: set[tuple[str, str, str]] = field(default_factory=set)  # (from, to, kind)
    field_edges: set[tuple[str, str, str]] = field(default_factory=set)  # (from, to, job)
    unresolved: int = 0
    tools: dict[str, dict[str, int]] = field(default_factory=lambda: defaultdict(Counter))

    def _dataset(self, d: dict[str, Any]) -> str:
        ns, name = naming.canonical(d["namespace"], d["name"])
        for ident in (d.get("facets") or {}).get("symlinks", {}).get("identifiers", []) or []:
            sns, sname = ident["namespace"], ident["name"]
            if sns.startswith(("iceberg://", naming.DUCKDB_NS, naming.FILE_NS)):
                ns, name = sns, sname
                break
        did = ds_id(ns, name)
        self.datasets.setdefault(did, {"id": did, "namespace": ns, "name": name})
        schema = (d.get("facets") or {}).get("schema", {}).get("fields") or []
        for f in schema:
            self.fields[did].add(f["name"])
        return did

    def add(self, event: dict[str, Any]) -> None:
        state = event.get("eventType")
        if state in ("FAIL", "ABORT"):
            return
        job = event["job"]
        jid = f"job:{job['namespace']}/{job['name']}"
        tool = tool_of(str(event.get("producer", "")))
        if not (event.get("inputs") or event.get("outputs")) and jid not in self.jobs:
            return  # an application-level event without datasets (Spark's for a read)
        node = self.jobs.setdefault(jid, {"id": jid, "name": job["name"], "tool": tool, "runs": 0})
        if state == "COMPLETE":
            node["runs"] += 1
            node["last_run"] = event["eventTime"]
        for d in event.get("inputs") or []:
            self.edges.add((self._dataset(d), jid, "reads"))
            self.tools[tool]["table_edges"] += 1
        for d in event.get("outputs") or []:
            out = self._dataset(d)
            self.edges.add((jid, out, "writes"))
            self.tools[tool]["table_edges"] += 1
            cll = (d.get("facets") or {}).get("columnLineage", {}).get("fields", {})
            for col, spec in cll.items():
                self.fields[out].add(col)
                for f in spec.get("inputFields", []):
                    src = ds_id(*naming.canonical(f["namespace"], f["name"]))
                    self.field_edges.add((f"{src}#{f['field']}", f"{out}#{col}", jid))
                    self.tools[tool]["column_edges"] += 1

    def resolve(self) -> None:
        """Drop column edges whose input dataset is unknown (counted in `unresolved`)."""
        kept = set()
        for src, dst, job in self.field_edges:
            if src.split("#", 1)[0] in self.datasets:
                kept.add((src, dst, job))
                self.fields[src.split("#", 1)[0]].add(src.split("#", 1)[1])
            else:
                self.unresolved += 1
        self.field_edges = kept


def build(events: Iterable[dict[str, Any]]) -> Graph:
    g = Graph()
    for e in events:
        g.add(e)
    g.resolve()
    _cap_hour_files(g)
    return g


def _cap_hour_files(g: Graph) -> None:
    hours = sorted(d for d in g.datasets if d.startswith(naming.GHARCHIVE_NS + "/"))
    if len(hours) <= MAX_HOUR_FILES:
        return
    drop = set(hours[:-MAX_HOUR_FILES])
    for d in drop:
        g.datasets.pop(d, None)
    g.edges = {e for e in g.edges if e[0] not in drop}


def is_hour_file(did: str) -> bool:
    return did.startswith(naming.GHARCHIVE_NS + "/")


def shortest_path(g: Graph, sources: set[str], targets: set[str]) -> list[str]:
    """Shortest dataset/job path from any source to any target (table-level edges)."""
    adj: dict[str, list[str]] = defaultdict(list)
    for a, b, _ in g.edges:
        adj[a].append(b)
    prev: dict[str, str | None] = {s: None for s in sources}
    queue = deque(sorted(sources))
    while queue:
        cur = queue.popleft()
        if cur in targets:
            path = [cur]
            while (p := prev[path[-1]]) is not None:
                path.append(p)
            return path[::-1]
        for nxt in sorted(adj[cur]):
            if nxt not in prev:
                prev[nxt] = cur
                queue.append(nxt)
    return []


def field_ancestry(g: Graph, target: str) -> list[str]:
    """Fields a field derives from, transitively (nearest first)."""
    parents: dict[str, list[str]] = defaultdict(list)
    for src, dst, _ in g.field_edges:
        parents[dst].append(src)
    seen, out, queue = {target}, [], deque([target])
    while queue:
        for p in sorted(parents[queue.popleft()]):
            if p not in seen:
                seen.add(p)
                out.append(p)
                queue.append(p)
    return out
