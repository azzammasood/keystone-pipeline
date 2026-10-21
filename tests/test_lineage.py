"""Lineage: canonical names, the event log, the graph and its gate path (Phase 6)."""

from pathlib import Path
from typing import Any

import orjson
import pytest
import yaml

from keystone.lineage import graph as lgraph
from keystone.lineage import naming
from keystone.lineage.dbt import canonicalise
from keystone.lineage.emit import Ds, LineageRun, events_dir
from keystone.paths import REPO_ROOT


def test_silver_sources_match_dbt() -> None:
    """naming.SILVER_SOURCES maps every dbt silver source to the directory it reads."""
    spec = yaml.safe_load((REPO_ROOT / "dbt/models/staging/_sources.yml").read_text("utf-8"))
    silver = next(s for s in spec["sources"] if s["name"] == "silver")
    for table in silver["tables"]:
        location = table["meta"]["external_location"]
        assert f"/{naming.SILVER_SOURCES[table['name']]}/" in location
    assert set(naming.SILVER_SOURCES) == {t["name"] for t in silver["tables"]}


@pytest.mark.parametrize(
    ("ns", "name", "expected"),
    [
        (
            "duckdb:///data/warehouse/keystone.duckdb",
            "keystone.marts.dim_repo",
            (naming.DUCKDB_NS, "keystone.marts.dim_repo"),
        ),
        (
            "duckdb://C:/x/keystone.duckdb",
            "keystone.silver.gharchive_events",
            (naming.FILE_NS, "silver/gharchive/events"),
        ),
        ("kafka://redpanda:9092", "t", (naming.KAFKA_NS, "t")),
        ("", "ice.rollup_full.repo_hour", (naming.ICEBERG_NS, "rollup_full.repo_hour")),
        (
            "https://data.gharchive.org",
            "/2026-10-19-12.json.gz",
            ("https://data.gharchive.org", "/2026-10-19-12.json.gz"),
        ),
    ],
)
def test_canonical(ns: str, name: str, expected: tuple[str, str]) -> None:
    assert naming.canonical(ns, name) == expected


def test_canonicalise_records_the_rewrite() -> None:
    ev: dict[str, Any] = {
        "inputs": [{"namespace": "duckdb:///d/keystone.duckdb", "name": "rollup.main.repo_hour"}],
        "outputs": [
            {
                "namespace": "duckdb:///d/keystone.duckdb",
                "name": "keystone.staging.stg_rollup__repo_hour",
                "facets": {
                    "columnLineage": {
                        "fields": {
                            "events": {
                                "inputFields": [
                                    {
                                        "namespace": "duckdb:///d/keystone.duckdb",
                                        "name": "rollup.main.repo_hour",
                                        "field": "events",
                                    }
                                ]
                            }
                        }
                    }
                },
            }
        ],
        "run": {"runId": "x"},
    }
    out = canonicalise(ev)
    assert out["inputs"][0]["namespace"] == naming.DUCKDB_NS
    assert out["run"]["facets"]["keystone_canonical"]["rewritten_identifiers"] == 3


def _events(root: Path) -> list[dict[str, Any]]:
    return [
        orjson.loads(line)
        for p in sorted(events_dir(root).glob("*.jsonl"))
        for line in p.read_bytes().splitlines()
    ]


def test_lineage_run_writes_start_and_complete(tmp_path: Path) -> None:
    src = Ds(*naming.duck("keystone.marts.fct_repo_day"), columns=[("events", "BIGINT")])
    out = Ds(
        *naming.iceberg("marts.fct_repo_day"),
        columns=[("events", "BIGINT")],
        lineage={"events": [(src.namespace, src.name, "events")]},
        identity=True,
    )
    LineageRun("warehouse.publish.marts.fct_repo_day", [src], [out], tmp_path).finish(True)
    start, done = _events(tmp_path)
    assert (start["eventType"], done["eventType"]) == ("START", "COMPLETE")
    assert start["run"]["runId"] == done["run"]["runId"]
    cll = done["outputs"][0]["facets"]["columnLineage"]["fields"]["events"]
    assert cll["transformationType"] == "IDENTITY"
    assert done["run"]["facets"]["parent"]["job"]["name"] == "pipeline"


def _event(
    job: str, ins: list[str], outs: list[tuple[str, dict[str, Any]]], state: str = "COMPLETE"
) -> dict[str, Any]:
    def ds(did: str, facets: dict[str, Any] | None = None) -> dict[str, Any]:
        for ns in (naming.ICEBERG_NS, naming.DUCKDB_NS, naming.FILE_NS, naming.KAFKA_NS):
            if did.startswith(ns + "/"):
                return {"namespace": ns, "name": did.removeprefix(ns + "/"), "facets": facets or {}}
        ns = naming.GHARCHIVE_NS
        return {"namespace": ns, "name": did.removeprefix(ns), "facets": facets or {}}

    return {
        "eventType": state,
        "eventTime": "2026-10-21T00:00:00+00:00",
        "producer": "https://github.com/azzammasood/keystone-pipeline/tree/v0.1.0",
        "job": {"namespace": "keystone", "name": job},
        "run": {"runId": job},
        "inputs": [ds(i) for i in ins],
        "outputs": [ds(o, f) for o, f in outs],
    }


HOUR = naming.GHARCHIVE_NS + "/2026-10-19-12.json.gz"
SILVER = naming.FILE_NS + "/silver/gharchive/events"
ROLLUP = naming.DUCKDB_NS + "/rollup.main.repo_hour"
MART = naming.DUCKDB_NS + "/keystone.marts.fct_repo_day"
PUB = naming.ICEBERG_NS + "/marts.fct_repo_day"


def _cll(src: str, col: str) -> dict[str, Any]:
    for prefix in (naming.DUCKDB_NS, naming.ICEBERG_NS):
        if src.startswith(prefix + "/"):
            return {
                "columnLineage": {
                    "fields": {
                        col: {
                            "inputFields": [
                                {
                                    "namespace": prefix,
                                    "name": src.removeprefix(prefix + "/"),
                                    "field": col,
                                },
                                {"namespace": naming.DUCKDB_NS, "name": "a_cte", "field": col},
                            ]
                        }
                    }
                }
            }
    raise ValueError(src)


def test_graph_gate_path_and_unresolved_columns() -> None:
    events = [
        _event("parse.gharchive", [HOUR], [(SILVER, {})]),
        _event("rollup.duckdb", [SILVER], [(ROLLUP, {})]),
        _event("dbt.fct_repo_day", [ROLLUP], [(MART, _cll(ROLLUP, "events"))]),
        _event("warehouse.publish.marts.fct_repo_day", [MART], [(PUB, _cll(MART, "events"))]),
        _event("failed", [PUB], [(HOUR, {})], state="FAIL"),  # never an edge
    ]
    g = lgraph.build(events)
    path = lgraph.shortest_path(g, {HOUR}, {PUB})
    assert path[0] == HOUR and path[-1] == PUB and len(path) == 9
    assert lgraph.field_ancestry(g, f"{PUB}#events") == [f"{MART}#events", f"{ROLLUP}#events"]
    # the chain along the path stops at the rollup: the parse hop has no column lineage here
    chain = lgraph.field_chain_along(g, path, f"{PUB}#events")
    assert chain == [f"{PUB}#events", f"{MART}#events", f"{ROLLUP}#events"]
    assert g.unresolved == 2  # the CTE inputs, dropped and counted
    assert not lgraph.shortest_path(g, {PUB}, {HOUR})


def test_graph_takes_datasets_from_start_events() -> None:
    """openlineage-flink reported its datasets on START only; edges still count, runs do not."""
    g = lgraph.build([_event("flink", [SILVER], [(ROLLUP, {})], state="START")])
    assert (SILVER, "job:keystone/flink", "reads") in g.edges
    assert g.jobs["job:keystone/flink"]["runs"] == 0


def test_hour_ids_keep_one_slash() -> None:
    assert lgraph.ds_id(naming.GHARCHIVE_NS, "/2026-10-19-12.json.gz") == HOUR
    assert "//" not in HOUR.removeprefix("https://")


def test_compaction_caps_and_summarises(tmp_path: Path) -> None:
    from datetime import date

    from keystone.lineage.retention import compact

    d = events_dir(tmp_path)
    d.mkdir(parents=True)
    for day in ("2026-10-18", "2026-10-19", "2026-10-21"):
        ev = _event("parse.gharchive", [HOUR], [(SILVER, {})])
        ev["eventTime"] = f"{day}T01:00:00+00:00"
        (d / f"{day}.jsonl").write_bytes(orjson.dumps(ev) + b"\n")
    c = compact(tmp_path, max_bytes=1, today=date(2026, 10, 21))
    assert (c.compressed, c.summarised) == (2, 2)
    assert [p.name for p in sorted(d.iterdir())] == ["2026-10-21.jsonl"]  # today stays plain
    rows = [
        orjson.loads(x) for x in (tmp_path / "lineage" / "summary.jsonl").read_bytes().splitlines()
    ]
    assert [r["day"] for r in rows] == ["2026-10-18", "2026-10-19"]
    assert rows[0]["jobs"]["keystone/parse.gharchive"] == {"events": 1, "runs": 1, "datasets": 2}


def test_hour_cap_keeps_the_newest_hours(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lgraph, "MAX_HOUR_FILES", 1)
    names = ["/2026-10-19-9.json.gz", "/2026-10-19-23.json.gz"]
    events = [
        {
            **_event("p", [], [(SILVER, {})]),
            "inputs": [{"namespace": naming.GHARCHIVE_NS, "name": n}],
        }
        for n in names
    ]
    g = lgraph.build(events)
    assert [d for d in g.datasets if lgraph.is_hour_file(d)] == [naming.GHARCHIVE_NS + names[1]]


def test_rollup_sql_column_lineage() -> None:
    from keystone.lineage.sqllineage import rollup_columns
    from keystone.rollup.spec import rollups

    cols = rollup_columns(next(r for r in rollups() if r.name == "repo_hour"))
    assert cols["window_start"] == ["created_at"]
    assert cols["merged"] == ["action", "pr_merged"]
    assert "events" not in cols  # a constant: no source column
