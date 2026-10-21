"""Metadata export, generated contracts, run log and pipeline_metrics.json (Phase 6).

One fixture run through the CLI (parse, rollup, OSV, dbt build, export, metrics) checks the
pieces together on the committed fixtures, as the CI `metadata` job does.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import jsonschema
import pytest
from typer.testing import CliRunner

from keystone.cli import app
from keystone.export.meta import Metadata
from keystone.paths import REPO_ROOT
from keystone.telemetry import grafana as gf
from keystone.telemetry import metrics as tm
from keystone.telemetry.runlog import Stage, read

FIXTURES = REPO_ROOT / "tests" / "fixtures"
runner = CliRunner()


def _ok(*args: str) -> str:
    result = runner.invoke(app, list(args))
    assert result.exit_code == 0, result.output
    return result.output


@pytest.fixture(scope="module")
def run(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("meta") / "data"
    out = root.parent / "metadata"
    mp = pytest.MonkeyPatch()
    mp.setenv("KEYSTONE_ACTOR_SALT", "test-salt-not-a-secret")
    mp.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    mp.delenv("OPENLINEAGE_URL", raising=False)
    mp.setenv("KEYSTONE_RUN_ID", "01a15d00-0000-7000-8000-000000000000")
    hours = sorted(str(p) for p in (FIXTURES / "gharchive").glob("*.json.gz"))
    _ok("parse", "files", *hours, "--root", str(root))
    _ok("rollup", "run", "--root", str(root))
    _ok("parse", "osv", "--zip", str(FIXTURES / "osv" / "PyPI" / "all.zip"), "--root", str(root))
    _ok("warehouse", "build", "--root", str(root))
    _ok("metadata", "export", "--root", str(root), "--out", str(out))
    _ok("metrics", "emit", "--root", str(root), "--metadata-dir", str(out))
    mp.undo()
    return root, out


def test_committed_contracts_are_current(run: tuple[Path, Path]) -> None:
    """metadata/contracts matches what the fixtures' schemas and dbt docs generate."""
    root, _ = run
    assert "out_of_date=0" in _ok("metadata", "contracts", "--root", str(root), "--check")


def test_export_completeness_and_privacy(run: tuple[Path, Path]) -> None:
    _, out = run
    catalog = json.loads((out / "catalog.json").read_text("utf-8"))
    comp = catalog["completeness"]
    assert comp["owners_missing"] == 0
    assert comp["datasets_complete"] == comp["datasets"]
    assert comp["documented_pct"] >= 95
    for ds in catalog["datasets"]:
        if ds["published"]:  # no personal data in published tables
            assert ds["classification"] == "public", ds["id"]
    glossary = json.loads((out / "glossary.json").read_text("utf-8"))
    terms = {t["term"] for t in glossary["terms"]}
    assert terms == {"criticality", "fragility", "maintenance actor", "bus factor", "era", "rollup"}


def test_export_lineage_from_hour_files(run: tuple[Path, Path]) -> None:
    """Without dbt-ol and a publish (no container or Iceberg in this test) the graph runs from
    every fixture hour file through parse into the free rollup."""
    _, out = run
    lineage = json.loads((out / "lineage.json").read_text("utf-8"))
    ids = {d["id"] for d in lineage["datasets"]}
    assert sum(i.startswith("https://data.gharchive.org/") for i in ids) == 7
    assert "duckdb://keystone/rollup.main.repo_hour" in ids
    assert lineage["tools"]["keystone CLI"]["table_edges"] > 0


def test_pipeline_metrics_valid_and_measured(run: tuple[Path, Path]) -> None:
    root, _ = run
    doc = json.loads((root / "metrics" / "pipeline_metrics.json").read_text("utf-8"))
    tm.validate(doc)
    assert doc["profile"] == "free"
    assert doc["totals"]["events_processed"] == doc["totals"]["events_stored"] == 303
    stages = {s["name"]: s for s in doc["last_run"]["stages"]}
    assert {"parse", "rollup", "parse-osv", "warehouse", "metadata-export"} <= set(stages)
    parse = stages["parse"]
    # The rate is computed from the unrounded duration; `duration_seconds` is rounded to 1 ms,
    # which on a ~35 ms fixture stage moves the recomputed rate by more than 1%. Bound it by
    # that rounding instead (and by the rate's own rounding to 0.001).
    out, secs, rate = parse["records_out"], parse["duration_seconds"], parse["records_per_second"]
    assert out / (secs + 0.0005) - 0.0005 <= rate <= out / max(secs - 0.0005, 1e-9) + 0.0005
    assert {e["name"] for e in doc["engines"]} >= {"duckdb", "dbt", "keystone-parser"}
    doc["cost"]["usd"] = -1
    with pytest.raises(jsonschema.ValidationError):
        tm.validate(doc)


def test_stage_marks_failures(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError), Stage(tmp_path, "s", "svc", "1") as st:
        st.records_out = 5
        raise RuntimeError("boom")
    (rec,) = read(tmp_path)
    assert (rec.status, rec.records_out) == ("failed", 5)


def test_metadata_templates() -> None:
    meta = Metadata()
    hour = meta.dataset("https://data.gharchive.org/2026-10-19-12.json.gz")
    assert hour.layer == "sources" and not hour.missing()
    with pytest.raises(KeyError):
        meta.dataset("https://data.gharchive.org/nested/2026-10-19-12.json.gz")


def test_grafana_comparison_tolerance() -> None:
    assert gf.Comparison("p", "k", 64.33, 64.32957).ok
    assert not gf.Comparison("p", "k", 40.0, 41.0).ok
    assert gf.Comparison("p", "k", None, None).ok
    assert not gf.Comparison("p", "k", 1.0, None).ok


def test_engines_active_is_the_last_run() -> None:
    from keystone.telemetry.runlog import StageRecord

    now = datetime.now(UTC).isoformat()
    recs = [
        StageRecord("a", "rollup", "duckdb", "1", now, now, 1.0, status="ok"),
        StageRecord("b", "warehouse", "dbt", "2", now, now, 1.0, status="ok"),
    ]
    rows = {e["name"]: e for e in tm.engines(recs, "b")}
    assert (rows["duckdb"]["active"], rows["dbt"]["active"]) == (False, True)


def test_last_attempt_per_stage_and_null_when_unmeasured(run: tuple[Path, Path]) -> None:
    root, _ = run
    doc = json.loads((root / "metrics" / "pipeline_metrics.json").read_text("utf-8"))
    names = [s["name"] for s in doc["last_run"]["stages"]]
    assert len(names) == len(set(names))
    assert doc["totals"]["bytes_downloaded"] is None  # no ingest ran: not a measured zero
    assert doc["totals"]["bytes_parsed"] == 52481
