"""Profile fallback (ADR-201): a `full` outage runs the job in `free` with the same output, and
the decision reaches pipeline_metrics.json and the export envelope (ADR-204)."""

import json
import socket
from pathlib import Path

import duckdb
import jsonschema
import pytest

from keystone import cli
from keystone.export import envelope, schemas
from keystone.paths import METADATA_DIR
from keystone.profile import probe
from keystone.profile import run as prun
from keystone.rollup.duck import DuckRollup
from keystone.rollup.spec import rollups
from keystone.telemetry import metrics as tm
from keystone.telemetry.runlog import read

SOURCES = [{"name": "GH Archive", "url": "https://www.gharchive.org/"}]


def _closed_port() -> int:
    """A local port nothing listens on (bound, then released)."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _outage() -> list[probe.Probe]:
    """The `full` probes, every one pointed at a port with no service behind it."""
    out = []
    for p in probe.FULL_PROBES:
        port = _closed_port()
        target = f"127.0.0.1:{port}" if p.kind == "tcp" else f"http://127.0.0.1:{port}/"
        out.append(probe.Probe(p.name, p.kind, target, p.max_status))
    return out


def _rows(db: Path) -> dict[str, list[tuple[object, ...]]]:
    con = duckdb.connect(str(db), read_only=True)
    out = {
        r.name: con.execute(f"SELECT * FROM {r.name} ORDER BY ALL").fetchall() for r in rollups()
    }
    con.close()
    return out


def _free(root: Path) -> prun.Job:
    return lambda: cli.rollup_run(root=root, json=False)


def _healthy(monkeypatch: pytest.MonkeyPatch) -> None:
    ok = [probe.ProbeResult(p.name, p.target, True, 0.0, "stub") for p in probe.FULL_PROBES]
    monkeypatch.setattr(prun, "check", lambda probes: ok)


def test_full_outage_falls_back_to_free_with_correct_output(
    parsed_fixtures: object, tmp_path: Path
) -> None:
    root = parsed_fixtures.root  # type: ignore[attr-defined]
    full_calls: list[int] = []

    out = prun.run(root, lambda: full_calls.append(1), _free(root), probes=_outage())

    assert full_calls == []  # the full job is never attempted against a dead stack
    assert out.produced_by == "free" and out.fallback and not out.full_healthy
    assert {p.name for p in out.probes if not p.ok} == {p.name for p in probe.FULL_PROBES}
    assert out.reason.startswith("full unhealthy: redpanda")

    # Correct output: the fallback's rollup equals a reference run of the free engine on the
    # same silver, and every silver event is counted once.
    produced = _rows(root / "rollup" / "rollup.duckdb")
    ref = DuckRollup(tmp_path / "ref.duckdb", root / "silver" / "gharchive")
    ref.run()
    ref.close()
    assert produced == _rows(tmp_path / "ref.duckdb")
    con = duckdb.connect()
    silver = (root / "silver" / "gharchive" / "events").as_posix()
    n = con.execute(f"SELECT count(*) FROM read_parquet('{silver}/*/*.parquet')").fetchone()
    assert n is not None and n[0] == 303
    rollup = duckdb.connect(str(root / "rollup" / "rollup.duckdb"), read_only=True)
    assert rollup.execute("SELECT sum(events) FROM repo_hour").fetchone() == (303,)
    rollup.close()

    # The decision is in the run log, pipeline_metrics.json and the export envelope.
    stages = [r for r in read(root) if r.stage == "profile"]
    assert len(stages) == 1 and stages[0].status == "ok"
    doc = tm.build(root, METADATA_DIR)
    tm.validate(doc)
    assert doc["profile"] == "free"
    assert doc["produced_by"]["profile"] == "free" and doc["produced_by"]["fallback"] is True
    assert doc["produced_by"]["probes"] == [{"name": p.name, "ok": False} for p in out.probes]
    env = envelope.wrap(
        "radar", "v1", [], produced=envelope.recorded_profile(root), sources=SOURCES
    )
    assert env["produced_by"]["profile"] == "free" and env["produced_by"]["fallback"] is True


def test_full_job_failure_falls_back(
    parsed_fixtures: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = parsed_fixtures.root  # type: ignore[attr-defined]
    _healthy(monkeypatch)
    calls: list[str] = []

    def broken_full() -> None:
        calls.append("full")
        raise RuntimeError("flink job failed (exit 1)")

    out = prun.run(root, broken_full, _free(root))
    assert calls == ["full"]
    assert out.full_healthy and out.produced_by == "free" and out.fallback
    assert out.reason == "full job failed: RuntimeError: flink job failed (exit 1)"
    assert (root / "rollup" / "rollup.duckdb").exists()


def test_healthy_full_runs_full_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _healthy(monkeypatch)
    calls: list[str] = []
    out = prun.run(tmp_path, lambda: calls.append("full"), lambda: calls.append("free"))
    assert calls == ["full"] and out.produced_by == "full" and not out.fallback
    assert tm.produced_by(read(tmp_path))["profile"] == "full"  # type: ignore[index]


def test_free_requested_skips_probes(tmp_path: Path) -> None:
    calls: list[str] = []
    out = prun.run(
        tmp_path, lambda: calls.append("full"), lambda: calls.append("free"), want="free"
    )
    assert calls == ["free"] and out.probes == [] and not out.fallback


def test_free_failure_is_not_swallowed(tmp_path: Path) -> None:
    def broken_free() -> None:
        raise RuntimeError("disk full")

    with pytest.raises(RuntimeError, match="disk full"):
        prun.run(tmp_path, lambda: None, broken_free, probes=_outage())
    (rec,) = read(tmp_path)
    assert rec.status == "failed" and rec.detail["produced_by"] == "free"
    with pytest.raises(envelope.NoProfileRecorded):  # a failed run is no basis for an export
        envelope.recorded_profile(tmp_path)


def test_check_only_records_no_producer(tmp_path: Path) -> None:
    out = prun.check_only(tmp_path, _outage())
    assert out.produced_by is None and not out.full_healthy
    assert tm.produced_by(read(tmp_path)) is None


def test_envelope_refuses_unknown_profile_and_bad_counts(tmp_path: Path) -> None:
    with pytest.raises(envelope.NoProfileRecorded):
        envelope.recorded_profile(tmp_path)
    produced = {"profile": "free", "fallback": False, "run_id": "r", "keystone_version": "0"}
    doc = envelope.wrap("radar", "v1", [{"repo": 1}], produced=produced, sources=SOURCES)
    assert doc["row_count"] == 1
    doc["row_count"] = 2
    with pytest.raises(jsonschema.ValidationError):
        envelope.validate(doc)
    with pytest.raises(jsonschema.ValidationError):
        envelope.wrap("Radar!", "v1", [], produced=produced, sources=SOURCES)


def test_schema_examples_pass_and_checker_catches_bad_ones(tmp_path: Path) -> None:
    assert schemas.check() == []
    contracts, examples = tmp_path / "contracts", tmp_path / "examples"
    contracts.mkdir()
    (contracts / "x.schema.json").write_text(
        json.dumps({"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"})
    )
    (examples / "x").mkdir(parents=True)
    (examples / "x" / "valid_a.json").write_text("[]")  # not an object: rejected
    (examples / "x" / "invalid_b.json").write_text("{}")  # an object: accepted
    problems = schemas.check(contracts, examples)
    assert len(problems) == 2
    (contracts / "y.schema.json").write_text(json.dumps({"type": 5}))
    assert any("not a valid 2020-12 schema" in p for p in schemas.check(contracts, examples))
