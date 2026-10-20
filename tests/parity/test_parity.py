"""Engine parity on the running stack (Redpanda, Flink, Lakekeeper, RustFS): the DuckDB rollup
and the Flink rollup must match row for row on shared fixtures, including late and
out-of-order events. Skipped unless KEYSTONE_STACK_TESTS=1 (CI: .github/workflows/parity.yml)."""

import json
import os
import uuid
from pathlib import Path

import pytest

from keystone.rollup import compare, flink, stream
from keystone.rollup.contract import schema_text
from keystone.rollup.duck import DuckRollup
from keystone.rollup.spec import rollups

from . import scenario

pytestmark = pytest.mark.skipif(
    os.environ.get("KEYSTONE_STACK_TESTS") != "1", reason="needs the compose stack"
)


def _engines(tmp: Path, files: list[Path], silver: Path, ns: str, mode: flink.Mode) -> Path:
    db = tmp / f"{ns}.duckdb"
    duck = DuckRollup(db, silver)
    duck.run()
    duck.close()
    topic = f"keystone.parity.{ns}"
    stream.delete_topic(stream.DEFAULT_BOOTSTRAP, topic)
    stream.ensure_topic(stream.DEFAULT_BOOTSTRAP, topic)
    stream.produce(files, topic)
    try:
        flink.submit(flink.render_job(ns, topic, mode), f"{ns}-{mode}")
        flink.submit(flink.render_fold(ns), f"{ns}-fold")
    finally:
        stream.delete_topic(stream.DEFAULT_BOOTSTRAP, topic)
    return db


@pytest.fixture(scope="module")
def scenario_silver(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, list[Path], Path]:
    tmp = tmp_path_factory.mktemp("parity")
    silver = tmp / "silver"
    files = [scenario.write_hour(silver, name, rows) for name, rows in scenario.hours()]
    return tmp, files, silver


@pytest.mark.parametrize("mode", ["stream", "batch"])
def test_scenario_parity(scenario_silver: tuple[Path, list[Path], Path], mode: str) -> None:
    tmp, files, silver = scenario_silver
    ns = f"parity_{mode}_{uuid.uuid4().hex[:8]}"
    db = _engines(tmp, files, silver, ns, mode)  # type: ignore[arg-type]
    results = compare.compare(db, ns)
    assert all(r.ok for r in results), results
    late = compare.late_rows(ns)
    if mode == "stream":
        # The late hours really took the late path (watermark per event, one partition) ...
        assert late["repo_hour"] > 0 and late["repo_actor_day"] > 0, late
        # ... and without it the result would be wrong: the test can see a difference.
        ontime = compare.compare(db, ns, table_suffix="_ontime")
        assert not any(r.ok for r in ontime), ontime
    else:
        assert late == {r.name: 0 for r in rollups()}


def test_fixture_hours_parity(parsed_fixtures: object, tmp_path: Path) -> None:
    root: Path = parsed_fixtures.root  # type: ignore[attr-defined]
    silver = root / "silver" / "gharchive"
    ns = f"parity_fixtures_{uuid.uuid4().hex[:8]}"
    db = _engines(tmp_path, stream.files_in_event_order(silver), silver, ns, "stream")
    results = compare.compare(db, ns)
    assert all(r.ok for r in results) and all(r.free_rows > 0 for r in results), results


def test_registry_compatibility_live() -> None:
    reg = stream.Registry()
    subject = f"keystone.parity.schema.{uuid.uuid4().hex[:8]}-value"
    reg.set_compatibility(subject)
    reg.register(subject, schema_text())
    schema = json.loads(schema_text())
    added = {
        **schema,
        "fields": [*schema["fields"], {"name": "x", "type": ["null", "string"], "default": None}],
    }
    assert reg.is_compatible(subject, json.dumps(added))  # optional field with a default
    required = {**schema, "fields": [*schema["fields"], {"name": "x", "type": "string"}]}
    assert not reg.is_compatible(subject, json.dumps(required))  # old data cannot be read
    retyped = {
        **schema,
        "fields": [
            {**f, "type": "int"} if f["name"] == "source_line" else f for f in schema["fields"]
        ],
    }
    assert not reg.is_compatible(subject, json.dumps(retyped))  # long -> int narrows
    with pytest.raises(stream.IncompatibleSchema):
        reg.register(subject, json.dumps(required))
