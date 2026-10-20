"""ODCS contracts (datacontract-cli) and Great Expectations suites on parsed fixtures (Phase 3)."""

from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from conftest import FIXTURES, DataDir, write_gz

from keystone.parsers.parquet import EVENT_SCHEMA, HOUR_TYPE_SCHEMA, QUARANTINE_SCHEMA
from keystone.quality.contracts import contract_files, load_contract

SCHEMAS = {
    "keystone-gharchive-events": EVENT_SCHEMA,
    "keystone-gharchive-hour-types": HOUR_TYPE_SCHEMA,
    "keystone-gharchive-quarantine": QUARANTINE_SCHEMA,
}
LOGICAL = {
    pa.string(): "string",
    pa.int64(): "integer",
    pa.bool_(): "boolean",
    pa.timestamp("us", tz="UTC"): "timestamp",
}


def _glob(root: Path, table: str) -> str:
    rel = {
        "keystone-gharchive-events": "silver/gharchive/events",
        "keystone-gharchive-hour-types": "silver/gharchive/hour_types",
        "keystone-gharchive-quarantine": "quarantine/gharchive",
    }[table]
    return (root / rel / "*" / "*.parquet").as_posix()


def test_contracts_match_the_written_schemas() -> None:
    """datacontract-cli cannot check Parquet types, so names, types and required are checked
    here against the Arrow schemas the writer uses."""
    files = contract_files()
    assert {load_contract(f)["id"] for f in files} == set(SCHEMAS)
    for path in files:
        spec = load_contract(path)
        assert spec["apiVersion"] == "v3.2.0" and spec["kind"] == "DataContract"
        [table] = spec["schema"]
        schema = SCHEMAS[spec["id"]]
        props = table["properties"]
        assert [p["name"] for p in props] == schema.names, path.name
        for prop, fld in zip(props, schema, strict=True):
            assert prop["logicalType"] == LOGICAL[fld.type], (path.name, fld.name)
            assert bool(prop.get("required", False)) == (not fld.nullable), (path.name, fld.name)
            assert prop.get("description"), (path.name, fld.name)  # every column documented


def test_contract_trim_instant_matches_config() -> None:
    from keystone.sources import load_sources

    trim = load_sources().gharchive.payload_trim_at
    text = (contract_files()[0].parent / "gharchive_events.odcs.yaml").read_text("utf-8")
    literal = f"TIMESTAMPTZ '{trim:%Y-%m-%d %H:%M:%S}+00'"
    assert text.count(literal) == 2 and text.count("TIMESTAMPTZ") == 2


datacontract = pytest.importorskip("datacontract", reason="quality group not installed")


def _quarantined(data: DataDir, tmp_path: Path) -> None:
    lines = [b"{not json", orjson.dumps({"id": "1", "type": "WatchEvent"})]
    data.parse(write_gz(tmp_path / "in" / "2015-01-01-13.json.gz", lines))


def test_contracts_pass_on_parsed_fixtures(parsed_fixtures: DataDir, tmp_path: Path) -> None:
    from keystone.quality.contracts import run_contract

    _quarantined(parsed_fixtures, tmp_path)
    for path in contract_files():
        result = run_contract(path, _glob(parsed_fixtures.root, load_contract(path)["id"]))
        assert result.ok, [(c.name, c.reason) for c in result.problems]
        assert sum(c.result == "passed" for c in result.checks) >= 10


def _seed_violation(data: DataDir) -> Path:
    """Rewrite one E2 events file with two contract violations."""
    path = next(p for p in data.events.glob("*/*.parquet") if "2015-01-01" in p.name)
    table = pq.read_table(path)
    kinds = pc.if_else(pc.equal(table["actor_key_kind"], "id"), "login", table["actor_key_kind"])
    table = table.set_column(
        table.schema.get_field_index("actor_key_kind"), "actor_key_kind", kinds
    )
    era = pa.array(["E9"] * table.num_rows)
    table = table.set_column(table.schema.get_field_index("era"), "era", era)
    pq.write_table(table, path)
    return path


def test_seeded_contract_violation_is_caught(parsed_fixtures: DataDir) -> None:
    from keystone.quality.contracts import run_contract

    _seed_violation(parsed_fixtures)
    path = next(p for p in contract_files() if p.name == "gharchive_events.odcs.yaml")
    result = run_contract(path, _glob(parsed_fixtures.root, "keystone-gharchive-events"))
    assert not result.ok
    problems = " | ".join(f"{c.field} {c.name}" for c in result.problems)
    assert "era" in problems and "ADR-013" in problems


gx = pytest.importorskip("great_expectations", reason="quality group not installed")


@pytest.fixture(scope="module")
def validator() -> object:
    from keystone.quality.expectations import Validator

    return Validator()


def test_gx_suites_pass_on_parsed_fixtures(
    parsed_fixtures: DataDir, validator: object, tmp_path: Path
) -> None:
    _quarantined(parsed_fixtures, tmp_path)
    tables = {
        "events": parsed_fixtures.events,
        "hour_types": parsed_fixtures.root / "silver" / "gharchive" / "hour_types",
        "quarantine": parsed_fixtures.root / "quarantine" / "gharchive",
    }
    checked = 0
    for table, root in tables.items():
        for path in root.glob("*/*.parquet"):
            result = validator.validate(table, path)  # type: ignore[attr-defined]
            assert result.ok, (path.name, result.failed)
            checked += 1
    assert checked == 2 * len(list(FIXTURES.glob("*.json.gz"))) + 3  # + one quarantine hour


def test_gx_catches_a_seeded_violation(parsed_fixtures: DataDir, validator: object) -> None:
    path = _seed_violation(parsed_fixtures)
    result = validator.validate("events", path)  # type: ignore[attr-defined]
    assert not result.ok
    failed = " ".join(result.failed)
    assert "expect_column_values_to_be_in_set" in failed and "era" in failed
    assert "row_condition" in failed  # the per-era rules (ADR-013) fire, not only column sets


def _null_push_sizes(data: DataDir) -> Path:
    """E2 pushes (before the payload trim) without a size: the trim rule must catch it."""
    path = next(p for p in data.events.glob("*/*.parquet") if "2015-01-01" in p.name)
    table = pq.read_table(path)
    index = table.schema.get_field_index("push_size")
    table = table.set_column(index, "push_size", pa.nulls(table.num_rows, pa.int64()))
    pq.write_table(table, path)
    return path


def test_trim_rule_catches_missing_push_sizes(parsed_fixtures: DataDir, validator: object) -> None:
    from keystone.quality.contracts import run_contract

    path = _null_push_sizes(parsed_fixtures)
    result = validator.validate("events", path)  # type: ignore[attr-defined]
    assert any("push_size" in f for f in result.failed), result.failed
    contract = next(p for p in contract_files() if p.name == "gharchive_events.odcs.yaml")
    run = run_contract(contract, _glob(parsed_fixtures.root, "keystone-gharchive-events"))
    assert any("payload trim" in c.name for c in run.problems)
