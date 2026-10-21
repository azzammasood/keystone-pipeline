"""Phase 5: OSV silver, the dbt warehouse on fixtures, docs coverage and the Iceberg helpers."""

import csv
import hashlib
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from conftest import FIXTURES, TEST_SALT, DataDir, fixture_hours

from keystone.parsers import osv as osvparse
from keystone.parsers.drift import load_registry
from keystone.parsers.parquet import GhArchiveSink, write_osv
from keystone.parsers.privacy import ActorHasher
from keystone.paths import REPO_ROOT
from keystone.rollup.duck import DuckRollup
from keystone.sources import load_sources
from keystone.warehouse import coverage, lake
from keystone.warehouse import dbt as whdbt

OSV_ZIP = REPO_ROOT / "tests" / "fixtures" / "osv" / "PyPI" / "all.zip"
SEEDS = REPO_ROOT / "dbt" / "seeds"


def _ctx() -> osvparse.ZipContext:
    sha = hashlib.sha256(OSV_ZIP.read_bytes()).hexdigest()
    return osvparse.ZipContext("osv/PyPI/all.zip", sha, datetime(2026, 10, 20, tzinfo=UTC))


def test_osv_parser_allow_list_and_quarantine(tmp_path: Path) -> None:
    result = osvparse.parse_zip(OSV_ZIP, _ctx())
    assert sorted(v.vuln_id for v in result.vulns) == [
        "GHSA-aaaa-0001-0001",
        "MAL-2026-4",
        "PYSEC-2026-2",
        "PYSEC-2026-3",
    ]
    assert sorted(q.reason for q in result.quarantine) == ["ValueError", "missing field 'modified'"]
    ghsa = next(v for v in result.vulns if v.vuln_id.startswith("GHSA"))
    assert ghsa.modified_at == datetime(2026, 2, 3, 4, 5, 6, 123456, tzinfo=UTC)  # ns cut
    assert ghsa.severity_label == "HIGH" and ghsa.cvss_v3_vector and ghsa.github_reviewed
    assert next(v for v in result.vulns if v.vuln_id == "MAL-2026-4").is_malicious
    refs = {(r.vuln_id, r.affected_index, r.kind, r.repo_full_name) for r in result.repo_refs}
    assert refs == {
        ("GHSA-aaaa-0001-0001", 0, "git_range", "owner-1/repo-1"),
        ("GHSA-aaaa-0001-0001", None, "fix_reference", "owner-1/repo-1"),
        ("PYSEC-2026-2", None, "fix_reference", "owner-3/repo-3"),
        ("PYSEC-2026-3", None, "fix_reference", "group/proj"),
    }  # the advisory-database link and the web link are not repositories
    write_osv("PyPI", result, tmp_path / "silver", tmp_path / "quarantine")
    for path in tmp_path.rglob("*.parquet"):  # no free text, credits or contacts anywhere
        text = str(pq.read_table(path).to_pylist())
        for needle in ("Some Person", "person@", "free text", "iocs"):
            assert needle not in text, (path, needle)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://github.com/Owner/Repo.git", ("github.com", "owner/repo")),
        ("git://github.com/o/r", ("github.com", "o/r")),
        ("https://github.com/o/r/commit/abc", ("github.com", "o/r")),
        ("https://www.github.com/o/r.js/pull/1", ("github.com", "o/r.js")),
        ("https://gitlab.com/g/p/-/commit/1", ("gitlab.com", "g/p")),
        ("https://github.com/advisories/GHSA-x", None),
        ("https://github.com/o", None),
        ("https://example.com/o/r", None),
    ],
)
def test_repo_of(url: str, expected: tuple[str, str] | None) -> None:
    assert osvparse.repo_of(url) == expected


@pytest.mark.parametrize(
    ("seed", "key", "rows"),
    [
        ("completeness_periods", "period", "completeness_periods"),
        ("schema_eras", "era", "eras"),
    ],
)
def test_seeds_match_sources_config(seed: str, key: str, rows: str) -> None:
    with (SEEDS / f"{seed}.csv").open(encoding="utf-8", newline="") as fh:
        got = [(r[key], r["label"], r["start_at"]) for r in csv.DictReader(fh)]
    cfg = load_sources().gharchive
    want = [
        (p.name, p.label, p.start.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))
        for p in getattr(cfg, rows)
    ]
    assert got == want


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> whdbt.WarehousePaths:
    """Fixture hours -> silver -> free rollup; OSV fixture -> silver; then `dbt build`."""
    root = tmp_path_factory.mktemp("wh") / "data"
    sink = GhArchiveSink(
        load_sources().gharchive,
        root / "silver" / "gharchive",
        root / "quarantine" / "gharchive",
        root / "quality" / "drift",
        ActorHasher(TEST_SALT.encode()),
        load_registry(),
    )
    data = DataDir(root, sink)
    for name in fixture_hours():
        data.parse(FIXTURES / f"{name}.json.gz")
    duck = DuckRollup(root / "rollup" / "rollup.duckdb", root / "silver" / "gharchive")
    duck.run()
    duck.con.close()
    write_osv(
        "PyPI",
        osvparse.parse_zip(OSV_ZIP, _ctx()),
        root / "silver" / "osv",
        root / "quarantine" / "osv",
    )
    # OSV repeats a record in every ecosystem zip it affects: a second zip with the same
    # records must not duplicate anything downstream.
    copy = osvparse.ZipContext("osv/npm/all.zip", _ctx().source_sha256, _ctx().fetched_at)
    write_osv(
        "npm",
        osvparse.parse_zip(OSV_ZIP, copy),
        root / "silver" / "osv",
        root / "quarantine" / "osv",
    )
    paths = whdbt.WarehousePaths(root)
    assert whdbt.run_dbt(["build"], paths)
    assert whdbt.run_dbt(["docs", "generate"], paths)
    return paths


def _q(paths: whdbt.WarehousePaths, sql: str) -> list[tuple[object, ...]]:
    con = duckdb.connect(str(paths.warehouse_db))  # dbt-duckdb holds it open in-process
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_dbt_build_on_fixtures(built: whdbt.WarehousePaths) -> None:
    assert _q(built, "SELECT COUNT(*), COUNT(DISTINCT vuln_id) FROM marts.fct_vuln") == [(5, 4)]
    assert _q(built, "SELECT COUNT(*) FROM staging.stg_osv__repo_refs") == [(4,)]
    bridge = _q(
        built,
        "SELECT package_key, repo_full_name, repo_id IS NOT NULL, repo_match, confidence "
        "FROM marts.bridge_package_repo ORDER BY 1, 2",
    )
    assert bridge == [
        # two fixture repository ids carry owner-1/repo-1, so no id is chosen
        ("PyPI/pkg-one", "owner-1/repo-1", False, "ambiguous_name", "high"),
        ("PyPI/pkg-three", "owner-3/repo-3", True, "unique_name", "low"),
        ("PyPI/pkg-two", "group/proj", False, "not_github", "medium"),
        ("PyPI/pkg-two", "owner-3/repo-3", True, "unique_name", "low"),
    ]
    # every era in the fixtures lands in its own metric versions only
    assert {r[0] for r in _q(built, "SELECT era FROM metrics.metric_maint_v1_repo_day")} <= {
        "E1",
        "E2",
    }
    assert {r[0] for r in _q(built, "SELECT era FROM metrics.metric_maint_v2_repo_day")} == {"E3"}
    eras = {r[0] for r in _q(built, "SELECT DISTINCT era FROM marts.fct_repo_day")}
    assert eras == {"E0", "E1", "E2", "E3"}
    (withdrawn,) = _q(
        built,
        "SELECT withdrawn_vuln_count FROM marts.dim_package WHERE package_key = 'PyPI/pkg-two'",
    )
    assert withdrawn == (1,)


def test_docs_coverage_gate(built: whdbt.WarehousePaths) -> None:
    cov = coverage.measure(built.target_dir)
    assert cov.ok and cov.models >= 19 and not cov.missing


def test_privacy_test_catches_a_person_column(built: whdbt.WarehousePaths) -> None:
    con = duckdb.connect(str(built.warehouse_db))
    con.execute("CREATE TABLE marts.leak AS SELECT 'k' AS actor_key")
    con.close()
    try:
        assert not whdbt.run_dbt(["test", "--select", "assert_no_person_columns_published"], built)
    finally:
        con = duckdb.connect(str(built.warehouse_db))
        con.execute("DROP TABLE marts.leak")
        con.close()


def test_published_models_are_marts_and_metrics(built: whdbt.WarehousePaths) -> None:
    models = lake.published_models(built.target_dir)
    assert {s for s, _ in models} == {"marts", "metrics"}
    assert all(not lake.PERSON.search(m) for _, m in models)


def test_fingerprint_sql_and_output_parsing(tmp_path: Path) -> None:
    con = duckdb.connect()
    con.execute(f"ATTACH '{(tmp_path / 'ice.duckdb').as_posix()}' AS ice")
    con.execute("CREATE SCHEMA ice.ns")
    con.execute(
        "CREATE TABLE ice.ns.t AS SELECT 1::BIGINT AS a, TRUE AS b, 'x' AS s, NULL::INT AS z"
    )
    sql = lake.fingerprint_sql(con, lake.Table("ns", "t"), "q000")
    assert sql == (
        "SELECT 'q000' AS t, COUNT(*) AS n, COUNT(a), SUM(CAST(a AS BIGINT)), COUNT(b), "
        "SUM(CASE WHEN b THEN 1 ELSE 0 END), COUNT(s), COUNT(DISTINCT s), "
        "SUM(CAST(CHAR_LENGTH(s) AS BIGINT)), COUNT(z), SUM(CAST(z AS BIGINT)) "
        "FROM ice.ns.t"
    )
    row = con.execute(sql).fetchone()
    assert row == ("q000", 1, 1, 1, 1, 1, 1, 1, 1, 0, None)
    spark = ["q000\t1\t1\t1\t1\t1\t1\t1\t1\t0\tNULL", "noise"]
    flink = ["| t | n |", "| q000 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | <NULL> |"]
    want = {"q000": (1, 1, 1, 1, 1, 1, 1, 1, 0, None)}
    assert lake._rows(spark, {"q000"}, "\t") == want
    assert lake._rows(flink, {"q000"}, "|") == want
    with pytest.raises(ValueError):
        lake.fingerprint_sql(con, lake.Table("ns", "t; DROP"), "q001")
