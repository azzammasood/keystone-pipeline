"""Era-aware parsing, privacy at read time, quarantine and drift (Phase 3)."""

import gzip
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import orjson
import pyarrow.parquet as pq
import pytest
from conftest import FIXTURES, TEST_SALT, DataDir, fixture_hours, write_gz

from keystone.parsers.drift import DriftError
from keystone.parsers.gharchive import TIMELINE_TS
from keystone.parsers.parquet import EVENT_SCHEMA, HOUR_TYPE_SCHEMA, QUARANTINE_SCHEMA
from keystone.parsers.privacy import ActorHasher, SaltMissing, is_bot, salt_from_env
from keystone.sources import load_sources

EXPECTED_ERA = {
    "2011-02-12-0": "E0",
    "2012-03-11-0": "E1",  # early E1: `2012/03/11 00:00:01 -0800` timestamps, integer `public`
    "2012-06-15-12": "E1",
    "2013-06-15-12": "E1",
    "2014-12-31-12": "E1",
    "2015-01-01-12": "E2",
    "2026-10-19-12": "E3",
}


def _records(name: str) -> list[dict[str, object]]:
    with gzip.open(FIXTURES / f"{name}.json.gz", "rb") as fh:
        return [orjson.loads(line) for line in fh]


def _sql(data: DataDir, query: str) -> list[tuple[object, ...]]:
    glob = (data.events / "*" / "*.parquet").as_posix()
    return duckdb.sql(query.replace("EVENTS", f"read_parquet('{glob}')")).fetchall()


def test_fixtures_cover_every_era() -> None:
    cfg = load_sources().gharchive
    assert set(fixture_hours()) == set(EXPECTED_ERA)
    for name, era in EXPECTED_ERA.items():
        assert cfg.era_for(datetime.fromisoformat(name[:10]).replace(tzinfo=UTC)) == era


def test_every_era_parses_without_quarantine_or_drift(parsed_fixtures: DataDir) -> None:
    for name in fixture_hours():
        types = Counter(str(r["type"]) for r in _records(name))
        rows = _sql(
            parsed_fixtures,
            f"SELECT event_type, era, count(*) FROM EVENTS "
            f"WHERE source_file = 'gharchive/{name}.json.gz' GROUP BY ALL",
        )
        assert {t: n for t, _, n in rows} == dict(types), name
        assert {e for _, e, _ in rows} == {EXPECTED_ERA[name]}, name
    assert not (parsed_fixtures.root / "quarantine").exists()


def test_schemas_written_as_declared(parsed_fixtures: DataDir) -> None:
    root = parsed_fixtures.root / "silver" / "gharchive"
    for table, schema in (("events", EVENT_SCHEMA), ("hour_types", HOUR_TYPE_SCHEMA)):
        for path in (root / table).glob("*/*.parquet"):
            assert pq.read_schema(path).equals(schema, check_metadata=False), path


def test_lineage_on_every_row(parsed_fixtures: DataDir) -> None:
    [(n, missing)] = _sql(
        parsed_fixtures,
        "SELECT count(*), count(*) FILTER (WHERE source_file IS NULL OR source_sha256 IS NULL "
        "OR fetched_at IS NULL OR source_line IS NULL) FROM EVENTS",
    )
    assert n == sum(len(_records(h)) for h in fixture_hours()) and missing == 0


def test_timestamps_are_utc_and_offsets_kept(parsed_fixtures: DataDir) -> None:
    rows = _sql(
        parsed_fixtures,
        "SELECT source_file, source_line, created_at, created_at_offset_min FROM EVENTS "
        "WHERE era = 'E1'",
    )
    assert rows
    for source_file, line, created, offset in rows:
        name = str(source_file).removeprefix("gharchive/").removesuffix(".json.gz")
        text = str(_records(name)[int(str(line)) - 1]["created_at"])
        raw = datetime.strptime(text, TIMELINE_TS) if "/" in text else datetime.fromisoformat(text)
        assert isinstance(created, datetime) and created == raw  # same instant
        assert offset in (-420, -480) and raw.utcoffset() is not None
        assert int(raw.utcoffset().total_seconds() // 60) == offset  # type: ignore[union-attr]
    [(non_utc,)] = _sql(
        parsed_fixtures,
        "SELECT count(*) FROM EVENTS WHERE era <> 'E1' AND created_at_offset_min <> 0",
    )
    assert non_utc == 0


def test_actor_keys_per_era(parsed_fixtures: DataDir) -> None:
    rows = dict(
        _sql(
            parsed_fixtures,
            "SELECT era, string_agg(DISTINCT actor_key_kind, ',') FROM EVENTS GROUP BY era",
        )
    )
    assert rows == {"E0": "id", "E1": "login", "E2": "id", "E3": "id"}
    [(bad,)] = _sql(
        parsed_fixtures,
        "SELECT count(*) FROM EVENTS WHERE actor_key IS NOT NULL "
        "AND NOT regexp_matches(actor_key, '^[0-9a-f]{32}$')",
    )
    assert bad == 0


def test_actor_key_is_the_salted_hash(parsed_fixtures: DataDir) -> None:
    rec = _records("2015-01-01-12")[0]
    actor = rec["actor"]
    assert isinstance(actor, dict)
    expected = ActorHasher(TEST_SALT.encode()).key("id", actor["id"])
    [(key,)] = _sql(
        parsed_fixtures,
        "SELECT actor_key FROM EVENTS WHERE source_file = 'gharchive/2015-01-01-12.json.gz' "
        "AND source_line = 1",
    )
    assert key == expected
    other = ActorHasher(b"another-salt-0123456789").key("id", actor["id"])
    assert other != expected
    # E1 logins are case-insensitive on GitHub: one person, one key.
    hasher = ActorHasher(TEST_SALT.encode())
    assert hasher.key("login", "Owner-1") == hasher.key("login", "owner-1")


def test_no_personal_fields_reach_parquet(parsed_fixtures: DataDir) -> None:
    names = set(EVENT_SCHEMA.names)
    assert not {n for n in names if any(w in n for w in ("login", "email", "name_", "avatar"))}
    logins: set[str] = set()
    for name in fixture_hours():
        for rec in _records(name):
            actor = rec.get("actor")
            login = actor if isinstance(actor, str) else (actor or {}).get("login")  # type: ignore[union-attr]
            if login:
                logins.add(str(login))
    for path in parsed_fixtures.events.glob("*/*.parquet"):
        table = pq.read_table(path).drop(["repo_name"])
        for column in table.column_names:
            values = {str(v) for v in table.column(column).to_pylist() if v is not None}
            assert not values & logins, (path.name, column)
            assert not any("@" in v for v in values), (path.name, column)
        assert b"redacted@example.invalid" not in path.read_bytes()


def test_salt_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEYSTONE_ACTOR_SALT")
    with pytest.raises(SaltMissing):
        salt_from_env()
    monkeypatch.setenv("KEYSTONE_ACTOR_SALT", "short")
    with pytest.raises(SaltMissing):
        salt_from_env()


def test_bots() -> None:
    assert is_bot("dependabot[bot]") and is_bot("Greenkeeperio-Bot")
    assert not is_bot("octocat") and not is_bot(None)


def test_early_e1_formats(parsed_fixtures: DataDir) -> None:
    rows = _sql(
        parsed_fixtures,
        "SELECT public, count(*) FROM EVENTS WHERE source_file = 'gharchive/2012-03-11-0.json.gz' "
        "GROUP BY ALL",
    )
    raw = Counter(r.get("public") for r in _records("2012-03-11-0"))
    assert dict(rows) == {True: raw[True], False: raw[0]}


def test_integer_public_other_than_0_or_1_is_quarantined(data_dir: DataDir, tmp_path: Path) -> None:
    recs = _records("2012-03-11-0")[:2]
    recs[1] = dict(recs[1], public=2)
    data_dir.parse(
        write_gz(tmp_path / "in" / "2012-03-11-0.json.gz", [orjson.dumps(r) for r in recs])
    )
    q = pq.read_table(next((data_dir.root / "quarantine").rglob("*.parquet")))
    assert q.column("reason").to_pylist() == ["public_value"]


def test_e0_placeholder_repo_is_null(parsed_fixtures: DataDir) -> None:
    [(slash,)] = _sql(parsed_fixtures, "SELECT count(*) FROM EVENTS WHERE repo_name = '/'")
    assert slash == 0


def test_reparse_is_idempotent(data_dir: DataDir) -> None:
    path = FIXTURES / "2015-01-01-12.json.gz"
    data_dir.parse(path)
    out = next(data_dir.events.glob("*/*.parquet"))
    first = pq.read_table(out)
    data_dir.parse(path)
    assert pq.read_table(out).equals(first)
    assert len(list(data_dir.events.glob("*/*.parquet"))) == 1


# --- quarantine ------------------------------------------------------------------------------


def test_bad_records_are_quarantined_with_a_reason(data_dir: DataDir, tmp_path: Path) -> None:
    good = [orjson.dumps(r) for r in _records("2015-01-01-12")[:3]]
    naive = dict(_records("2015-01-01-12")[0], created_at="2015-01-01T12:00:00")
    missing = {k: v for k, v in _records("2015-01-01-12")[1].items() if k != "created_at"}
    secret = b'{"id": "1", "actor": {"login": "someone@example.invalid"'  # truncated JSON
    lines = [*good, secret, b"[1, 2]", orjson.dumps(naive), orjson.dumps(missing)]
    path = write_gz(tmp_path / "in" / "2015-01-01-12.json.gz", lines)
    data_dir.parse(path)
    qfile = next((data_dir.root / "quarantine").rglob("*.parquet"))
    q = pq.read_table(qfile)
    assert q.schema.equals(QUARANTINE_SCHEMA, check_metadata=False)
    reasons = dict(
        zip(q.column("source_line").to_pylist(), q.column("reason").to_pylist(), strict=True)
    )
    assert reasons == {
        4: "json_decode",
        5: "not_object",
        6: "timestamp_naive:created_at",
        7: "missing:created_at",
    }
    assert b"someone" not in qfile.read_bytes()  # no record content in quarantine
    assert pq.read_table(next(data_dir.events.glob("*/*.parquet"))).num_rows == 3


# --- seeded schema drift ---------------------------------------------------------------------


def _drifted(tmp_path: Path, name: str, change: object) -> Path:
    recs = _records(name)
    change(recs)  # type: ignore[operator]
    return write_gz(tmp_path / "in" / f"{name}.json.gz", [orjson.dumps(r) for r in recs])


@pytest.mark.parametrize(
    ("name", "mutate", "expect"),
    [
        ("2015-01-01-12", lambda r: r[0]["payload"].update(brand_new=1), "unknown_field"),
        ("2015-01-01-12", lambda r: r[0].update(surprise={"x": 1}), "unknown_field _envelope"),
        ("2026-10-19-12", lambda r: r[0].update(type="TeleportEvent"), "unknown_event_type"),
        ("2013-06-15-12", lambda r: r[0].update(public="yes"), "unknown_type _envelope public"),
        (
            "2026-10-19-12",
            lambda r: next(x for x in r if x["type"] == "PushEvent")["payload"].update(size=1),
            "PushEvent payload.size",
        ),
        ("2015-01-01-12", lambda r: r.append(_records("2013-06-15-12")[0]), "era_shape"),
    ],
)
def test_seeded_drift_fails_loudly(
    data_dir: DataDir, tmp_path: Path, name: str, mutate: object, expect: str
) -> None:
    path = _drifted(tmp_path, name, mutate)
    with pytest.raises(DriftError, match=expect):
        data_dir.parse(path)
    assert not data_dir.events.exists()  # nothing written for a drifted file
    report = orjson.loads(next((data_dir.root / "quality" / "drift").glob("*.json")).read_bytes())
    assert report["findings"] and "someone" not in str(report)


def test_ingest_parses_good_hours_and_fails_drifted_ones(data_dir: DataDir, tmp_path: Path) -> None:
    """Through `ingest_hours(on_file=...)`: a drifted hour becomes `error` (retried next run)."""
    import asyncio
    from datetime import timedelta

    import httpx
    from test_ingest import FakeHost, policy

    from keystone.fetch.client import DiskGuard, PoliteClient
    from keystone.fetch.manifest import Manifest
    from keystone.sources import gharchive as gha

    good = (FIXTURES / "2015-01-01-12.json.gz").read_bytes()
    bad = _drifted(tmp_path, "2015-01-01-12", lambda r: r[0]["payload"].update(brand_new=1))
    host = FakeHost({"2015-01-01-12.json.gz": good, "2015-01-01-13.json.gz": bad.read_bytes()})
    manifest = Manifest(tmp_path / "manifest.duckdb")
    start = datetime(2015, 1, 1, 12, tzinfo=UTC)

    async def go() -> gha.RunSummary:
        async with PoliteClient(policy(), httpx.MockTransport(host), backoff_seconds=0) as c:
            return await gha.ingest_hours(
                c,
                manifest,
                data_dir.sink.cfg,
                start,
                start + timedelta(hours=2),
                tmp_path / "raw",
                DiskGuard(10**9),
                on_file=data_dir.sink,
                now=datetime(2026, 10, 20, tzinfo=UTC),
            )

    try:
        summary = asyncio.run(go())
        rec = manifest.get("https://data.gharchive.org/2015-01-01-13.json.gz")
    finally:
        manifest.close()
    assert summary.counts == {"empty": 1, "error": 1}  # small fixture: near-empty, parsed
    assert rec is not None and rec.error is not None and "DriftError" in rec.error
    assert rec.sha256 is None  # not processed: fetched again on the next run
    assert [p.name for p in data_dir.events.glob("*/*.parquet")] == ["2015-01-01-12.parquet"]
    assert not list((tmp_path / "raw").rglob("*.gz"))  # stream and delete


def test_hours_verified_before_parsing_existed_are_parsed_once(
    data_dir: DataDir, tmp_path: Path
) -> None:
    """A Phase 2 run (no on_file) verified the hour; the first parsing run must not skip it as
    unchanged, and the run after that is a cheap 304 again."""
    import asyncio
    from datetime import timedelta

    import httpx
    from test_ingest import FakeHost, policy

    from keystone.fetch.client import DiskGuard, PoliteClient
    from keystone.fetch.manifest import Manifest
    from keystone.sources import gharchive as gha

    body = (FIXTURES / "2015-01-01-12.json.gz").read_bytes()
    host = FakeHost({"2015-01-01-12.json.gz": body})
    manifest = Manifest(tmp_path / "manifest.duckdb")
    start = datetime(2015, 1, 1, 12, tzinfo=UTC)

    def run(parse: bool) -> gha.RunSummary:
        async def go() -> gha.RunSummary:
            async with PoliteClient(policy(), httpx.MockTransport(host), backoff_seconds=0) as c:
                return await gha.ingest_hours(
                    c,
                    manifest,
                    data_dir.sink.cfg,
                    start,
                    start + timedelta(hours=1),
                    tmp_path / "raw",
                    DiskGuard(10**9),
                    on_file=data_dir.sink if parse else None,
                    is_done=data_dir.sink.is_done if parse else None,
                    now=datetime(2026, 10, 20, tzinfo=UTC),
                )

        return asyncio.run(go())

    try:
        run(parse=False)
        assert not data_dir.events.exists()
        assert run(parse=True).counts == {"unchanged": 1}
        assert data_dir.sink.totals["files"] == 1 and data_dir.sink.is_done(start)
        host.calls.clear()
        assert run(parse=True).counts == {"unchanged": 1}
        assert data_dir.sink.totals["files"] == 1  # not parsed again
        assert [m for m, p in host.calls if p.endswith(".json.gz")] == ["GET"]
    finally:
        manifest.close()
