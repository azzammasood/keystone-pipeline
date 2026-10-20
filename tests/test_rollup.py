"""Rollup logic and the DuckDB engine (no Docker): spec, incremental idempotence, late hours,
re-parsed and removed hours, the ADR-020 overlap rule, and the Flink and registry glue."""

import json
import sys
from pathlib import Path

import duckdb
import httpx
import pytest

from keystone.rollup import flink, stream
from keystone.rollup.contract import fields
from keystone.rollup.duck import DuckRollup
from keystone.rollup.spec import rollups, strip_comments

sys.path.insert(0, str(Path(__file__).parent / "parity"))
import scenario  # noqa: E402

# Engine-specific functions that must never appear in the shared contribution SQL.
NOT_PORTABLE = ("date_trunc", "time_bucket", "tumble", "floor(", "::", "epoch", "strftime")


def _rows(db: Path) -> dict[str, list[tuple[object, ...]]]:
    con = duckdb.connect(str(db), read_only=True)
    out = {
        r.name: con.execute(f"SELECT * FROM {r.name} ORDER BY ALL").fetchall() for r in rollups()
    }
    con.close()
    return out


def _full(tmp: Path, silver: Path) -> dict[str, list[tuple[object, ...]]]:
    db = tmp / "full.duckdb"
    db.unlink(missing_ok=True)
    DuckRollup(db, silver).run()
    return _rows(db)


def test_spec_is_portable_and_complete() -> None:
    names = {r.name: r for r in rollups()}
    assert set(names) == {"repo_hour", "repo_actor_day"}
    hour = names["repo_hour"]
    assert hour.unit == "hour" and hour.keys == ("repo_id", "era", "event_type")
    assert hour.measures[0] == "events" and hour.max_measures == {"fetched_at_max"}
    assert names["repo_actor_day"].unit == "day"
    contract = {f.name for f in fields()}
    for r in rollups():
        body = strip_comments(r.sql).lower()
        assert not any(tok in body for tok in NOT_PORTABLE), r.name
        assert set(r.keys) <= contract  # keys come straight from the event contract


def test_incremental_equals_full_in_any_arrival_order(tmp_path: Path) -> None:
    hours = scenario.hours()
    for label, order in (("arrival", hours), ("reversed", hours[::-1])):
        silver = tmp_path / label / "silver"
        duck = DuckRollup(tmp_path / label / "inc.duckdb", silver)
        for name, rows in order:
            scenario.write_hour(silver, name, rows)
            duck.run()
        assert duck.run().changed_files == 0  # re-run is a no-op
        duck.close()
        assert _rows(tmp_path / label / "inc.duckdb") == _full(tmp_path / label, silver)
    assert _rows(tmp_path / "arrival" / "inc.duckdb") == _rows(tmp_path / "reversed" / "inc.duckdb")


def test_late_hour_corrects_earlier_windows(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    duck = DuckRollup(tmp_path / "r.duckdb", silver)
    hours = dict(scenario.hours())
    for name in ("2026-01-01-23", "2026-01-02-1", "2026-01-02-2", "2026-01-02-3"):
        scenario.write_hour(silver, name, hours[name])
    duck.run()
    stats_before = duck.con.execute(
        "SELECT sum(events) FROM repo_actor_day WHERE window_start = '2026-01-01'"
    ).fetchone()
    scenario.write_hour(silver, "2026-01-01-22", hours["2026-01-01-22"])  # arrives last
    stats = duck.run()
    assert stats.changed_files == 1
    # The late hour touches its own hour window and the 2026-01-01 day window only.
    assert stats.windows == {"repo_hour": 1, "repo_actor_day": 1}
    after = duck.con.execute(
        "SELECT sum(events) FROM repo_actor_day WHERE window_start = '2026-01-01'"
    ).fetchone()
    assert after is not None and stats_before is not None and after[0] > stats_before[0]
    duck.close()


def test_reparsed_and_removed_hours(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    duck = DuckRollup(tmp_path / "r.duckdb", silver)
    for name, rows in scenario.hours():
        scenario.write_hour(silver, name, rows)
    duck.run()
    # Re-parse one hour with different content: its windows are replaced, not added.
    target = "2026-01-02-1"
    rows = dict(scenario.hours())[target][:50]
    # Same raw file and source sha, different silver content (a parser or bot-list fix).
    scenario.write_hour(silver, target, rows)
    assert duck.run().changed_files == 1
    duck.close()
    assert _rows(tmp_path / "r.duckdb") == _full(tmp_path, silver)
    # Remove an hour from silver: its windows are recomputed without it.
    for p in silver.glob(f"*/*/{target}.parquet"):
        p.unlink()
    duck = DuckRollup(tmp_path / "r.duckdb", silver)
    stats = duck.run()
    assert stats.removed_files == 1
    duck.close()
    assert _rows(tmp_path / "r.duckdb") == _full(tmp_path, silver)


def test_e1_overlap_is_dropped_and_eras_stay_apart(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    for name, rows in scenario.hours():
        scenario.write_hour(silver, name, rows)
    duck = DuckRollup(tmp_path / "r.duckdb", silver)
    duck.run()
    con = duck.con
    assert con.execute(
        "SELECT count(*) FROM repo_hour WHERE era = 'E1' AND window_start >= '2015-01-01'"
    ).fetchone() == (0,)
    hour0 = con.execute(
        "SELECT era, sum(events) FROM repo_hour WHERE window_start = '2015-01-01 00:00' GROUP BY 1"
    ).fetchall()
    assert hour0 == [("E2", 82)]  # the E2 hour only (80 random + 2 boundary events)
    total = con.execute("SELECT sum(events) FROM repo_hour").fetchone()
    n_rows = sum(len(rows) for _, rows in scenario.hours())
    assert total == (n_rows - 62,)  # all but the 62 overlapping E1 rows
    # Milliseconds are floored: 23:59:59.9995 stays in the 23h window, not the next one.
    assert con.execute(
        "SELECT count(*) FROM rollup_input WHERE created_at >= '2026-01-02' "
        "AND source_file = 'synthetic-2026-01-01-23.json.gz'"
    ).fetchone() == (0,)
    duck.close()


def test_real_fixture_hours_roll_up(parsed_fixtures: object, tmp_path: Path) -> None:
    root = parsed_fixtures.root  # type: ignore[attr-defined]
    duck = DuckRollup(tmp_path / "r.duckdb", root / "silver" / "gharchive")
    stats = duck.run()
    assert stats.changed_files == len(list((root / "silver" / "gharchive" / "events").glob("*/*")))
    events = duck.con.execute("SELECT sum(events) FROM repo_hour").fetchone()
    silver = duck.con.execute("SELECT count(*) FROM rollup_input").fetchone()
    assert events == silver and events is not None and events[0] > 0
    duck.close()


def test_flink_job_routes_late_rows_and_shares_the_sql() -> None:
    job = flink.render_job("ns", "topic", "stream")
    assert "WATERMARK FOR created_at AS created_at - INTERVAL '1' HOUR" in job
    assert "'scan.watermark.emit.strategy' = 'on-event'" in job
    assert job.count("CURRENT_WATERMARK(created_at)") == len(rollups())
    for r in rollups():
        assert strip_comments(r.sql).strip() in job  # the contribution query, verbatim
        assert f"INSERT INTO ice.`ns`.{r.name}_late" in job
    batch = flink.render_job("ns", "topic", "batch")
    assert "CURRENT_WATERMARK" not in batch and "'execution.runtime-mode' = 'batch'" in batch
    assert "INSERT OVERWRITE ice.`ns`.repo_hour\n" in flink.render_fold("ns")


def test_registry_refuses_incompatible_schema() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/compatibility/"):
            body = json.loads(request.content)
            return httpx.Response(200, json={"is_compatible": "repo_id" in body["schema"]})
        if request.url.path.endswith("/versions"):
            return httpx.Response(200, json={"id": 7})
        return httpx.Response(200, json={})

    reg = stream.Registry(
        client=httpx.Client(transport=httpx.MockTransport(handler), base_url="http://sr")
    )
    assert reg.register("t-value", '{"repo_id": 1}') == 7
    with pytest.raises(stream.IncompatibleSchema):
        reg.register("t-value", "{}")


def test_replay_order_follows_event_time(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    for name, rows in scenario.hours()[::-1]:
        scenario.write_hour(silver, name, rows)
    names = [p.stem for p in stream.files_in_event_order(silver)]
    assert names[:3] == ["2014-12-31-15", "2014-12-31-16", "2015-01-01-0"]
    assert names[3:] == [
        "2026-01-01-22",
        "2026-01-01-23",
        "2026-01-02-0",
        "2026-01-02-1",
        "2026-01-02-2",
        "2026-01-02-3",
    ]
