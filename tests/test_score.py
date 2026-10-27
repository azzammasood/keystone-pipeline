"""Phase 8: score_v1 components, point-in-time backtests, leakage and sensitivity checks."""

import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest
from typer.testing import CliRunner

from keystone.cli import app
from keystone.paths import REPO_ROOT
from keystone.score import backtest as bt
from keystone.score import model, signals
from keystone.score.signals import Window

CUT = datetime(2022, 1, 1, tzinfo=UTC)
FRAGILE, HEALTHY = 1, list(range(2, 22))


def _rollup(path: Path, poison: bool) -> None:
    """A full 12-month window before CUT: a filler repo every hour, 20 steady repositories with
    five actors each, and repo 1 kept up by one actor whose activity falls off. With `poison`,
    rows at and after the cut that would change every signal of repo 1 if they leaked in."""
    con = duckdb.connect(str(path))
    con.execute("SET TimeZone = 'UTC'")
    healthy = ", ".join(str(r) for r in HEALTHY)
    con.execute(
        f"""
        CREATE TABLE repo_hour AS
        WITH hours AS (
          SELECT CAST(ts AS TIMESTAMPTZ) AS ts FROM generate_series(
            TIMESTAMP '2021-01-01', TIMESTAMP '2022-02-28 23:00', INTERVAL 1 HOUR) t(ts)
        ), days AS (SELECT ts FROM hours WHERE hour(ts) = 12)
        SELECT ts AS window_start, 999::BIGINT AS repo_id, 'E2' AS era,
          'WatchEvent' AS event_type, 1::BIGINT AS events, 1::BIGINT AS human_events,
          0::BIGINT AS opened, 0::BIGINT AS closed, 0::BIGINT AS published, 0::BIGINT AS added
        FROM hours WHERE ts < TIMESTAMPTZ '2022-01-01'
        UNION ALL
        SELECT ts, r, 'E2', 'PushEvent', 5, 5, 0, 0, 0, 0
        FROM days, (SELECT unnest([{healthy}]) AS r) WHERE ts < TIMESTAMPTZ '2022-01-01'
        UNION ALL
        SELECT ts, r, 'E2', 'IssuesEvent', 2, 2, 1, 1, 0, 0
        FROM days, (SELECT unnest([{healthy}]) AS r)
        WHERE dayofweek(ts) = 1 AND ts < TIMESTAMPTZ '2022-01-01'
        UNION ALL
        SELECT ts, r, 'E2', 'ReleaseEvent', 1, 1, 0, 0, 1, 0
        FROM days, (SELECT unnest([{healthy}]) AS r)
        WHERE day(ts) = 1 AND ts < TIMESTAMPTZ '2022-01-01'
        UNION ALL
        SELECT ts, {FRAGILE}, 'E2', 'PushEvent', 3, 3, 0, 0, 0, 0 FROM days
        WHERE ts < TIMESTAMPTZ '2021-10-01'
           OR (ts < TIMESTAMPTZ '2022-01-01' AND day(ts) = 10)
        UNION ALL
        SELECT ts, {FRAGILE}, 'E2', 'IssuesEvent', 1, 1, 1, 0, 0, 0 FROM days
        WHERE dayofweek(ts) = 3 AND ts < TIMESTAMPTZ '2022-01-01'
        UNION ALL
        SELECT TIMESTAMPTZ '2021-02-03 12:00', {FRAGILE}, 'E2', 'ReleaseEvent', 1, 1, 0, 0, 1, 0
        UNION ALL
        SELECT TIMESTAMPTZ '2021-09-15 12:00', {FRAGILE}, 'E2', 'MemberEvent', 1, 1, 0, 0, 0, 1
        """
    )
    con.execute(
        f"""
        CREATE TABLE repo_actor_day AS
        WITH days AS (
          SELECT CAST(ts AS TIMESTAMPTZ) AS ts FROM generate_series(
            TIMESTAMP '2021-01-01', TIMESTAMP '2021-12-31', INTERVAL 1 DAY) t(ts)
        )
        SELECT ts AS window_start, r AS repo_id, 'k' || r || '-' || a AS actor_key,
          'id' AS actor_key_kind, 'E2' AS era, 1::BIGINT AS events, 1::BIGINT AS pushes,
          0::BIGINT AS prs_closed, 0::BIGINT AS prs_merged, 0::BIGINT AS releases,
          0::BIGINT AS issues_closed
        FROM days, (SELECT unnest([{healthy}]) AS r), (SELECT unnest(range(5)) AS a)
        UNION ALL
        SELECT ts, {FRAGILE}, 'solo', 'id', 'E2', 3, 3, 0, 0, 0, 0 FROM days
        WHERE ts < TIMESTAMPTZ '2021-10-01' OR day(ts) = 10
        UNION ALL
        SELECT ts, {FRAGILE}, 'gone' || (month(ts) % 3), 'id', 'E2', 1, 1, 0, 0, 0, 0 FROM days
        WHERE month(ts) <= 3 AND day(ts) = 5
        """
    )
    if poison:
        con.execute(
            f"""
            INSERT INTO repo_hour BY NAME
            SELECT ts AS window_start, {FRAGILE} AS repo_id, 'E2' AS era, et AS event_type,
              500 AS events, 500 AS human_events, 50 AS opened, 400 AS closed,
              3 AS published, 4 AS added
            FROM (SELECT unnest([TIMESTAMPTZ '2022-01-01 00:00', TIMESTAMPTZ '2022-01-15 12:00',
                                 TIMESTAMPTZ '2022-02-20 12:00']) AS ts),
                 (SELECT unnest(['PushEvent', 'IssuesEvent', 'ReleaseEvent', 'MemberEvent'])
                  AS et)
            """
        )
        con.execute(
            f"""
            INSERT INTO repo_actor_day BY NAME
            SELECT TIMESTAMPTZ '2022-01-01 00:00' + INTERVAL (d) DAY AS window_start,
              {FRAGILE} AS repo_id, 'new' || a AS actor_key, 'id' AS actor_key_kind,
              'E2' AS era, 50 AS events, 50 AS pushes, 0 AS prs_closed, 0 AS prs_merged,
              0 AS releases, 0 AS issues_closed
            FROM range(40) r(d), range(30) s(a)
            """
        )
    con.close()


def _warehouse(path: Path, poison: bool) -> None:
    con = duckdb.connect(str(path))
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE SCHEMA marts; CREATE SCHEMA staging")
    con.execute(
        "CREATE TABLE marts.map_package_repo AS SELECT * FROM (VALUES "
        "('npm/p1', 1::BIGINT, 'accepted'), ('npm/p2', 2, 'auto_exact'), "
        "('npm/p3', 3, 'rejected')) t(package_key, repo_id, status)"
    )
    rows = [
        ("V1", "npm/p1", "2021-06-01", None, "HIGH"),
        ("V2", "npm/p1", "2021-03-01", "2022-02-01", "LOW"),  # withdrawn after the cut
        ("V3", "npm/p2", "2021-04-01", "2021-05-01", "LOW"),  # withdrawn before the cut
        ("MAL-1", "npm/p1", "2021-07-01", None, "CRITICAL"),  # malicious record: left out
    ]
    if poison:
        rows += [("V4", "npm/p1", "2022-01-01", None, "CRITICAL")]
        rows += [("V5", "npm/p1", "2022-02-10", None, "CRITICAL")]
    con.execute(
        "CREATE TABLE marts.fct_vuln (vuln_id VARCHAR, package_key VARCHAR, "
        "published_at TIMESTAMPTZ, withdrawn_at TIMESTAMPTZ, severity_label VARCHAR, "
        "is_malicious BOOLEAN)"
    )
    rows = [(*r, r[0].startswith("MAL")) for r in rows]
    con.executemany("INSERT INTO marts.fct_vuln VALUES (?, ?, ?, ?, ?, ?)", rows)
    con.execute(
        "CREATE TABLE staging.stg_registry__packages AS SELECT * FROM (VALUES "
        "('npm/p1', true, 120000::BIGINT, TIMESTAMPTZ '2026-10-21'), "
        "('npm/p2', true, 9::BIGINT, TIMESTAMPTZ '2026-10-21')) "
        "t(package_key, found, dependent_repos, fetched_at)"
    )
    con.close()


@pytest.fixture
def data(tmp_path: Path) -> dict[str, Path]:
    out = {}
    for name, poison in (("clean", False), ("poison", True)):
        _rollup(tmp_path / f"{name}.duckdb", poison)
        _warehouse(tmp_path / f"{name}_wh.duckdb", poison)
        out[name] = tmp_path / f"{name}.duckdb"
        out[f"{name}_wh"] = tmp_path / f"{name}_wh.duckdb"
    return out


INC = bt.Incident("t", "2022-01", [{"name": "x/fragile", "id": FRAGILE}], ["https://x"], "test")


def _run(rollup: Path, wh: Path, inc: bt.Incident = INC, months: int = 0) -> dict[str, Any]:
    inp = signals.open_inputs(rollup, wh)
    try:
        return bt.run_incident(inp, inc, months)
    finally:
        inp.close()


def test_window_is_a_utc_month_start() -> None:
    w = Window(CUT)
    assert (w.start, w.half, w.recent) == (
        datetime(2021, 1, 1, tzinfo=UTC),
        datetime(2021, 7, 1, tzinfo=UTC),
        datetime(2021, 10, 1, tzinfo=UTC),
    )
    assert w.hours == 365 * 24
    with pytest.raises(ValueError):
        Window(datetime(2022, 1, 2, tzinfo=UTC))


def test_maintenance_version_follows_the_eras() -> None:
    assert (
        signals.maint_version(signals.spans("schema_eras.csv", "era", Window(CUT).start, CUT))
        == "maint_v1"
    )
    e3 = datetime(2026, 10, 1, tzinfo=UTC)
    assert signals.spans("schema_eras.csv", "era", Window(e3).start, e3) == ["E2", "E3"]
    assert signals.maint_version(["E2", "E3"]) == "maint_v0"
    assert signals.maint_version(["E3"]) == "maint_v2"
    assert signals.maint_version(["E0", "E1"]) == "maint_v0"
    live = signals.spans("completeness_periods.csv", "period", Window(e3).start, e3)
    assert len(live) > 1  # the live window crosses completeness periods: no activity trend


def test_signals_and_explained_components(data: dict[str, Path]) -> None:
    inp = signals.open_inputs(data["clean"], data["clean_wh"])
    try:
        assert signals.hour_coverage(inp, Window(CUT)) == 1.0
        s = signals.compute(inp, Window(CUT), [FRAGILE, 2, 3])
    finally:
        inp.close()
    f = s[FRAGILE]
    assert f["maint_version"] == "maint_v1" and f["one_completeness_period"]
    assert 0.99 < f["top1"] < 1 and f["maint_actors"] == 4 and f["top3"] == round(830 / 831, 4)
    assert (f["first_half_actors"], f["departed"], f["new_actors"]) == (4, 3, 0)
    assert (f["vulns"], f["vulns_high"], f["mapped_packages"]) == (2, 1, 1)
    assert (f["members_added_6m"], f["releases"]) == (1, 1)
    assert s[2]["vulns"] == 0 and 3 not in s or "vulns" not in s[3]  # rejected mapping ignored
    sc = model.score(FRAGILE, CUT.isoformat(), f)
    by = {c.name: c for c in sc.components}
    assert by["concentration"].sub == f["top1"] and "100%" in by["concentration"].reason
    assert by["contributor_churn"].sub == 0.75
    assert by["activity_trend"].available and by["activity_trend"].sub > 0.5
    assert by["responsiveness"].sub == 1.0 and by["members_added"].sub == 1.0
    assert by["vulnerabilities"].sub == pytest.approx(1.5 / 5)
    assert sc.coverage == 1.0 and sc.fragility is not None and sc.risk is None
    healthy = model.score(2, CUT.isoformat(), s[2])
    assert healthy.fragility is not None and healthy.fragility < sc.fragility


def test_unavailable_components_are_reasoned_and_withhold_fragility() -> None:
    sc = model.score(7, "c", {"actor_data": False, "vuln_data": False})
    assert sc.fragility is None and "insufficient data" in sc.note
    assert all(c.reason for c in sc.components)
    assert {c.name for c in sc.components if not c.available} == set(model.WEIGHTS) - {
        "members_added"
    }
    assert model.criticality(None) is None and model.criticality(10**6 - 1) == 1.0


def test_backtest_flags_and_reports(data: dict[str, Path]) -> None:
    res = _run(data["clean"], data["clean_wh"])
    p = res["points"][-1]
    assert p["status"] == "scored" and p["leakage_check"] == "pass"
    r = p["repos"][0]
    assert r["flagged"] and r["percentile"] == 100.0 and res["outcome"] == "flagged"
    # Criticality is never part of a backtest (snapshot, not point-in-time).
    assert r["score"]["criticality"] is None and r["score"]["risk"] is None
    sens = res["sensitivity"]
    assert sens["variants"] == 1 + 3 * len(model.WEIGHTS)
    assert (
        sens["per_repo"][FRAGILE]["flagged_variants"]
        == sens["per_repo"][FRAGILE]["scored_variants"]
    )


def test_a_miss_is_reported_as_a_miss(data: dict[str, Path]) -> None:
    inc = bt.Incident("m", "2022-01", [{"name": "x/steady", "id": 2}], ["https://x"], "")
    res = _run(data["clean"], data["clean_wh"], inc)
    assert res["outcome"] == "miss"
    md = bt.report_md(
        {
            "score_version": "score_v1",
            "generated_at": "2026-10-22",
            "rules": {"flag_percentile": 90, "active_events": 100, "min_hour_coverage": 0.9},
            "inputs": {"actor_data": True, "vuln_data": True},
            "incidents": [res],
        }
    )
    assert "| m | 2022-01-01 | miss |" in md and "not a prediction" in md


def test_no_leakage_from_rows_at_or_after_the_cut(data: dict[str, Path]) -> None:
    clean = _run(data["clean"], data["clean_wh"])
    poisoned = _run(data["poison"], data["poison_wh"])
    a, b = clean["points"][-1]["repos"][0], poisoned["points"][-1]["repos"][0]
    assert a["score"] == b["score"] and a["percentile"] == b["percentile"]
    assert poisoned["points"][-1]["leakage_check"] == "pass"


def test_leakage_check_catches_a_leaky_signal(
    data: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    def leaky(inp: signals.Inputs, w: Window, ids: list[int] | None = None) -> Any:
        return inp.con.execute("SELECT SUM(events) FROM repo_hour WHERE repo_id = 1").fetchall()

    inp = signals.open_inputs(data["poison"], data["poison_wh"])
    try:
        assert bt.leakage_check(inp, Window(CUT), [FRAGILE])
        monkeypatch.setattr(signals, "compute", leaky)
        assert not bt.leakage_check(inp, Window(CUT), [FRAGILE])
    finally:
        inp.close()


def test_pending_and_partial_data(data: dict[str, Path]) -> None:
    res = _run(data["clean"], data["clean_wh"], months=2)
    assert [p["status"] for p in res["points"]] == ["partial data", "scored", "scored"]
    far = bt.Incident("f", "2019-01", [{"name": "x/y", "id": 1}], ["https://x"], "")
    res = _run(data["clean"], data["clean_wh"], far)
    assert res["outcome"] == "pending data" and res["points"][0]["leakage_check"] is None


def test_parquet_directory_inputs_match_duckdb(data: dict[str, Path], tmp_path: Path) -> None:
    d = tmp_path / "pq"
    con = duckdb.connect(str(data["clean"]), read_only=True)
    for t in ("repo_hour", "repo_actor_day"):
        (d / t).mkdir(parents=True)
        con.execute(f"COPY {t} TO '{(d / t / 'part.parquet').as_posix()}' (FORMAT parquet)")
    con.close()
    a = _run(data["clean"], data["clean_wh"])
    b = _run(d, data["clean_wh"])
    assert a["points"][-1]["repos"] == b["points"][-1]["repos"]


def test_config_incidents_have_sources_and_month_cuts() -> None:
    incs = bt.load_incidents()
    assert {i.id for i in incs} == {"event-stream", "colors-faker", "xz"}
    for i in incs:
        assert i.sources and all(s.startswith("https://") for s in i.sources)
        assert i.repos and Window(i.cut)


def test_cli_live_and_explain(data: dict[str, Path], tmp_path: Path) -> None:
    out = tmp_path / "live.json"
    args = [
        "score",
        "live",
        "--month",
        "2022-01",
        "--rollup",
        str(data["clean"]),
        "--warehouse",
        str(data["clean_wh"]),
        "--out",
        str(out),
    ]
    r = CliRunner().invoke(app, args)
    assert r.exit_code == 0, r.output
    assert "2 of 2 repositories scored" in r.output
    r = CliRunner().invoke(app, ["score", "explain", "1", "--path", str(out)])
    assert r.exit_code == 0 and "concentration" in r.output and "risk" in r.output
    import json

    row = next(x for x in json.loads(out.read_text())["scores"] if x["repo_id"] == 1)
    assert row["criticality"] == pytest.approx(0.8466, abs=1e-4) and row["risk"] is not None


def test_empty_scope_and_actor_month_parquet(data: dict[str, Path], tmp_path: Path) -> None:
    inp = signals.open_inputs(data["clean"], data["clean_wh"])
    try:
        assert signals.compute(inp, Window(CUT), []) == {}
        assert signals.criticality_inputs(inp, []) == {}
        month = inp.con.execute("SELECT * FROM repo_actor_month").fetchdf()
        hour = inp.con.execute("SELECT * FROM repo_hour").fetchdf()
        want = signals.compute(inp, Window(CUT), [FRAGILE])
    finally:
        inp.close()
    d = tmp_path / "pqm"
    for name, df in (("repo_hour", hour), ("repo_actor_month", month)):
        (d / name).mkdir(parents=True)
        df.to_parquet(d / name / "part.parquet")
    inp = signals.open_inputs(d, data["clean_wh"])
    try:
        assert signals.compute(inp, Window(CUT), [FRAGILE]) == want
    finally:
        inp.close()


def test_e3_window_uses_pr_closes_not_merges(tmp_path: Path) -> None:
    db = tmp_path / "e3.duckdb"
    con = duckdb.connect(str(db))
    con.execute("SET TimeZone = 'UTC'")
    con.execute(
        "CREATE TABLE repo_hour AS SELECT TIMESTAMPTZ '2026-08-01 12:00' AS window_start, "
        "1::BIGINT AS repo_id, 'E3' AS era, 'PushEvent' AS event_type, 1::BIGINT AS events, "
        "1::BIGINT AS human_events, 0::BIGINT AS opened, 0::BIGINT AS closed, "
        "0::BIGINT AS published, 0::BIGINT AS added"
    )
    con.execute(
        "CREATE TABLE repo_actor_month AS SELECT * FROM (VALUES "
        "(DATE '2026-08-01', 1::BIGINT, 'a', 20::BIGINT, 0::BIGINT, 0::BIGINT, 20::BIGINT, "
        "0::BIGINT, 0::BIGINT), "
        "(DATE '2026-08-01', 1::BIGINT, 'b', 20::BIGINT, 0::BIGINT, 99::BIGINT, 0::BIGINT, "
        "0::BIGINT, 0::BIGINT)) t(month, repo_id, actor_key, events, pushes, prs_merged, "
        "prs_closed, releases, issues_closed)"
    )
    con.close()
    inp = signals.open_inputs(db)
    try:
        cut = datetime(2026, 10, 1, tzinfo=UTC)  # window 2025-10 to 2026-09 crosses E2/E3
        s = signals.compute(inp, Window(cut), [1])[1]
        assert s["maint_version"] == "maint_v0" and s["maint_actions"] == 0
    finally:
        inp.close()
    assert "prs_merged" not in signals.MAINT_SQL["maint_v2"]
    assert "prs_merged" not in signals.MAINT_SQL["maint_v0"]


@pytest.mark.skipif(
    sys.platform == "win32" or not shutil.which("openssl"), reason="needs bash and openssl"
)
def test_actor_data_roundtrip(data: dict[str, Path], tmp_path: Path) -> None:
    """The backtest workflow decrypts what the backfill encrypts (actor-data.sh, ADR-045)."""
    script = str(REPO_ROOT / ".github" / "scripts" / "actor-data.sh")
    env = {**os.environ, "KEYSTONE_ACTOR_DATA_KEY": "test-key-not-a-secret"}
    (tmp_path / "enc").mkdir()
    plain = tmp_path / "m.parquet"
    con = duckdb.connect(str(data["clean"]), read_only=True)
    con.execute(f"COPY (SELECT 1 AS x) TO '{plain.as_posix()}' (FORMAT parquet)")
    con.close()
    cipher = tmp_path / "enc" / "repo_actor_month-1.parquet.enc"
    subprocess.run(["bash", script, "encrypt", str(plain), str(cipher)], env=env, check=True)
    assert b"PAR1" not in cipher.read_bytes()[:16]
    out = tmp_path / "out"
    subprocess.run(
        ["bash", script, "decrypt", str(tmp_path / "enc"), str(out)], env=env, check=True
    )
    assert (out / "repo_actor_month-1.parquet").read_bytes() == plain.read_bytes()


def test_filtered_compute_equals_the_reference_of_the_full_compute(data: dict[str, Path]) -> None:
    inp = signals.open_inputs(data["clean"], data["clean_wh"])
    w = signals.Window(INC.cut)
    full = signals.compute(inp, w)
    for k in (1, 50, 10_000):
        keep = sorted(full)[:1]
        part = signals.compute(inp, w, min_human_events=k, keep=keep)
        want = {r for r, s in full.items() if s.get("human_events", 0) >= k or r in keep}
        assert set(part) == want
        assert all(part[r] == full[r] for r in part)
    inp.close()
