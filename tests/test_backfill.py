"""Backfill planning and one chunk end to end on a committed fixture hour (ADR-050)."""

import shutil
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

from keystone.backfill import chunk as chunkmod
from keystone.backfill import plan
from keystone.backfill.cli import totals_of
from keystone.fetch.manifest import FileRecord
from keystone.sources import gharchive as gha

FIXTURES = Path(__file__).parent / "fixtures" / "gharchive"


def test_chunks_follow_months_and_split_evenly() -> None:
    wins = {
        "x": (date(2020, 1, 15), date(2020, 3, 1)),
        "live": (date(2021, 1, 1), date(2021, 1, 5)),
    }
    cs = plan.chunks(wins)
    assert [(c.window, c.start, c.end) for c in cs] == [
        ("x", date(2020, 1, 15), date(2020, 2, 1)),
        ("x", date(2020, 2, 1), date(2020, 3, 1)),
        ("live", date(2021, 1, 1), date(2021, 1, 5)),
    ]
    split = plan.chunks({"x": (date(2020, 2, 1), date(2020, 3, 1))}, chunk_days=10)
    assert [(c.end - c.start).days for c in split] == [10, 9, 10]
    assert split[0].done_asset == "chunk-20200201-20200211.json" and split[0].tag == "data-x"


def test_pending_skips_done_and_incomplete_unless_asked() -> None:
    a, b = plan.chunks({"x": (date(2020, 1, 1), date(2020, 3, 1))})
    published = {a.done_asset, a.actor_asset, b.incomplete_asset}
    assert plan.pending([a, b], published) == []
    assert plan.pending([a, b], published, redo_incomplete=True) == [b]
    # Done before ADR-045 (no sealed repo_actor_month): redone.
    assert plan.pending([a, b], {a.done_asset, b.incomplete_asset}) == [a]
    with pytest.raises(ValueError, match="overlap"):
        plan.chunks(
            {"x": (date(2020, 1, 1), date(2020, 3, 1)), "y": (date(2020, 2, 1), date(2020, 4, 1))}
        )


def test_the_configured_windows_are_twelve_months_and_disjoint() -> None:
    wins = plan.windows(date(2026, 10, 22))
    assert wins["event-stream"] == (date(2017, 11, 1), date(2018, 12, 1))
    assert wins["live"] == (date(2025, 10, 1), date(2026, 10, 22))
    assert len(plan.chunks(wins)) == 52


def _fake_ingest(hour_files: list[Path]) -> Any:
    async def fake(client: Any, manifest: Any, cfg: Any, start: datetime, end: datetime,
                   work_dir: Path, disk: Any, *, keep: bool, on_file: Any, now: Any = None,
                   **_: Any) -> gha.RunSummary:  # fmt: skip
        counts: Counter[str] = Counter()
        for src in hour_files:
            hour = datetime.strptime(src.name.removesuffix(".json.gz"), "%Y-%m-%d-%H")
            hour = hour.replace(tzinfo=UTC)
            dest = work_dir / src.name
            shutil.copy(src, dest)
            rec = FileRecord(url=gha.hour_url(cfg, hour), source=gha.SOURCE,
                             fetched_at=datetime.now(UTC), status="new", hour=hour,
                             bytes=dest.stat().st_size, sha256=gha.sha256_file(dest))  # fmt: skip
            on_file(dest, rec)
            manifest.upsert(rec)
            counts["new"] += 1
        return gha.RunSummary(counts, 0, sum(f.stat().st_size for f in hour_files), len(hour_files))

    return fake


class _NoClient:
    def __init__(self, *_: Any) -> None: ...
    async def __aenter__(self) -> "_NoClient":
        return self

    async def __aexit__(self, *_: Any) -> None: ...


def test_one_chunk_publishes_key_free_files_that_add_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        chunkmod.gha, "ingest_hours", _fake_ingest([FIXTURES / "2026-10-19-12.json.gz"])
    )
    monkeypatch.setattr(chunkmod, "PoliteClient", _NoClient)
    out = tmp_path / "out"
    rec = chunkmod.run_chunk("t", date(2026, 10, 19), date(2026, 10, 20), tmp_path / "work", out,
                             workers=1)  # fmt: skip
    assert rec["complete"] and rec["hours"]["parse_failed"] == []
    assert sorted(p.name for p in out.iterdir()) == [
        "contributors-20261019-20261020.parquet",
        "repo_hour-20261019-20261020.parquet",
    ]
    con = duckdb.connect()
    rh = (out / "repo_hour-20261019-20261020.parquet").as_posix()
    co = (out / "contributors-20261019-20261020.parquet").as_posix()
    total = con.execute(f"SELECT sum(events) FROM '{rh}'").fetchone()
    assert total is not None and total[0] == rec["events"]["in_chunk"] > 0
    ev = rec["events"]
    outside = sum(v for k, v in ev.items() if k != "in_chunk")
    assert ev["in_chunk"] + outside == rec["parse"]["events"]  # every parsed event is counted
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{co}'").fetchall()]
    assert cols == ["window_start", "repo_id", "era", "human_actors", "human_events",
                    "top_actor_events"]  # fmt: skip
    assert not any("actor_key" in c for c in cols)
    assert rec["rows"]["repo_hour"] == con.execute(f"SELECT count(*) FROM '{rh}'").fetchone()[0]  # type: ignore[index]
    # repo_actor_month (salted keys) is written under work/private only, never into out/, for
    # the workflow to encrypt (ADR-045); its sums match the human events of the contributors.
    am = (tmp_path / "work" / "private" / "repo_actor_month-20261019-20261020.parquet").as_posix()
    acols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{am}'").fetchall()]
    assert acols[:3] == ["month", "repo_id", "actor_key"] and "login" not in " ".join(acols)
    keys = con.execute(f"SELECT DISTINCT length(actor_key) FROM '{am}'").fetchall()
    assert keys == [(32,)]  # salted HMAC keys only (ADR-019)
    am_events = con.execute(f"SELECT sum(events) FROM '{am}'").fetchone()
    co_events = con.execute(f"SELECT sum(human_events) FROM '{co}'").fetchone()
    assert am_events == co_events and rec["rows"]["repo_actor_month_sealed"] > 0
    assert rec["manifest"][0]["url"].endswith("2026-10-19-12.json.gz")
    assert not list((tmp_path / "work" / "raw").rglob("*.json.gz"))  # raw deleted
    t = totals_of([rec])
    assert t["all"]["events_in_chunk"] == ev["in_chunk"] and t["t"]["days"] == 1


def test_backfill_reports_rare_types_but_live_parse_fails(data_dir: Any) -> None:
    """ADR-051: in report mode an unknown event type is recorded and the hour kept; the
    default (live ingest) still fails the hour (ADR-022)."""
    from keystone.parsers.drift import DriftError

    hour = FIXTURES / "2026-10-19-12.json.gz"
    reg = data_dir.sink.registry
    assert reg is not None
    dropped = sorted(reg["E3"])[0]
    data_dir.sink.registry = {**reg, "E3": {k: v for k, v in reg["E3"].items() if k != dropped}}
    with pytest.raises(DriftError, match="unknown_event_type"):
        data_dir.parse(hour)
    assert not list(data_dir.events.rglob("*.parquet"))
    data_dir.sink.drift_mode = "report"
    data_dir.parse(hour)
    assert list(data_dir.events.rglob("*.parquet"))  # the hour is kept
    assert any(
        line.startswith(f"unknown_event_type {dropped}") for line in data_dir.sink.last_drift
    )
    summary = chunkmod._drift_summary([{"drift": data_dir.sink.last_drift}, {"drift": []}])
    assert summary == {f"unknown_event_type {dropped}": 1}


def test_totals_skip_superseded_incomplete_records(tmp_path: Path) -> None:
    from keystone.backfill.cli import record_files

    for n in ("chunk-a.json", "chunk-a.incomplete.json", "chunk-b.incomplete.json"):
        (tmp_path / n).write_text("{}")
    assert [p.name for p in record_files(tmp_path)] == ["chunk-a.json", "chunk-b.incomplete.json"]


def test_drift_review_learns_by_chunk_era() -> None:
    from keystone.backfill import drift as bd

    def rec(start: str, end: str, drift: dict[str, int]) -> dict[str, object]:
        return {"chunk": start, "start": start, "end": end, "hours": {"drift": drift}}

    rev = bd.review(
        [
            rec("2018-11-01T00:00:00+00:00", "2018-12-01T00:00:00+00:00",
                {"unknown_field IssuesEvent payload.issue.draft bool": 2,
                 "unknown_event_type FollowEvent": 1}),
            rec("2025-10-01T00:00:00+00:00", "2025-10-17T00:00:00+00:00",
                {"unknown_field IssuesEvent payload.x int": 1}),
        ]
    )  # fmt: skip
    assert rev.ambiguous == ["2025-10-01T00:00:00+00:00"]
    reg: dict[str, dict[str, dict[str, list[str]]]] = {"E2": {"IssuesEvent": {}}}
    assert bd.learn(rev, reg) == ["E2 IssuesEvent payload.issue.draft bool"]
    assert "FollowEvent" not in reg["E2"]  # unknown types are reported, never learned


def test_split_from_keeps_earlier_months_whole() -> None:
    from datetime import date

    from keystone.backfill import plan as planmod

    wins = {"a": (date(2022, 12, 1), date(2023, 2, 1))}
    names = [c.name for c in planmod.chunks(wins, 16, date(2023, 1, 1))]
    assert names == ["20221201-20230101", "20230101-20230117", "20230117-20230201"]
