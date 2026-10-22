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
    published = {a.done_asset, b.incomplete_asset}
    assert plan.pending([a, b], published) == []
    assert plan.pending([a, b], published, redo_incomplete=True) == [b]
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
    assert rec["manifest"][0]["url"].endswith("2026-10-19-12.json.gz")
    assert not list((tmp_path / "work" / "raw").rglob("*.json.gz"))  # raw deleted
    t = totals_of([rec])
    assert t["all"]["events_in_chunk"] == ev["in_chunk"] and t["t"]["days"] == 1
