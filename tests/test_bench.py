"""Phase 9 scale ladder and storage measurement (`keystone bench`)."""

import json
from dataclasses import asdict
from datetime import date
from pathlib import Path

import duckdb
import pytest

from keystone.bench import ladder, storage
from keystone.rollup.spec import rollups


def test_scopes_parse_and_select_silver_hours(parsed_fixtures: object) -> None:
    silver = parsed_fixtures.root / "silver"  # type: ignore[attr-defined]
    assert ladder.parse_scope("hour=2026-10-19-12").name == "hour-2026-10-19-12"
    for bad in ("hour=2026-10-19", "week=2026-10", "day=19-10-2026"):
        with pytest.raises(ValueError):
            ladder.parse_scope(bad)
    hour = ladder.parse_scope("hour=2026-10-19-12").files(silver)
    assert [p.name for p in hour] == ["2026-10-19-12.parquet"]
    assert ladder.parse_scope("day=2026-10-19").files(silver) == hour
    assert len(ladder.parse_scope("month=2012-06").files(silver)) == 1
    assert ladder.parse_scope("day=2020-01-01").files(silver) == []


def test_duckdb_rung_rolls_up_every_input_event(parsed_fixtures: object, tmp_path: Path) -> None:
    silver = parsed_fixtures.root / "silver"  # type: ignore[attr-defined]
    prep = ladder.prepare(ladder.parse_scope("day=2026-10-19"), silver, tmp_path)
    assert prep.rows > 0 and prep.silver_files == 1
    result = ladder.run_rung("duckdb", prep, tmp_path, timeout=300)
    assert result.cost_usd == 0.0 and result.peak_memory_bytes and result.wall_seconds > 0
    for r in rollups():  # every rollup counts each (E3) input event once
        assert result.outputs[r.name].rows > 0
        assert result.outputs[r.name].sums["events"] == prep.rows
    ladder.record([result], tmp_path)
    assert ladder.parity_recorded(tmp_path) == {prep.scope: []}


def test_recorded_parity_flags_a_disagreeing_engine(tmp_path: Path) -> None:
    out = ladder.Output(1, 10, 5, {"events": 7})
    base = dict(scope="s", measured_at="t", input_rows=1, input_bytes=1, silver_files=1,
                silver_bytes=1, wall_seconds=1.0, engine_seconds=None, peak_memory_bytes=None,
                peak_memory_method="-")  # fmt: skip
    good = ladder.RunResult(engine="duckdb", outputs={"repo_hour": out}, **base)  # type: ignore[arg-type]
    bad = ladder.RunResult(  # type: ignore[arg-type]
        engine="spark", outputs={"repo_hour": ladder.Output(1, 10, 5, {"events": 8})}, **base
    )
    ladder.record([good, bad], tmp_path)
    assert ladder.parity_recorded(tmp_path) == {"s": ["spark"]}
    assert ladder.parity([good, bad]) == {"s": False}


def test_engine_jobs_share_the_rollup_sql() -> None:
    spark, flink = ladder.render_spark("day-x"), ladder.render_flink("day-x")
    for job in (spark, flink):
        assert not any(line.lstrip().startswith("--") for line in job.splitlines())
        for r in rollups():
            assert f"{r.name}" in job and "human_events" in job
    assert "/bench/in/day-x" in spark and "INSERT OVERWRITE DIRECTORY" in spark
    assert "'format' = 'parquet'" in flink and "TUMBLE(TABLE repo_hour_contrib" in flink
    assert "'execution.runtime-mode' = 'batch'" in flink


def test_storage_ratios_and_projection_are_estimates(tmp_path: Path) -> None:
    assert storage.era_of(date(2012, 3, 9)) == "E0" and storage.era_of(date(2012, 3, 10)) == "E1"
    assert storage.era_of(date(2025, 10, 7)) == "E2" and storage.era_of(date(2025, 10, 8)) == "E3"
    sweep = tmp_path / "head_sweep.csv"
    sweep.write_text("hour,status,bytes,last_modified\n2011-03-15-0,200,100,x\n"
                     "2011-03-15-6,200,300,x\n2020-06-15-0,404,,x\n", encoding="utf-8")  # fmt: skip
    months = storage.raw_per_day_by_month(sweep)
    assert months == {"2011-03": 24 * 200}
    samples = [
        storage.SampleDate("2011-02-12", "E0", 24, 10, 1000, 300, {"a": 40, "b": 10}, {}),
        storage.SampleDate("2011-02-13", "E0", 1, 10, 1000, 100),
    ]
    sr = storage.era_ratios(samples, "silver")["E0"]
    assert (sr.pooled, sr.low, sr.high) == (0.2, 0.1, 0.3)
    rr = storage.era_ratios(samples, "rollup")["E0"]
    assert rr.pooled == 0.05 and rr.dates == ["2011-02-12"]
    eras = [e for e, _ in storage.ERA_STARTS]
    full = samples + [storage.SampleDate("x", e, 24, 1, 1000, 100, {"a": 10}, {}) for e in eras[1:]]
    p = storage.project(full, months, date(2011, 2, 22))
    assert p["label"] == "estimate"
    y = p["years"]["2011"]
    assert y["days"] == 10 and y["per_day"]["raw"] == 4800
    assert y["per_day"]["silver"] == pytest.approx(4800 * 0.2)
    assert p["windows"]["full archive"]["total"]["rollup"] == pytest.approx(10 * 4800 * 0.05)


def test_storage_samples_join_raw_and_silver(parsed_fixtures: object) -> None:
    root = parsed_fixtures.root  # type: ignore[attr-defined]
    (root / "manifest").mkdir()
    con = duckdb.connect(str(root / "manifest" / "manifest.duckdb"))
    con.execute("CREATE TABLE files (url VARCHAR, source VARCHAR, bytes BIGINT)")
    stems = [p.stem for p in (root / "silver" / "gharchive" / "events").glob("*/*.parquet")]
    for s in stems:
        con.execute("INSERT INTO files VALUES (?, 'gharchive', 1000)",
                    [f"https://data.gharchive.org/{s}.json.gz"])  # fmt: skip
    con.close()
    samples = storage.sample_dates(root)
    assert sum(s.hours for s in samples) == len(stems)
    assert all(s.raw_bytes == 1000 * s.hours and s.silver_bytes > 0 for s in samples)
    assert json.dumps([asdict(s) for s in samples])  # serialisable for storage.json
