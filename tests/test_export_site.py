"""Site exports (ADR-060) and the weekly brief (ADR-061)."""

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest

from keystone.export import envelope, site
from keystone.narrate import brief
from keystone.paths import DOCS_DIR

PRODUCED = {"profile": "free", "fallback": True, "run_id": "r1", "keystone_version": "0"}


def _comp(name: str, sub: float | None, weight: float) -> dict[str, Any]:
    return {
        "name": name,
        "available": sub is not None,
        "sub": sub,
        "weight": weight,
        "raw": {"internal": 1},
        "reason": f"{name} reason",
    }


def _score(rid: int, risk: float | None, frag: float | None) -> dict[str, Any]:
    return {
        "repo_id": rid,
        "score_version": "score_v1",
        "cut": "2026-10-01T00:00:00+00:00",
        "fragility": frag,
        "criticality": None if risk is None else 0.5,
        "risk": risk,
        "coverage": 0.9,
        "note": "signals only; not a prediction",
        "components": [
            _comp("concentration", 0.9, 0.3),
            _comp("trend", 0.1, 0.15),
            _comp("osv", None, 0.15),
            _comp("churn", 0.5, 0.15),
        ],
        "criticality_inputs": {"dependent_repos": 10},
    }


def test_benchmark_rows_read_the_measured_ladder() -> None:
    rows = site.benchmark_rows((DOCS_DIR / "benchmarks.md").read_text("utf-8"))
    assert len(rows) == 9 and {r["engine"] for r in rows} == {"DuckDB", "Spark", "Flink"}
    first = rows[0]  # oracle: docs/benchmarks.md section 1, first line
    assert first["rung"].startswith("1 hour") and first["input_rows"] == 46849
    assert first["wall_s"] == [2.9, 1.6] and first["peak_mb"] == [86.1, 86.0]
    assert rows[2]["engine"] == "Flink" and rows[2]["engine_s"] is None  # "-": not measured


def test_radar_ranks_by_risk_and_keeps_raw_internal() -> None:
    live = {"scores": [_score(1, None, 40.0), _score(2, 30.0, 60.0), _score(3, 45.0, 70.0)]}
    radar, pages = site.radar_and_pages(live, {2: site.Repo("o/two", ["npm/two"])})
    assert [r["repo_id"] for r in radar] == [3, 2, 1]
    assert [s["name"] for s in radar[0]["top_signals"]] == ["concentration", "churn", "trend"]
    assert radar[1]["repo_name"] == "o/two" and radar[1]["packages"] == ["npm/two"]
    assert "raw" not in json.dumps(radar + pages)
    doc = envelope.wrap("radar", "v1", radar, produced=PRODUCED, sources=site.SCORE_SOURCES)
    assert doc["row_count"] == 3
    with pytest.raises(jsonschema.ValidationError, match="radar row 0"):
        envelope.wrap(
            "radar",
            "v1",
            [{**radar[0], "actor": "x"}],  # a person field is refused by the row schema
            produced=PRODUCED,
            sources=site.SCORE_SOURCES,
        )


FACTS = {
    "week": "2026-W43",
    "radar": {"repositories": 12, "top": [{"risk": 44.28}]},
    "data_freshness_target": "met",
}


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("12 repositories carry signals; the top risk is 44.28.", None),
        ("Week 43 of 2026 lists 12 repositories.", None),
        ("13 repositories carry signals.", "number not in the facts: 13"),
        ("The top project will be abandoned soon.", "banned wording"),
        ("We predict trouble for 12 repositories.", "banned wording"),
        ("Ask @someone about 12 repositories.", "banned wording"),
        ("", "empty text"),
    ],
)
def test_brief_check(text: str, reason: str | None) -> None:
    got = brief.check(text, FACTS)
    if reason is None:
        assert got is None
    else:
        assert (got or "").startswith(reason)


def _site(tmp_path: Path) -> Path:
    s = tmp_path / "site"
    s.mkdir()
    radar = envelope.wrap(
        "radar",
        "v1",
        [],
        produced=PRODUCED,
        sources=site.SCORE_SOURCES,
        status=("partial data", "partial data: 0.1% of hours"),
    )
    bt = envelope.wrap("backtest", "v1", [], produced=PRODUCED, sources=site.SCORE_SOURCES)
    (s / "radar.json").write_text(json.dumps(radar), encoding="utf-8")
    (s / "backtests.json").write_text(json.dumps(bt), encoding="utf-8")
    pm = {"freshness": {"status": "breach"}}
    (s / "pipeline_metrics.json").write_text(json.dumps(pm), encoding="utf-8")
    return s


def test_brief_rejects_then_accepts_caches_and_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(brief.envelope, "recorded_profile", lambda root: PRODUCED)
    s, cache = _site(tmp_path), tmp_path / "cache"
    answers = [
        "Risk rose by 7 points.",
        "The radar has 0 repositories: partial data, 0.1% of hours.",
    ]
    asked: list[str] = []

    def ask(system: str, user: str) -> str:
        asked.append(user)
        return answers[len(asked) - 1]

    row = brief.write_brief(s, cache, root=tmp_path, model="m:free", ask=ask)
    assert row["outcome"] == "written" and len(asked) == 2
    assert "refused (number not in the facts: 7)" in asked[1]
    doc = json.loads((s / "brief.json").read_text("utf-8"))
    envelope.validate(doc)
    assert doc["rows"][0]["text"] == answers[1]

    again = brief.write_brief(s, cache, root=tmp_path, model="m:free", ask=ask)
    assert again["outcome"] == "cached" and len(asked) == 2  # same input: never asked twice

    skipped = brief.write_brief(
        s,
        cache,
        root=tmp_path,
        model="other:free",
        ask=None,
        skip_reason="OPENROUTER_API_KEY is not set",
    )
    assert skipped["outcome"] == "skipped" and skipped["text"] is None
    assert json.loads((s / "brief.json").read_text("utf-8"))["status"]["state"] == "skipped"


def test_brief_never_publishes_a_failing_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(brief.envelope, "recorded_profile", lambda root: PRODUCED)
    row = brief.write_brief(
        _site(tmp_path),
        tmp_path / "c",
        root=tmp_path,
        model="m:free",
        ask=lambda s, u: "It will be compromised by 99 people.",
    )
    assert row["outcome"] == "rejected" and row["text"] is None
    assert not (tmp_path / "c").exists()  # a rejection is not cached


def test_stage_under_airflow_records_the_orchestrator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from keystone.telemetry import metrics as tm
    from keystone.telemetry.runlog import Stage, read

    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "keystone_pipeline")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "export_site")
    monkeypatch.setenv("AIRFLOW_VERSION", "3.3.2")
    with Stage(tmp_path, "export", "keystone", "0"):
        pass
    (rec,) = read(tmp_path)
    assert rec.detail["orchestrator"] == {"dag": "keystone_pipeline", "task": "export_site"}
    names = {e["name"]: e["version"] for e in tm.engines([rec], rec.run_id)}
    assert names == {"keystone": "0", "airflow": "3.3.2"}
