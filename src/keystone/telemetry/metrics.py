"""`pipeline_metrics.json` (brief section 9c): measured numbers only, validated by JSON Schema.

Sources: the stage run log (telemetry/runs.jsonl), the data itself (silver and rollup counts,
newest data hour, quarantine rows) and the metadata export (lineage size, documented share).
Rate = records out / the stage's wall-clock seconds in that run. Nothing is estimated; a value
that was not measured is null, and the schema allows null only there.
"""

import json
import shutil
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import jsonschema

from keystone.export.meta import Metadata
from keystone.paths import CONTRACTS_DIR, REPO_ROOT
from keystone.telemetry.runlog import StageRecord, iter_runs, read, record_gauges

SCHEMA_PATH = CONTRACTS_DIR / "pipeline_metrics.schema.json"
HISTORY_RUNS = 48
DAILY_DAYS = 30
FULL_SERVICES = {"redpanda", "flink"}

# What each service does in Keystone (engines[].role).
ROLES = {
    "gharchive-fetch": "polite GH Archive download, MD5/SHA-256 verification",
    "keystone-parser": "era-aware parsing to typed Parquet",
    "duckdb": "free rollup engine",
    "redpanda": "event stream (Kafka API, schema registry)",
    "flink": "full rollup engine (stream and batch)",
    "dbt": "warehouse models and tests",
    "duckdb-iceberg": "Iceberg publish of the marts",
    "datacontract-cli": "ODCS data contract tests",
    "great-expectations": "per-era expectation suites",
    "openlineage-dbt": "dbt lineage events (dbt-ol)",
    "marquez": "OpenLineage server",
    "datahub": "metadata catalog (lineage, glossary, owners, assertions)",
    "grafana": "dashboards",
    "prometheus": "metrics store",
    "otel-collector": "OpenTelemetry metrics pipeline",
    "spark": "lakehouse reader (Iceberg)",
    "trino": "lakehouse reader (Iceberg)",
    "keystone": "metadata export and metrics",
}

# Stages whose records_out are GH Archive events parsed.
EVENT_STAGES = {"ingest", "parse"}


def _hours(td: timedelta) -> float:
    return round(td.total_seconds() / 3600, 3)


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _stage(r: StageRecord) -> dict[str, Any]:
    return {
        "name": r.stage,
        "service": r.service,
        "version": r.version,
        "records_in": r.records_in,
        "records_out": r.records_out,
        "bytes_in": r.bytes_in,
        "bytes_out": r.bytes_out,
        "duration_seconds": round(r.seconds, 3),
        "records_per_second": _round(r.records_per_second),
        "mb_per_second": _round(r.mb_per_second),
        "status": r.status,
        "started_at": r.started_at,
    }


def _round(v: float | None) -> float | None:
    return None if v is None else round(v, 3)  # a measured 0.0 stays 0.0


def _last_per_stage(recs: list[StageRecord]) -> list[StageRecord]:
    """The last attempt of each stage, in order of first appearance: a stage re-run under the
    same run id (a repeated check) replaces its earlier attempt."""
    last: dict[str, StageRecord] = {}
    for r in recs:
        last.pop(r.stage, None)
        last[r.stage] = r
    order = list(dict.fromkeys(r.stage for r in recs))
    return [last[s] for s in order]


def _sum_or_none(values: list[int | None]) -> int | None:
    """Sum of measured values; None when nothing was measured (not a measured zero)."""
    known = [v for v in values if v is not None]
    return sum(known) if known else None


def engines(records: list[StageRecord], last_run_id: str | None) -> list[dict[str, Any]]:
    latest: dict[str, StageRecord] = {}
    extra: dict[str, tuple[str, str, str]] = {}  # service -> (version, ended_at, run_id)
    for r in records:
        if r.status != "ok":
            continue
        if r.service not in latest or r.ended_at > latest[r.service].ended_at:
            latest[r.service] = r
        for svc, ver in (r.detail.get("services") or {}).items():
            if svc not in extra or r.ended_at > extra[svc][1]:
                extra[svc] = (str(ver), r.ended_at, r.run_id)
    rows = [(s, r.version, r.ended_at, r.run_id) for s, r in latest.items()]
    rows += [(s, v, at, run) for s, (v, at, run) in extra.items() if s not in latest]
    return [
        {
            "name": s,
            "version": v,
            "role": ROLES.get(s, ""),
            "active": run == last_run_id,
            "last_used": at,
        }
        for s, v, at, run in sorted(rows)
    ]


def _count(con: duckdb.DuckDBPyConnection, glob: str) -> int:
    try:
        row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{glob}')").fetchone()
    except duckdb.IOException:
        return 0
    return int(row[0]) if row else 0


def _utc(value: datetime | None) -> datetime | None:
    """Timestamps are reported in UTC (CLAUDE.md). DuckDB returns TIMESTAMPTZ in the session's
    local zone and TIMESTAMP naive (silver stores UTC)."""
    if value is None:
        return None
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _data_facts(root: Path) -> dict[str, Any]:
    con = duckdb.connect()
    silver = (root / "silver" / "gharchive").as_posix()
    facts: dict[str, Any] = {"events_stored": _count(con, f"{silver}/events/*/*.parquet")}
    try:
        row = con.execute(
            f"""SELECT MIN(created_at_min), MAX(created_at_max), COUNT(DISTINCT source_file)
            FROM read_parquet('{silver}/hour_types/*/*.parquet')"""
        ).fetchone()
    except duckdb.IOException:
        row = None
    first, newest, hours = row or (None, None, 0)
    facts["first_event"], facts["newest_event"] = _utc(first), _utc(newest)
    facts["hours"] = hours
    q = (root / "quarantine").as_posix()
    facts["quarantined_rows"] = _count(con, f"{q}/gharchive/*/*.parquet") + _count(
        con, f"{q}/osv/*.parquet"
    )
    con.close()
    rollup = root / "rollup" / "rollup.duckdb"
    facts["rollup_rows"] = None
    if rollup.exists():
        c = duckdb.connect(str(rollup), read_only=True)
        try:
            total = 0
            for t in ("repo_hour", "repo_actor_day"):
                row = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
                total += int(row[0]) if row else 0
            facts["rollup_rows"] = total
        finally:
            c.close()
    return facts


def _lineage_summary(metadata_dir: Path) -> dict[str, Any]:
    lin = metadata_dir / "lineage.json"
    cat = metadata_dir / "catalog.json"
    if not (lin.exists() and cat.exists()):
        return {"nodes": None, "edges": None, "documented_pct": None}
    summary = json.loads(lin.read_text("utf-8"))["summary"]
    comp = json.loads(cat.read_text("utf-8"))["completeness"]
    return {
        "nodes": summary["nodes"],
        "edges": summary["edges"],
        "documented_pct": comp["documented_pct"],
    }


def _latest(records: list[StageRecord], stage: str) -> StageRecord | None:
    found = [r for r in records if r.stage == stage]
    return max(found, key=lambda r: r.ended_at) if found else None


def produced_by(records: list[StageRecord]) -> dict[str, Any] | None:
    """Which profile produced the newest output: the latest `profile` stage that ran a job
    (ADR-040). None when no job ran through the profile check (not measured)."""
    runs = [r for r in records if r.stage == "profile" and r.detail.get("produced_by")]
    if not runs:
        return None
    r = max(runs, key=lambda x: x.ended_at)
    return {
        "profile": r.detail["produced_by"],
        "fallback": bool(r.detail.get("fallback")),
        "reason": str(r.detail.get("reason", "")),
        "run_id": r.run_id,
        "decided_at": r.ended_at,
        "status": r.status,
        "probes": [{"name": p["name"], "ok": p["ok"]} for p in r.detail.get("probes", [])],
    }


def build(root: Path, metadata_dir: Path, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    records = read(root)
    runs = list(iter_runs(records))
    last_id, last = runs[-1] if runs else (None, [])
    facts = _data_facts(root)
    meta = Metadata()

    ok = [r for r in records if r.status == "ok"]
    event_stages = [r for r in ok if r.stage in EVENT_STAGES]
    history = []
    for run_id, all_recs in runs[-HISTORY_RUNS:]:
        recs = _last_per_stage(all_recs)
        start = min(_ts(r.started_at) for r in recs)
        end = max(_ts(r.ended_at) for r in recs)
        history.append(
            {
                "run_id": run_id,
                "started_at": start.isoformat(),
                "ended_at": end.isoformat(),
                "wall_seconds": round((end - start).total_seconds(), 3),
                "stages": len(recs),  # distinct stages (last attempt of each)
                "events_processed": sum(r.records_out or 0 for r in recs if r in event_stages),
                "status": "ok" if all(r.status == "ok" for r in recs) else "failed",
            }
        )
    daily: dict[date, int] = defaultdict(int)
    for r in event_stages:
        daily[_ts(r.ended_at).date()] += r.records_out or 0
    first_day = (now - timedelta(days=DAILY_DAYS - 1)).date()
    daily_rows = [
        {"date": d.isoformat(), "events_processed": n}
        for d, n in sorted(daily.items())
        if d >= first_day
    ]

    newest = facts["newest_event"]
    slo = int(meta.raw["freshness_sla_hours"]["silver"])
    lag = _hours(now - newest) if newest else None
    contracts = _latest(records, "quality-contracts")
    warehouse = _latest(records, "warehouse")
    registry = _latest(records, "resolve-fetch")
    sources = load_window()
    services = {r.service for r in last}
    disk = shutil.disk_usage(REPO_ROOT)
    from keystone.policy import load_policy

    fraction = load_policy().disk_budget_fraction
    budget = disk.free * fraction
    data_bytes = sum(p.stat().st_size for p in root.rglob("*") if p.is_file())
    produced = produced_by(records)
    return {
        "generated_at": now.isoformat(),
        # The recorded decision when there is one; else inferred from the services that ran.
        "profile": produced["profile"]
        if produced
        else ("full" if services & FULL_SERVICES else "free"),
        "produced_by": produced,
        "engines": engines(records, last_id),
        "totals": {
            "events_processed": sum(r.records_out or 0 for r in event_stages),
            "events_stored": facts["events_stored"],
            "bytes_downloaded": _sum_or_none([r.bytes_in for r in ok if r.stage == "ingest"]),
            # ingest's bytes_in is the download; what it parsed is in detail.bytes_parsed
            "bytes_parsed": _sum_or_none(
                [r.bytes_in for r in ok if r.stage == "parse"]
                + [r.detail.get("bytes_parsed") for r in ok if r.stage == "ingest"]
            ),
            "files_parsed": sum(int(r.detail.get("files", 0)) for r in event_stages),
            "rollup_rows": facts["rollup_rows"],
            "date_range": {
                "first_event": facts["first_event"].isoformat() if facts["first_event"] else None,
                "newest_event": newest.isoformat() if newest else None,
                "source_hours": facts["hours"],
            },
            "backfill_window": sources,
        },
        "last_run": {
            "run_id": last_id,
            "stages": [_stage(r) for r in _last_per_stage(last)],
        },
        "history": {"runs": history, "daily_volume": daily_rows},
        "freshness": {
            "newest_data_hour": newest.replace(minute=0, second=0, microsecond=0).isoformat()
            if newest
            else None,
            "lag_hours": lag,
            "slo_hours": slo,
            "status": None if lag is None else ("ok" if lag <= slo else "breach"),
        },
        "quality": {
            "checks_passed": _sum_or_none(
                [
                    int(contracts.detail["checks_passed"]) if contracts else None,
                    int(warehouse.detail["tests_passed"]) if warehouse else None,
                ]
            ),
            "dbt_tests_failed": int(warehouse.detail.get("tests_failed", 0)) if warehouse else None,
            "quarantined_rows": facts["quarantined_rows"],
            "contract_violations": int(contracts.detail.get("violations", 0))
            if contracts
            else None,
        },
        "cost": {
            "usd": 0.0,
            "basis": "no billing account exists for any service used (CLAUDE.md $0 rule)",
            "free_quota": [
                {
                    "name": f"disk budget ({fraction:.0%} of free disk, config/policy.yml)",
                    "used_pct": round(100 * data_bytes / budget, 2) if budget else None,
                },
                # Not measurable from a local run; null until the production workflow reads
                # the repository's Actions usage (Phase 10).
                {"name": "GitHub Actions minutes (private repository)", "used_pct": None},
                {
                    "name": "ecosyste.ms anonymous API, hour of the last fetch (policy.yml)",
                    "used_pct": _quota_pct(registry),
                },
            ],
        },
        "lineage_summary": _lineage_summary(metadata_dir),
    }


def _quota_pct(rec: StageRecord | None) -> float | None:
    """Share of the hourly quota used when the last registry fetch ended; null if unknown."""
    if rec is None:
        return None
    left, size = rec.detail.get("quota_left"), rec.detail.get("quota_limit")
    if not isinstance(left, int) or not isinstance(size, int) or size <= 0:
        return None
    return round(100 * (size - left) / size, 2)


def load_window() -> dict[str, Any]:
    """The configured backfill window (ADR-009): from the first archive hour to the present."""
    from keystone.sources import load_sources

    first = load_sources().gharchive.first_hour
    return {"start": first.isoformat(), "end": "present", "decision": "ADR-009"}


def validate(doc: dict[str, Any]) -> None:
    schema = json.loads(SCHEMA_PATH.read_text("utf-8"))
    jsonschema.validate(doc, schema, format_checker=jsonschema.FormatChecker())


# Pipeline-level gauges for Grafana; the name maps to a path in the JSON (check-grafana).
PIPELINE_GAUGES = {
    "keystone_pipeline_events_processed": ("totals", "events_processed"),
    "keystone_pipeline_rollup_rows": ("totals", "rollup_rows"),
    "keystone_pipeline_freshness_lag_hours": ("freshness", "lag_hours"),
    "keystone_pipeline_quality_checks_passed": ("quality", "checks_passed"),
    "keystone_pipeline_quarantined_rows": ("quality", "quarantined_rows"),
    "keystone_pipeline_lineage_nodes": ("lineage_summary", "nodes"),
    "keystone_pipeline_documented_pct": ("lineage_summary", "documented_pct"),
    "keystone_pipeline_cost_usd": ("cost", "usd"),
}


def export_gauges(doc: dict[str, Any]) -> None:
    values = {}
    for name, (section, key) in PIPELINE_GAUGES.items():
        v = doc[section][key]
        if v is not None:
            values[name] = float(v)
    record_gauges(values)
