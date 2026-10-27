"""Point-in-time backtests for `config/incidents.yml`, sensitivity checks and the report.

For each incident the cut is the first instant of the incident month (UTC), so no data from the
incident month itself is read, whatever the exact disclosure day. Trajectory points are earlier
month starts. A point is scored only when the rollup covers enough of its 12-month window;
otherwise it is reported as pending data. A runtime leakage check rescores the incident
repositories with every row at or after the cut removed and requires identical results.

Wording: these are signals; a high fragility at a cut is not a prediction, and a miss is
reported as a miss.
"""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from keystone.paths import CONFIG_DIR
from keystone.score import model, signals
from keystone.score.signals import Inputs, Window

INCIDENTS = CONFIG_DIR / "incidents.yml"
MIN_HOUR_COVERAGE = 0.9  # share of the window's hours the rollup must hold to score a point
ACTIVE_EVENTS = 100  # reference set: repositories with at least this many human events
FLAG_PERCENTILE = 90.0  # a repository at or above this fragility percentile is "flagged"


@dataclass(frozen=True)
class Incident:
    id: str
    month: str  # YYYY-MM
    repos: list[dict[str, Any]]  # {name, id?, id_source?}
    sources: list[str]
    summary: str

    @property
    def cut(self) -> datetime:
        y, m = self.month.split("-")
        return datetime(int(y), int(m), 1, tzinfo=UTC)


def load_incidents(path: Path = INCIDENTS) -> list[Incident]:
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    out = []
    for i in cfg["incidents"]:
        if not i.get("sources"):
            raise ValueError(f"incident {i['id']} has no source link")
        out.append(
            Incident(
                i["id"], str(i["month"]), i.get("repos", []), i["sources"], i.get("summary", "")
            )
        )
    return out


def percentile(value: float, population: list[float]) -> float:
    """Share of the population at or below `value`, in percent."""
    if not population:
        return 0.0
    return round(100 * sum(1 for p in population if p <= value) / len(population), 2)


def _reference(
    sig: dict[int, dict[str, Any]],
    rules: model.Rules,
    targets: tuple[int, ...] | list[int] = (),
) -> dict[int, float]:
    """Fragility of every active repository in the window that has enough data, plus the
    targets themselves, so a target is always ranked within the population it is compared to."""
    out = {}
    for repo, s in sig.items():
        if s.get("human_events", 0) >= ACTIVE_EVENTS or repo in targets:
            f = model.score(repo, s["cut"], s, None, rules).fragility
            if f is not None:
                out[repo] = f
    return out


def variants(base: model.Rules) -> list[tuple[str, model.Rules]]:
    """Weight perturbations: each component halved, raised by half, and dropped."""
    out = [("baseline", base)]
    for name in base.weights:
        for label, k in (("x0.5", 0.5), ("x1.5", 1.5), ("drop", 0.0)):
            w = dict(base.weights)
            w[name] = w[name] * k
            out.append((f"{name} {label}", model.with_weights(base, w)))
    return out


def sensitivity(
    sig: dict[int, dict[str, Any]], targets: list[int], rules: model.Rules = model.RULES
) -> dict[str, Any]:
    """Target percentiles and flags under every weight variant (the signals are reused)."""
    rows: list[dict[str, Any]] = []
    for label, r in variants(rules):
        ref = _reference(sig, r, targets)
        pop = list(ref.values())
        for t in targets:
            f = model.score(t, "", sig[t], None, r).fragility if t in sig else None
            p = None if f is None else percentile(f, pop)
            rows.append({"variant": label, "repo_id": t, "fragility": f, "percentile": p})
    out: dict[str, Any] = {"variants": len(variants(rules)), "per_repo": {}}
    for t in targets:
        ps: list[float] = [
            x["percentile"] for x in rows if x["repo_id"] == t and x["percentile"] is not None
        ]
        out["per_repo"][t] = {
            "scored_variants": len(ps),
            "percentile_min": min(ps) if ps else None,
            "percentile_max": max(ps) if ps else None,
            "flagged_variants": len([p for p in ps if p >= FLAG_PERCENTILE]),
        }
    out["rows"] = rows
    return out


@dataclass
class Point:
    cut: str
    hour_coverage: float
    status: str  # scored | pending data | partial data
    repos: list[dict[str, Any]] = field(default_factory=list)
    reference_size: int = 0
    leakage_check: str | None = None


def leakage_check(inp: Inputs, w: Window, repo_ids: list[int]) -> bool:
    """Signals for `repo_ids` are identical when the inputs end at the cut."""
    full = signals.compute(inp, w, repo_ids)
    con = inp.con
    cut = w.cut.isoformat()
    con.execute("ALTER VIEW repo_hour RENAME TO repo_hour_all")
    con.execute(
        f"CREATE VIEW repo_hour AS SELECT * FROM repo_hour_all WHERE window_start < '{cut}'"
    )
    renamed = []
    trims = {
        "repo_actor_month": (
            inp.actor_data,
            f"SELECT * FROM repo_actor_month_all WHERE month + INTERVAL 1 MONTH <= '{cut}'",
        ),
        "vulns": (
            inp.vuln_data,
            f"SELECT * REPLACE (CASE WHEN withdrawn_at < '{cut}' "
            f"THEN withdrawn_at END AS withdrawn_at) FROM vulns_all "
            f"WHERE published_at < '{cut}'",
        ),
    }
    for v, (present, sql) in trims.items():
        if present:
            con.execute(f"ALTER VIEW {v} RENAME TO {v}_all")
            con.execute(f"CREATE VIEW {v} AS {sql}")
            renamed.append(v)
    try:
        cut_only = signals.compute(inp, w, repo_ids)
    finally:
        for v in ["repo_hour", *renamed]:
            con.execute(f"DROP VIEW {v}")
            con.execute(f"ALTER VIEW {v}_all RENAME TO {v}")
    return full == cut_only


def run_incident(
    inp: Inputs,
    inc: Incident,
    months_before: int = 0,
    rules: model.Rules = model.RULES,
    min_coverage: float = MIN_HOUR_COVERAGE,
) -> dict[str, Any]:
    points = []
    sens: dict[str, Any] | None = None
    for k in range(months_before, -1, -1):
        w = Window(signals.add_months(inc.cut, -k))
        cov = min(signals.hour_coverage(inp, w), signals.hour_coverage(inp, w, recent=True))
        if cov == 0:
            points.append(Point(w.cut.isoformat(), cov, "pending data"))
            continue
        if cov < min_coverage:
            points.append(Point(w.cut.isoformat(), cov, "partial data"))
            continue
        ids: dict[str, int | None] = {}
        for r in inc.repos:
            ids[r["name"]] = r.get("id") or signals.resolve_name(inp, r["name"], w)
        found = [i for i in ids.values() if i is not None]
        sig = signals.compute(inp, w, min_human_events=ACTIVE_EVENTS, keep=found)
        ref = _reference(sig, rules, found)
        pop = list(ref.values())
        pt = Point(w.cut.isoformat(), cov, "scored", reference_size=len(pop))
        for name, rid in ids.items():
            if rid is None:
                pt.repos.append({"name": name, "status": "unresolved: no id before the cut"})
                continue
            if rid not in sig:
                pt.repos.append({"name": name, "repo_id": rid, "status": "no activity in window"})
                continue
            sc = model.score(rid, w.cut.isoformat(), sig[rid], None, rules)
            p = None if sc.fragility is None else percentile(sc.fragility, pop)
            pt.repos.append(
                {
                    "name": name,
                    "repo_id": rid,
                    "status": "scored" if sc.fragility is not None else sc.note,
                    "fragility": sc.fragility,
                    "percentile": p,
                    "flagged": p is not None and p >= FLAG_PERCENTILE,
                    "score": sc.as_dict(),
                }
            )
        pt.leakage_check = "pass" if leakage_check(inp, w, found) else "FAIL"
        if k == 0:
            sens = sensitivity(sig, [i for i in found if i in sig], rules)
            sens.pop("rows")
        points.append(pt)
    primary = points[-1]
    if primary.status != "scored":
        outcome = primary.status
    elif not any(r.get("fragility") is not None for r in primary.repos):
        outcome = "not scored (see repositories)"
    else:
        outcome = "flagged" if any(r.get("flagged") for r in primary.repos) else "miss"
    return {
        "id": inc.id,
        "month": inc.month,
        "cut": inc.cut.isoformat(),
        "sources": inc.sources,
        "summary": inc.summary,
        "outcome": outcome,
        "points": [p.__dict__ for p in points],
        "sensitivity": sens,
    }


def run(
    inp: Inputs,
    incidents: list[Incident],
    months_before: int = 0,
    rules: model.Rules = model.RULES,
) -> dict[str, Any]:
    return {
        "score_version": model.SCORE_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "rules": {
            "weights": rules.weights,
            "min_coverage": rules.min_coverage,
            "min_hour_coverage": MIN_HOUR_COVERAGE,
            "active_events": ACTIVE_EVENTS,
            "flag_percentile": FLAG_PERCENTILE,
        },
        "inputs": {"actor_data": inp.actor_data, "vuln_data": inp.vuln_data},
        "incidents": [run_incident(inp, i, months_before, rules) for i in incidents],
    }


def report_md(res: dict[str, Any]) -> str:
    """Markdown report: every incident, its outcome (misses included) and the checks."""
    lines = [
        "# Backtest report",
        "",
        f"Generated by `keystone score backtest` ({res['score_version']}, "
        f"{res['generated_at'][:10]}). Rules: ADR-044. Fragility is a signal, not a prediction;",
        "a high score before an incident does not mean the incident was foreseeable, and a miss",
        "is reported as a miss. Criticality is left out of every backtest: the dependents counts",
        "are a current snapshot, not point-in-time (ADR-044), so risk is not backtested.",
        "",
        f"A repository is *flagged* at a cut when its fragility is at or above the "
        f"{res['rules']['flag_percentile']:.0f}th percentile of active repositories "
        f"(>= {res['rules']['active_events']} human events in the window) at the same cut. "
        f"A point is scored only when the rollup holds >= "
        f"{res['rules']['min_hour_coverage']:.0%} of the window's hours and of its last 3 months.",
        "",
        "Read with care: the cut is the first day of the disclosure month, which can fall after",
        "the compromise began (the event-stream handover, the xz 5.6.0 release), so a flag can",
        "reflect the incident's own activity rather than an earlier signal. The package mapping,",
        "OSV severity labels and alias sets are today's snapshot (ADR-044); OSV aliases of one",
        "flaw can count twice, and malicious-package records are left out. Repositories without",
        "a mapped package or per-actor data are ranked on fewer components.",
        "",
        f"Inputs: per-actor data {'yes' if res['inputs']['actor_data'] else 'no'}, "
        f"OSV {'yes' if res['inputs']['vuln_data'] else 'no'}.",
        "",
        "| Incident | Cut (UTC) | Outcome | Hours covered | Leakage check |",
        "| --- | --- | --- | --- | --- |",
    ]
    for inc in res["incidents"]:
        p = inc["points"][-1]
        lines.append(
            f"| {inc['id']} | {inc['cut'][:10]} | {inc['outcome']} | "
            f"{p['hour_coverage']:.1%} | {p['leakage_check'] or 'not run (no data)'} |"
        )
    for inc in res["incidents"]:
        lines += ["", f"## {inc['id']} ({inc['month']})", "", inc["summary"], ""]
        lines += [f"- Source: {s}" for s in inc["sources"]]
        lines += ["", "| Cut | Status | Repository | Fragility | Percentile | Flagged |"]
        lines += ["| --- | --- | --- | --- | --- | --- |"]
        for p in inc["points"]:
            if not p["repos"]:
                lines.append(f"| {p['cut'][:10]} | {p['status']} | | | | |")
            for r in p["repos"]:
                lines.append(
                    f"| {p['cut'][:10]} | {r['status']} | {r['name']} | "
                    f"{r.get('fragility', '')} | {r.get('percentile', '')} | "
                    f"{r.get('flagged', '')} |"
                )
        last = inc["points"][-1]
        for r in last["repos"]:
            if "score" in r:
                lines += ["", f"Components for {r['name']} at {last['cut'][:10]}:", ""]
                for c in r["score"]["components"]:
                    sub = "n/a" if c["sub"] is None else f"{c['sub']:.2f}"
                    lines.append(f"- {c['name']} (weight {c['weight']}): {sub}, {c['reason']}")
        if inc["sensitivity"]:
            lines += ["", f"Sensitivity ({inc['sensitivity']['variants']} weight variants):", ""]
            for rid, s in inc["sensitivity"]["per_repo"].items():
                lines.append(
                    f"- repo {rid}: percentile {s['percentile_min']} to {s['percentile_max']}, "
                    f"flagged in {s['flagged_variants']} of {s['scored_variants']}"
                )
    lines.append("")
    return "\n".join(lines)


def write(res: dict[str, Any], json_path: Path, md_path: Path | None) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(res, indent=2, default=str) + "\n"
    json_path.write_text(text, encoding="utf-8", newline="\n")
    if md_path is not None:
        md_path.write_text(report_md(res), encoding="utf-8", newline="\n")
