"""score_v1: fixed rules that turn raw signals into components, fragility and risk (ADR-044).

Every number here is a rule, not a fit: no learned weights. A component is either available
(sub-score in [0, 1], 1 = most fragile) or unavailable with a reason; fragility is the weighted
mean of the available components and is withheld when they carry too little of the weight.
"""

import math
from dataclasses import asdict, dataclass, field, replace
from typing import Any

SCORE_VERSION = "score_v1"

# Component weights (sum 1.0). Sensitivity checks perturb these (backtest.sensitivity).
WEIGHTS: dict[str, float] = {
    "concentration": 0.30,
    "contributor_churn": 0.15,
    "activity_trend": 0.15,
    "responsiveness": 0.10,
    "release_recency": 0.10,
    "members_added": 0.05,
    "vulnerabilities": 0.15,
}


@dataclass(frozen=True)
class Rules:
    """Thresholds of score_v1. Changing any of them is a new score_version."""

    weights: dict[str, float] = field(default_factory=lambda: dict(WEIGHTS))
    min_coverage: float = 0.5  # share of the weight that must be available
    min_maint_actions: int = 10  # concentration needs at least this many maintenance actions
    min_prior_events: int = 30  # trend needs this many human events in the prior 9 months
    min_opened: int = 3  # responsiveness needs this many issues + PRs opened
    min_first_half_actors: int = 3  # churn needs this many contributors in the first half
    vuln_saturation: float = 5.0  # severity-weighted advisories at which the sub-score is 1
    dependents_saturation_log10: float = 6.0  # criticality 1 at 10^6 dependent repositories


RULES = Rules()


@dataclass(frozen=True)
class Component:
    name: str
    available: bool
    sub: float | None  # [0, 1], 1 = most fragile
    weight: float
    raw: dict[str, Any]
    reason: str  # why it has this value, or why it is unavailable

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clamp(x: float) -> float:
    return max(0.0, min(1.0, x))


def _na(name: str, rules: Rules, raw: dict[str, Any], reason: str) -> Component:
    return Component(name, False, None, rules.weights[name], raw, reason)


def _ok(name: str, rules: Rules, raw: dict[str, Any], sub: float, reason: str) -> Component:
    return Component(name, True, round(_clamp(sub), 4), rules.weights[name], raw, reason)


def components(s: dict[str, Any], rules: Rules = RULES) -> list[Component]:
    """Map one repository's raw signals (signals.py) to score_v1 components."""
    out: list[Component] = []

    # Maintenance-actor concentration: share of maintenance actions by the top actor (top 3 shown).
    raw = {k: s.get(k) for k in ("maint_version", "maint_actions", "maint_actors", "top1", "top3")}
    if not s.get("actor_data"):
        out.append(_na("concentration", rules, raw, "no per-actor data for this window"))
    elif (s.get("maint_actions") or 0) < rules.min_maint_actions:
        out.append(
            _na(
                "concentration",
                rules,
                raw,
                f"fewer than {rules.min_maint_actions} maintenance actions",
            )
        )
    else:
        out.append(
            _ok(
                "concentration",
                rules,
                raw,
                float(s["top1"]),
                f"top actor holds {s['top1']:.0%} of {s['maint_actions']} maintenance actions "
                f"({s['maint_version']}); top 3 hold {s['top3']:.0%}",
            )
        )

    # New versus departed contributors: share of the actors with maintenance actions in months 1-6
    # that have none in months 7-12 (stars, forks and comments do not make a contributor).
    raw = {k: s.get(k) for k in ("first_half_actors", "departed", "new_actors")}
    if not s.get("actor_data"):
        out.append(_na("contributor_churn", rules, raw, "no per-actor data for this window"))
    elif (s.get("first_half_actors") or 0) < rules.min_first_half_actors:
        out.append(
            _na(
                "contributor_churn",
                rules,
                raw,
                f"fewer than {rules.min_first_half_actors} maintainers in months 1-6",
            )
        )
    else:
        share = s["departed"] / s["first_half_actors"]
        out.append(
            _ok(
                "contributor_churn",
                rules,
                raw,
                share,
                f"{s['departed']} of {s['first_half_actors']} maintainers of months 1-6 made no "
                f"maintenance action in months 7-12; {s['new_actors']} new",
            )
        )

    # Activity trend: monthly human events, last 3 months against the 9 before.
    raw = {k: s.get(k) for k in ("recent_events", "prior_events", "one_completeness_period")}
    if not s.get("one_completeness_period"):
        out.append(
            _na("activity_trend", rules, raw, "window crosses a completeness period (ADR-012)")
        )
    elif (s.get("prior_events") or 0) < rules.min_prior_events:
        out.append(
            _na(
                "activity_trend",
                rules,
                raw,
                f"fewer than {rules.min_prior_events} human events in months 1-9",
            )
        )
    else:
        ratio = (s["recent_events"] / 3) / (s["prior_events"] / 9)
        out.append(
            _ok(
                "activity_trend",
                rules,
                raw,
                1 - ratio,
                f"monthly human events in the last 3 months are {ratio:.0%} of months 1-9",
            )
        )

    # Responsiveness proxy: issue and PR closes against opens (rollups keep no per-issue timing).
    raw = {k: s.get(k) for k in ("opened", "closed")}
    opened = s.get("opened") or 0
    if opened < rules.min_opened:
        out.append(
            _na(
                "responsiveness", rules, raw, f"fewer than {rules.min_opened} issues and PRs opened"
            )
        )
    else:
        ratio = (s.get("closed") or 0) / opened
        out.append(
            _ok(
                "responsiveness",
                rules,
                raw,
                1 - ratio,
                f"{s.get('closed') or 0} issue and PR closes for {opened} opened (bots included)",
            )
        )

    # Release recency: days since the last GitHub release in the window.
    raw = {k: s.get(k) for k in ("releases", "days_since_release")}
    if not s.get("releases"):
        out.append(_na("release_recency", rules, raw, "no GitHub release in the window"))
    else:
        days = int(s["days_since_release"])
        out.append(
            _ok(
                "release_recency",
                rules,
                raw,
                days / 365,
                f"{s['releases']} releases; last one {days} days before the cut",
            )
        )

    # Collaborators added (MemberEvent) in the last 6 months: a change-of-control signal.
    raw = {k: s.get(k) for k in ("members_added_6m", "members_added_12m")}
    added = s.get("members_added_6m") or 0
    if "E0" in (s.get("eras") or []):
        out.append(_na("members_added", rules, raw, "MemberEvent has no member in E0"))
    else:
        out.append(
            _ok(
                "members_added",
                rules,
                raw,
                1.0 if added > 0 else 0.0,
                f"{added} collaborators added in the last 6 months",
            )
        )

    # OSV advisories published before the cut and not withdrawn by it, severity-weighted.
    raw = {k: s.get(k) for k in ("vulns", "vulns_high", "mapped_packages")}
    if not s.get("vuln_data"):
        out.append(_na("vulnerabilities", rules, raw, "no OSV data loaded"))
    elif not s.get("mapped_packages"):
        out.append(_na("vulnerabilities", rules, raw, "no package mapped to this repository"))
    else:
        high = s.get("vulns_high") or 0
        weighted = high + 0.5 * ((s.get("vulns") or 0) - high)
        out.append(
            _ok(
                "vulnerabilities",
                rules,
                raw,
                weighted / rules.vuln_saturation,
                f"{s.get('vulns') or 0} advisories before the cut ({high} high or critical; "
                "severity as recorded today)",
            )
        )
    return out


@dataclass(frozen=True)
class Score:
    repo_id: int
    cut: str
    score_version: str
    fragility: float | None  # 0-100
    coverage: float
    criticality: float | None  # 0-1, live only (not point-in-time; ADR-044)
    risk: float | None  # 0-100 = 100 x criticality x fragility
    components: list[Component]
    note: str

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["components"] = [c.as_dict() for c in self.components]
        return d


def criticality(dependent_repos: int | None, rules: Rules = RULES) -> float | None:
    if dependent_repos is None:
        return None
    return round(_clamp(math.log10(1 + dependent_repos) / rules.dependents_saturation_log10), 4)


def score(
    repo_id: int,
    cut: str,
    s: dict[str, Any],
    crit: float | None = None,
    rules: Rules = RULES,
) -> Score:
    comps = components(s, rules)
    avail = [c for c in comps if c.available]
    total = sum(rules.weights.values())
    coverage = round(sum(c.weight for c in avail) / total, 4)
    if coverage < rules.min_coverage:
        frag = None
        note = f"insufficient data: {coverage:.0%} of the weight available"
    else:
        w = sum(c.weight for c in avail)
        frag = round(100 * sum(c.weight * (c.sub or 0.0) for c in avail) / w, 2)
        note = "signals only; not a prediction"
    risk = None if frag is None or crit is None else round(crit * frag, 2)
    return Score(repo_id, cut, SCORE_VERSION, frag, coverage, crit, risk, comps, note)


def with_weights(rules: Rules, weights: dict[str, float]) -> Rules:
    return replace(rules, weights=weights)
