"""Match-rate report and review CSVs (the Phase 7 gate), read from the warehouse marts.

The report measures, never estimates: every figure is a count over `map_package_repo`,
`dim_repo_rename`, `map_repo_name_id` and their inputs. Suggested rows go to review CSVs; a
reviewer fills `decision` (accepted or rejected) and `decided_on`, and copies the first six
columns (the seed's columns) to `dbt/seeds/resolve_decisions.csv`, which overrides the rules
on the next build. Queries run in UTC, as the dbt profile does.
"""

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

DECIDED = ("auto_exact", "accepted")


@dataclass(frozen=True)
class ReportPaths:
    root: Path

    @property
    def dir(self) -> Path:
        return self.root / "resolve"

    @property
    def json(self) -> Path:
        return self.dir / "match_rate.json"

    @property
    def markdown(self) -> Path:
        return self.dir / "match_rate.md"

    @property
    def review_packages(self) -> Path:
        return self.dir / "review_package_repo.csv"

    @property
    def review_names(self) -> Path:
        return self.dir / "review_repo_name_id.csv"


def _connect(warehouse_db: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(warehouse_db), read_only=True)
    con.execute("SET TimeZone = 'UTC'")  # hour cuts and era bounds as the marts make them
    return con


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    rel = con.execute(sql)
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, r, strict=True)) for r in rel.fetchall()]


PACKAGE_SQL = """
WITH best AS (
    SELECT
        package_key,
        MAX(CASE WHEN status = 'auto_exact' THEN 1 ELSE 0 END) AS auto_exact,
        MAX(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END) AS accepted,
        MAX(CASE WHEN status = 'suggested' THEN 1 ELSE 0 END) AS suggested,
        MAX(CASE WHEN status IN ('auto_exact', 'accepted') AND repo_id IS NOT NULL
            THEN 1 ELSE 0 END) AS with_id,
        MAX(CASE WHEN status IN ('auto_exact', 'accepted') AND seen_in_gharchive
            THEN 1 ELSE 0 END) AS seen,
        COUNT(*) AS candidates
    FROM marts.map_package_repo
    GROUP BY package_key
)
SELECT
    u.ecosystem,
    u.package_scope AS scope,
    COUNT(*) AS packages,
    COUNT(b.package_key) AS with_candidate,
    SUM(COALESCE(b.auto_exact, 0)) AS auto_exact,
    SUM(CASE WHEN b.auto_exact = 0 AND b.accepted = 1 THEN 1 ELSE 0 END) AS accepted,
    SUM(CASE WHEN b.auto_exact = 0 AND b.accepted = 0 AND b.suggested = 1 THEN 1 ELSE 0 END)
        AS suggested_only,
    SUM(CASE WHEN b.package_key IS NOT NULL AND b.auto_exact = 0 AND b.accepted = 0
        AND b.suggested = 0 THEN 1 ELSE 0 END) AS rejected_only,
    SUM(CASE WHEN b.package_key IS NULL THEN 1 ELSE 0 END) AS no_candidate,
    SUM(COALESCE(b.with_id, 0)) AS decided_with_github_id,
    SUM(COALESCE(b.seen, 0)) AS decided_seen_in_gharchive
FROM intermediate.int_package_universe AS u
LEFT JOIN best AS b ON u.package_key = b.package_key
GROUP BY u.ecosystem, u.package_scope
ORDER BY u.package_scope, u.ecosystem
"""

CROSSCHECK_SQL = """
WITH e AS (
    SELECT package_key, repo_host, repo_full_name, found, url_rejected
    FROM staging.stg_registry__packages WHERE source = 'ecosystems'
),
n AS (
    SELECT package_key, repo_host, repo_full_name, found, url_rejected
    FROM staging.stg_registry__packages WHERE source = 'npm_registry'
),
ids AS (
    SELECT repo_name_lower, repo_id FROM intermediate.int_repo_identity
)
SELECT
    CASE
        WHEN NOT n.found THEN 'npm_not_found'
        WHEN e.package_key IS NULL OR NOT e.found THEN 'ecosystems_not_found'
        WHEN n.repo_full_name IS NULL AND e.repo_full_name IS NULL THEN 'neither_names_a_repo'
        WHEN n.repo_full_name IS NULL THEN 'npm_names_no_repo'
        WHEN e.repo_full_name IS NULL THEN 'ecosystems_names_no_repo'
        WHEN n.repo_host = e.repo_host AND n.repo_full_name = e.repo_full_name THEN 'same_name'
        WHEN ni.repo_id IS NOT NULL AND ni.repo_id = ei.repo_id THEN 'same_id_renamed'
        ELSE 'differ'
    END AS outcome,
    COUNT(*) AS packages
FROM n
LEFT JOIN e ON n.package_key = e.package_key
LEFT JOIN ids AS ni ON n.repo_host = 'github.com' AND n.repo_full_name = ni.repo_name_lower
LEFT JOIN ids AS ei ON e.repo_host = 'github.com' AND e.repo_full_name = ei.repo_name_lower
GROUP BY 1
ORDER BY 1
"""


def build(warehouse_db: Path) -> dict[str, Any]:
    con = _connect(warehouse_db)
    try:
        packages = _rows(con, PACKAGE_SQL)
        in_scope = [r for r in packages if r["scope"] in ("critical", "osv")]
        total = sum(r["packages"] for r in in_scope)
        decided = sum(r["auto_exact"] + r["accepted"] for r in in_scope)
        report: dict[str, Any] = {
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "packages": {
                "by_scope": packages,
                "in_scope": total,
                "decided": decided,
                "auto_exact": sum(r["auto_exact"] for r in in_scope),
                "match_rate": round(decided / total, 4) if total else None,
                "with_github_id": sum(r["decided_with_github_id"] for r in in_scope),
                "seen_in_gharchive": sum(r["decided_seen_in_gharchive"] for r in in_scope),
            },
            "rules": _rows(
                con,
                "SELECT status, rule, COUNT(*) AS candidates FROM marts.map_package_repo "
                "GROUP BY ALL ORDER BY status, rule",
            ),
            "sources": _rows(
                con,
                "SELECT source, found, url_rejected, COUNT(*) AS packages "
                "FROM staging.stg_registry__packages GROUP BY ALL ORDER BY ALL",
            ),
            "npm_crosscheck": _rows(con, CROSSCHECK_SQL),
            "repo_identity": _rows(
                con,
                "SELECT id_rule, COUNT(*) AS names, "
                "SUM(CASE WHEN gharchive_disagrees THEN 1 ELSE 0 END) AS gharchive_disagrees "
                "FROM intermediate.int_repo_identity GROUP BY ALL ORDER BY id_rule",
            ),
            "renames": _rows(
                con,
                "SELECT status, rule, COUNT(*) AS renames, "
                "SUM(CASE WHEN by_ecosystems THEN 1 ELSE 0 END) AS confirmed_by_ecosystems, "
                "COUNT(DISTINCT repo_id) AS repos FROM marts.dim_repo_rename "
                "GROUP BY ALL ORDER BY status",
            ),
            "names_without_id": _rows(
                con,
                "SELECT status, rule, COUNT(*) AS name_hours, SUM(events) AS events "
                "FROM marts.map_repo_name_id GROUP BY ALL ORDER BY status, rule",
            ),
        }
        unresolved = con.execute(
            "SELECT COUNT(*), COALESCE(SUM(n), 0) FROM ("
            "  SELECT COUNT(*) AS n FROM staging.stg_gharchive__repo_names"
            "  WHERE repo_id IS NULL"
            "    AND NOT (era = 'E1' AND created_at >= TIMESTAMP '2015-01-01 00:00:00')"
            "  GROUP BY LOWER(repo_name), DATE_TRUNC('hour', created_at)) AS t"
        ).fetchone()
        assert unresolved is not None
        hours, events = int(unresolved[0]), int(unresolved[1])
        named = report["names_without_id"]
        report["names_without_id_totals"] = {
            "name_hours": hours,
            "events": events,
            "no_candidate_name_hours": hours - sum(r["name_hours"] for r in named),
            "no_candidate_events": events - sum(int(r["events"]) for r in named),
        }
        ties = con.execute(
            "SELECT COUNT(DISTINCT package_key) FROM marts.map_package_repo WHERE rule = 'tie'"
        ).fetchone()
        report["llm_rung"] = {"waiting_ties": int(ties[0]) if ties else 0}
        return report
    finally:
        con.close()


def write_review(warehouse_db: Path, paths: ReportPaths) -> tuple[int, int]:
    """Suggested rows to the review CSVs (sorted so the most consequential come first)."""
    con = _connect(warehouse_db)
    try:
        packages = _rows(
            con,
            "SELECT 'package_repo' AS subject, package_key AS subject_key, repo_key AS candidate, "
            "'' AS decision, '' AS decided_on, '' AS note, ecosystem, package_name, package_scope, "
            "repo_full_name, repo_id, rule, candidates, independent_sources, by_ecosystems, "
            "by_npm_registry, by_osv_git_range, by_osv_fix_sole, by_osv_fix_shared, in_subpath "
            "FROM marts.map_package_repo WHERE status = 'suggested' "
            "ORDER BY package_scope, package_key, repo_key",
        )
        names = _rows(
            con,
            "SELECT 'repo_name_id' AS subject, repo_name_lower AS subject_key, "
            "CAST(repo_id AS VARCHAR) AS candidate, '' AS decision, '' AS decided_on, "
            "'' AS note, rule, MIN(candidate_ids) AS candidate_ids, COUNT(*) AS name_hours, "
            "SUM(events) AS events, MIN(event_hour) AS first_hour, MAX(event_hour) AS last_hour "
            "FROM marts.map_repo_name_id WHERE status = 'suggested' "
            "GROUP BY ALL ORDER BY events DESC, subject_key",
        )
    finally:
        con.close()
    paths.dir.mkdir(parents=True, exist_ok=True)
    for dest, rows in ((paths.review_packages, packages), (paths.review_names, names)):
        with dest.open("w", newline="", encoding="utf-8") as fh:
            if rows:
                writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
            else:
                fh.write("subject,subject_key,candidate,decision,decided_on,note\n")
    return len(packages), len(names)


def _pct(n: int, d: int) -> str:
    return f"{n / d:.1%}" if d else "n/a"


def markdown(report: dict[str, Any], review: tuple[int, int]) -> str:
    p = report["packages"]
    lines = [
        "# Phase 7 match-rate report",
        "",
        f"Generated {report['generated_at']} from the warehouse marts (measured counts).",
        "",
        "## Packages to repositories",
        "",
        f"In scope (critical or with a non-malware OSV advisory): **{p['in_scope']:,}** packages; "
        f"decided (auto_exact or accepted): **{p['decided']:,}** "
        f"(**{_pct(p['decided'], p['in_scope'])}**), of which auto_exact {p['auto_exact']:,}. "
        f"Decided with a GitHub id: {p['with_github_id']:,}; that id seen in the loaded GH "
        f"Archive data: {p['seen_in_gharchive']:,}.",
        "",
        "| Scope | Ecosystem | Packages | Candidate | auto_exact | accepted | suggested only "
        "| rejected only | none | match rate |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in report["packages"]["by_scope"]:
        decided = r["auto_exact"] + r["accepted"]
        lines.append(
            f"| {r['scope']} | {r['ecosystem']} | {r['packages']:,} | {r['with_candidate']:,} "
            f"| {r['auto_exact']:,} | {r['accepted']:,} | {r['suggested_only']:,} "
            f"| {r['rejected_only']:,} | {r['no_candidate']:,} | {_pct(decided, r['packages'])} |"
        )
    lines += [
        "",
        "Candidates by rule:",
        "",
        "| Status | Rule | Candidates |",
        "| --- | --- | ---: |",
    ]
    lines += [f"| {r['status']} | {r['rule']} | {r['candidates']:,} |" for r in report["rules"]]
    lines += ["", "npm cross-check (npm registry `repository` vs ecosyste.ms):", ""]
    lines += ["| Outcome | Packages |", "| --- | ---: |"]
    lines += [f"| {r['outcome']} | {r['packages']:,} |" for r in report["npm_crosscheck"]]
    lines += ["", "Repository ids of claimed names:", "", "| Rule | Names | GH Archive disagrees |"]
    lines += ["| --- | ---: | ---: |"]
    lines += [
        f"| {r['id_rule']} | {r['names']:,} | {r['gharchive_disagrees']:,} |"
        for r in report["repo_identity"]
    ]
    lines += ["", "## Renames", "", "| Status | Rule | Renames | Repos | Also in ecosyste.ms |"]
    lines += ["| --- | --- | ---: | ---: | ---: |"]
    lines += [
        f"| {r['status']} | {r['rule']} | {r['renames']:,} | {r['repos']:,} "
        f"| {r['confirmed_by_ecosystems']:,} |"
        for r in report["renames"]
    ]
    t = report["names_without_id_totals"]
    lines += [
        "",
        "## GH Archive events without a repository id",
        "",
        f"{t['events']:,} events in {t['name_hours']:,} name-hours; no candidate id in the "
        f"loaded data: {t['no_candidate_events']:,} events ({t['no_candidate_name_hours']:,} "
        "name-hours).",
        "",
        "| Status | Rule | Name-hours | Events |",
        "| --- | --- | ---: | ---: |",
    ]
    lines += [
        f"| {r['status']} | {r['rule']} | {r['name_hours']:,} | {int(r['events']):,} |"
        for r in report["names_without_id"]
    ]
    lines += [
        "",
        "## Review and the language-model rung",
        "",
        f"Review CSVs: {review[0]:,} package candidates, {review[1]:,} names (suggested rows). "
        f"Ties waiting for the language-model rung: {report['llm_rung']['waiting_ties']:,} "
        "packages.",
        "",
    ]
    return "\n".join(lines)


def run(warehouse_db: Path, paths: ReportPaths) -> dict[str, Any]:
    report = build(warehouse_db)
    review = write_review(warehouse_db, paths)
    report["review_rows"] = {"package_repo": review[0], "repo_name_id": review[1]}
    paths.dir.mkdir(parents=True, exist_ok=True)
    paths.json.write_text(json.dumps(report, indent=2, default=str) + "\n", "utf-8")
    paths.markdown.write_text(markdown(report, review), "utf-8")
    return report
