"""Phase 7 entity resolution: URL rules, registry fetch (mocked HTTP), the language-model rung."""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx
import pytest

from keystone.fetch.client import PoliteClient
from keystone.parsers.osv import repo_of
from keystone.policy import HostLimits, Policy
from keystone.resolve import chooser
from keystone.resolve.urls import Rejected, RepoUrl, normalize, package_key, pypi_name
from keystone.sources import registry


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://github.com/psf/requests", RepoUrl("github.com", "psf/requests")),
        ("git+https://github.com/Lodash/Lodash.git", RepoUrl("github.com", "lodash/lodash")),
        ("git://github.com/a/b.git", RepoUrl("github.com", "a/b")),
        ("git+ssh://git@github.com/a/b.git", RepoUrl("github.com", "a/b")),
        ("ssh://git@github.com:22/a/b", RepoUrl("github.com", "a/b")),
        ("git@github.com:a/b.git", RepoUrl("github.com", "a/b")),
        ("github:a/b", RepoUrl("github.com", "a/b")),
        ("a/b", RepoUrl("github.com", "a/b")),  # npm shorthand
        ("gitlab:g/p", RepoUrl("gitlab.com", "g/p")),
        ("https://www.github.com/a/b#readme", RepoUrl("github.com", "a/b")),
        ("https://github.com/a/b/tree/main/packages/c", RepoUrl("github.com", "a/b", "packages/c")),
        ("https://gitlab.com/g/p/-/tree/main/sub", RepoUrl("gitlab.com", "g/p", "sub")),
        ("github.com/a/b", RepoUrl("github.com", "a/b")),
        ("https://github.com/sponsors/someone", Rejected("not_a_repository")),
        ("https://github.com/a/b.wiki", Rejected("not_a_repository")),
        ("https://github.com/a", Rejected("no_repository_path")),
        ("https://example.org/a/b", Rejected("other_host")),
        ("https://readthedocs.io", Rejected("other_host")),
        ("not a url", Rejected("not_a_url")),
        ("", Rejected("empty")),
        (None, Rejected("empty")),
    ],
)
def test_normalize_repo_urls(raw: str | None, expected: RepoUrl | Rejected) -> None:
    assert normalize(raw) == expected


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/pallets/flask/commit/abc",
        "https://github.com/a/b.git",
        "git://github.com/a/b",
        "https://gitlab.com/g/p/-/merge_requests/1",
        "https://bitbucket.org/o/r/src",
    ],
)
def test_normalize_agrees_with_the_osv_parser(url: str) -> None:
    got = normalize(url)
    assert isinstance(got, RepoUrl) and (got.host, got.full_name) == repo_of(url)


@pytest.mark.parametrize("name", ["Django", "zope.interface", "Foo__Bar-.baz", "a-b"])
def test_pep503_python_and_sql_agree(name: str) -> None:
    sql = duckdb.sql(f"SELECT LOWER(REGEXP_REPLACE('{name}', '[-_.]+', '-', 'g'))").fetchone()
    assert sql is not None and sql[0] == pypi_name(name)
    assert package_key("PyPI", name) == f"PyPI/{pypi_name(name)}"
    assert package_key("npm", name) == f"npm/{name}"
    assert package_key("NuGet", "Azure.Identity") == "NuGet/azure.identity"


def test_purls() -> None:
    assert registry.purl_for("PyPI", "requests") == "pkg:pypi/requests"
    assert registry.purl_for("npm", "@babel/core") == "pkg:npm/%40babel/core"
    assert registry.purl_for("Maven", "org.x:y") == "pkg:maven/org.x/y"
    assert registry.purl_for("Unknown", "x") is None
    assert registry.npm_path("@babel/core") == "@babel%2Fcore"


ECO_ITEM = {
    "name": "Requests",
    "purl": "pkg:pypi/requests",
    "critical": True,
    "repository_url": "https://github.com/psf/requests",
    "downloads": 10,
    "downloads_period": "last-month",
    "dependent_packages_count": 3,
    "latest_release_published_at": "2026-05-14T19:25:26.000Z",
    "maintainers": [{"login": "someone", "email": "x@example.org"}],
    "repo_metadata": {
        "uuid": "1362490",
        "full_name": "psf/requests",
        "fork": False,
        "archived": False,
        "previous_names": ["requests/requests"],
        "host": {"name": "GitHub"},
        "owner_record": {"email": "x@example.org"},
    },
}


class Registries:
    """ecosyste.ms and the npm registry, in memory."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.quota = 4000

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append(f"{request.method} {request.url.host}{path}")
        headers = {"x-ratelimit-remaining": str(self.quota)}
        if path == "/robots.txt":
            return httpx.Response(404)
        if path.endswith("/package_names"):
            names = ["requests"] if "pypi" in path else ["left-pad"]
            return httpx.Response(200, json=names, headers=headers)
        if path.endswith("/bulk_lookup"):
            purls = json.loads(request.content)["purls"]
            assert len(purls) <= registry.BULK_MAX
            items = [ECO_ITEM] if "pkg:pypi/requests" in purls else []
            return httpx.Response(200, json=items, headers=headers)
        if request.url.host == "registry.npmjs.org":
            if request.headers.get("if-none-match") == '"v1"':
                return httpx.Response(304, headers={"etag": '"v1"'})
            doc = {
                "name": "left-pad",
                "repository": {"url": "git+https://github.com/x/left-pad.git"},
            }
            return httpx.Response(200, json=doc, headers={"etag": '"v1"'})
        return httpx.Response(404)


def _policy() -> Policy:
    fast = HostLimits(max_concurrent=1, requests_per_second=1000)
    return Policy(
        user_agent="keystone-test/0",
        default_limits=fast,
        hosts={
            "packages.ecosyste.ms": HostLimits(
                max_concurrent=1,
                requests_per_second=1000,
                quota_header="x-ratelimit-remaining",
                quota_floor=500,
            )
        },
        disk_budget_fraction=0.7,
    )


def _fetch(tmp_path: Path, host: Registries, **kw: object) -> registry.FetchStats:
    paths = registry.RegistryPaths(tmp_path)
    return registry.run_fetch(
        lambda: PoliteClient(_policy(), httpx.MockTransport(host)),
        paths,
        tmp_path / "warehouse" / "none.duckdb",
        **kw,
    )


def _silver(tmp_path: Path, table: str) -> list[dict[str, object]]:
    folder = (tmp_path / "silver/registry" / table).as_posix()
    rel = duckdb.sql(f"SELECT * FROM read_parquet('{folder}/*.parquet')")
    cols = rel.columns
    return [dict(zip(cols, r, strict=True)) for r in rel.fetchall()]


def test_fetch_projects_claims_and_never_people(tmp_path: Path) -> None:
    host = Registries()
    stats = _fetch(tmp_path, host)
    assert stats.requested == {"ecosystems": 2, "npm_registry": 1}
    rows = {(r["source"], r["package_key"]): r for r in _silver(tmp_path, "packages")}
    req = rows[("ecosystems", "PyPI/requests")]
    assert (req["repo_full_name"], req["repo_github_id"], req["critical"]) == (
        "psf/requests",
        1362490,
        True,
    )
    assert rows[("ecosystems", "npm/left-pad")]["found"] is False  # asked, absent: not a guess
    assert rows[("npm_registry", "npm/left-pad")]["repo_full_name"] == "x/left-pad"
    names = {(r["repo_name_lower"], r["kind"]) for r in _silver(tmp_path, "repo_names")}
    assert names == {("psf/requests", "current"), ("requests/requests", "previous")}
    blob = json.dumps([*rows.values()], default=str)
    assert "example.org" not in blob and "someone" not in blob  # no maintainer data in silver

    # Fresh claims are not fetched again; a stale npm claim is revalidated with its ETag.
    before = len(host.calls)
    again = _fetch(tmp_path, host)
    assert again.requested == {"ecosystems": 1, "npm_registry": 0}  # absences are asked again
    assert not any("registry.npmjs" in c for c in host.calls[before:])
    third = _fetch(tmp_path, host, refresh_days=-1)
    assert third.not_modified == 1


def test_fetch_stops_at_the_quota_floor(tmp_path: Path) -> None:
    host = Registries()
    host.quota = 500
    stats = _fetch(tmp_path, host)
    assert stats.stopped and "floor 500" in stats.stopped
    assert not any("bulk_lookup" in c for c in host.calls)


class Fake:
    model = "fake"

    def __init__(self, answers: dict[str, chooser.Answer]) -> None:
        self.answers = answers

    def ask(self, question: chooser.Question) -> chooser.Answer:
        return self.answers[question.package_key]


def test_llm_rung_refuses_candidates_it_was_not_shown(tmp_path: Path) -> None:
    qs = chooser.questions(
        [
            ("PyPI/a", "github.com#1", True, False, False),
            ("PyPI/a", "github.com#2", False, False, True),
            ("PyPI/b", "github.com#3", True, False, False),
            ("PyPI/b", "gitlab.com/o/b", False, False, True),
            ("PyPI/c", "github.com#5", True, False, False),
            ("PyPI/c", "github.com#6", False, False, True),
            ("PyPI/d", "github.com#7", True, False, False),
            ("PyPI/d", "github.com#8", False, False, True),
        ]
    )
    assert qs[0].candidates == ("github.com#1", "github.com#2")
    fake = Fake(
        {
            "PyPI/a": chooser.Answer("github.com#2", 0.9),
            "PyPI/b": chooser.Answer("github.com#999", 0.99),  # invented: refused
            "PyPI/c": chooser.Answer(None, 0.2),
            "PyPI/d": chooser.Answer("github.com#7", 1.5),  # impossible confidence: refused
        }
    )
    rows = chooser.decide(fake, qs, now=lambda: datetime(2026, 10, 21, tzinfo=UTC))
    got = {r.package_key: (r.outcome, r.candidate) for r in rows}
    assert got == {
        "PyPI/a": ("chosen", "github.com#2"),
        "PyPI/b": ("refused", None),
        "PyPI/c": ("declined", None),
        "PyPI/d": ("refused", None),
    }
    dest = tmp_path / "c.parquet"
    assert chooser.write_choices(rows, dest) == 4


def test_llm_rung_is_skipped_without_a_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    with pytest.raises(chooser.NotConfigured):
        chooser.openrouter_chooser()


def test_ensure_tables_are_empty_and_typed(tmp_path: Path) -> None:
    registry.ensure_tables(registry.RegistryPaths(tmp_path))
    chooser.ensure_choices(tmp_path)
    assert _silver(tmp_path, "packages") == []
    folder = (tmp_path / "silver/registry/packages").as_posix()
    assert (
        "repo_github_id" in duckdb.sql(f"SELECT * FROM read_parquet('{folder}/*.parquet')").columns
    )
    assert asyncio.iscoroutinefunction(registry.fetch)


def test_replay_fixtures_go_through_the_same_projection(tmp_path: Path) -> None:
    fixtures = Path(__file__).parent / "fixtures" / "registry"
    stats = registry.run_fetch(
        lambda: PoliteClient(_policy(), httpx.MockTransport(registry.replay_handler(fixtures))),
        registry.RegistryPaths(tmp_path),
        tmp_path / "warehouse" / "none.duckdb",
    )
    assert stats.found == {"ecosystems": 2, "npm_registry": 1}
    rows = {(r["source"], r["package_key"]): r for r in _silver(tmp_path, "packages")}
    assert rows[("npm_registry", "npm/fixture-npm")]["repo_full_name"] == "owner-4/repo-4"
    assert rows[("ecosystems", "npm/fixture-npm")]["repo_github_id"] == 1000008
    quarantined = duckdb.sql(
        f"SELECT reason FROM read_parquet('{(tmp_path / 'quarantine/registry').as_posix()}/*')"
    ).fetchall()
    assert ("no_purl_or_name",) in quarantined


def test_an_unattributable_record_is_not_turned_into_absences(tmp_path: Path) -> None:
    fixtures = Path(__file__).parent / "fixtures" / "registry"
    registry.run_fetch(
        lambda: PoliteClient(_policy(), httpx.MockTransport(registry.replay_handler(fixtures))),
        registry.RegistryPaths(tmp_path),
        tmp_path / "warehouse" / "none.duckdb",
    )
    eco = [r for r in _silver(tmp_path, "packages") if r["source"] == "ecosystems"]
    assert eco and all(r["found"] for r in eco)  # no `found = false` from a keyless record
