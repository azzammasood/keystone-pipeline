"""Registry claims for package-to-repository mapping (Phase 7, ADR-010 and ADR-037).

Two sources, both through the polite client:
- ecosyste.ms packages API: the critical sets (`package_names?critical=true`) and
  `POST /packages/bulk_lookup` with at most 100 purls per request. Each package gives the
  repository URL ecosyste.ms derived, the GitHub id of that repository (`repo_metadata.uuid`)
  and its previous names. The 5,000 requests/hour anonymous quota is guarded by policy.yml.
- npm registry `/<name>/latest`: the `repository` field the maintainers published, as an
  independent cross-check. PyPI's JSON API is disallowed by pypi.org's robots.txt
  (`Disallow: /pypi/*/json`), so PyPI has no registry cross-check here.

Only allow-listed fields reach silver; every row keeps its lineage. A package fetched within
`refresh_days` is not fetched again; npm refreshes are conditional (ETag).
"""

import asyncio
import hashlib
import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import quote

import duckdb
import httpx

from keystone.fetch.client import FetchError, PoliteClient, QuotaExhausted
from keystone.parsers.osv import parse_ts
from keystone.parsers.parquet import arrow_schema, write_rows
from keystone.resolve.models import (
    RegistryPackageRow,
    RegistryQuarantineRow,
    RegistryRepoNameRow,
    RegistrySource,
)
from keystone.resolve.urls import Rejected, RepoUrl, normalize, package_key

ECOSYSTEMS = "https://packages.ecosyste.ms/api/v1"
NPM = "https://registry.npmjs.org"
BULK_MAX = 100
# purl type <-> OSV ecosystem name (the spelling dim_package uses)
PURL_TO_OSV = {
    "pypi": "PyPI", "npm": "npm", "cargo": "crates.io", "golang": "Go", "maven": "Maven",
    "nuget": "NuGet", "gem": "RubyGems", "composer": "Packagist", "hex": "Hex", "pub": "Pub",
    "swift": "SwiftURL",
}  # fmt: skip
CRITICAL_REGISTRIES = {"PyPI": "pypi.org", "npm": "npmjs.org"}

PACKAGE_SCHEMA = arrow_schema(RegistryPackageRow)
NAME_SCHEMA = arrow_schema(RegistryRepoNameRow)
QUARANTINE_SCHEMA = arrow_schema(RegistryQuarantineRow)


class Lineage(TypedDict):
    source_file: str
    source_sha256: str
    fetched_at: datetime


@dataclass(frozen=True)
class Wanted:
    ecosystem: str
    name: str
    purl: str

    @property
    def key(self) -> str:
        return package_key(self.ecosystem, self.name)


def purl_for(ecosystem: str, name: str) -> str | None:
    kind = {v: k for k, v in PURL_TO_OSV.items()}.get(ecosystem)
    if kind is None:
        return None
    if kind == "maven" and ":" in name:
        group, artifact = name.split(":", 1)
        return f"pkg:maven/{quote(group)}/{quote(artifact)}"
    if kind == "npm" and name.startswith("@") and "/" in name:
        scope, rest = name.split("/", 1)
        return f"pkg:npm/{quote(scope)}/{quote(rest)}"
    if kind == "golang":
        return f"pkg:golang/{name}"
    return f"pkg:{kind}/{quote(name)}"


@dataclass
class FetchStats:
    requested: dict[str, int] = field(default_factory=dict)
    found: dict[str, int] = field(default_factory=dict)
    skipped_fresh: dict[str, int] = field(default_factory=dict)
    not_modified: int = 0
    requests: int = 0
    bytes: int = 0
    stopped: str | None = None  # quota floor or error that ended the run early
    quota_left: int | None = None  # ecosyste.ms requests left this hour, as last reported
    quota_limit: int | None = None


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _repo_fields(url: str | None) -> dict[str, Any]:
    got = normalize(url)
    if isinstance(got, Rejected):
        return {
            "repo_host": None,
            "repo_full_name": None,
            "repo_subpath": None,
            "url_rejected": None if url is None else got.reason,
        }
    assert isinstance(got, RepoUrl)
    return {
        "repo_host": got.host,
        "repo_full_name": got.full_name,
        "repo_subpath": got.subpath,
        "url_rejected": None,
    }


def _int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def ecosystems_rows(
    item: dict[str, Any], wanted: Wanted, lineage: Lineage
) -> tuple[RegistryPackageRow, list[RegistryRepoNameRow]]:
    """Project one ecosyste.ms package record to the allow-listed row (and its repo names)."""
    repo = item.get("repo_metadata") or {}
    host = repo.get("host") or {}
    on_github = isinstance(host, dict) and host.get("name") == "GitHub"
    github_id = _int(repo.get("uuid")) if on_github else None
    current = repo.get("full_name")
    current = current.lower() if isinstance(current, str) and on_github else None
    released = item.get("latest_release_published_at")
    row = RegistryPackageRow(
        **lineage,
        source="ecosystems",
        ecosystem=wanted.ecosystem,
        package_name=wanted.name,
        package_key=wanted.key,
        found=True,
        critical=bool(item["critical"]) if item.get("critical") is not None else None,
        repository_url=item.get("repository_url") or None,
        **_repo_fields(item.get("repository_url") or None),
        repo_github_id=github_id,
        repo_current_name=current,
        repo_fork=repo.get("fork") if isinstance(repo.get("fork"), bool) else None,
        repo_archived=repo.get("archived") if isinstance(repo.get("archived"), bool) else None,
        dependent_packages=_int(item.get("dependent_packages_count")),
        dependent_repos=_int(item.get("dependent_repos_count")),
        downloads=_int(item.get("downloads")),
        downloads_period=item.get("downloads_period") or None,
        latest_release_at=parse_ts(released) if released else None,
        etag=None,
    )
    names: list[RegistryRepoNameRow] = []
    if github_id is not None and current:
        names.append(
            RegistryRepoNameRow(
                **lineage, repo_github_id=github_id, repo_name_lower=current, kind="current"
            )
        )
        for prev in repo.get("previous_names") or []:
            if isinstance(prev, str) and "/" in prev and prev.lower() != current:
                names.append(
                    RegistryRepoNameRow(
                        **lineage,
                        repo_github_id=github_id,
                        repo_name_lower=prev.lower(),
                        kind="previous",
                    )
                )
    return row, names


def missing_row(
    source: RegistrySource, wanted: Wanted, lineage: Lineage, etag: str | None = None
) -> RegistryPackageRow:
    """The source was asked and has no record: a measured absence, not a guess."""
    return RegistryPackageRow(
        **lineage,
        source=source,
        ecosystem=wanted.ecosystem,
        package_name=wanted.name,
        package_key=wanted.key,
        found=False,
        critical=None,
        repository_url=None,
        repo_host=None,
        repo_full_name=None,
        repo_subpath=None,
        url_rejected=None,
        repo_github_id=None,
        repo_current_name=None,
        repo_fork=None,
        repo_archived=None,
        dependent_packages=None,
        dependent_repos=None,
        downloads=None,
        downloads_period=None,
        latest_release_at=None,
        etag=etag,
    )


def npm_row(
    doc: dict[str, Any], wanted: Wanted, lineage: Lineage, etag: str | None
) -> RegistryPackageRow:
    """The npm `repository` field (string or {type, url, directory}) as a claim."""
    repo = doc.get("repository")
    url: str | None
    directory: str | None = None
    if isinstance(repo, str):
        url = repo
    elif isinstance(repo, dict):
        url = repo.get("url") if isinstance(repo.get("url"), str) else None
        directory = repo.get("directory") if isinstance(repo.get("directory"), str) else None
    else:
        url = None
    fields = _repo_fields(url)
    if directory and fields["repo_full_name"] and not fields["repo_subpath"]:
        fields["repo_subpath"] = directory.strip("/") or None
    base = missing_row("npm_registry", wanted, lineage, etag).model_dump()
    base.update(found=True, repository_url=url, **fields)
    return RegistryPackageRow(**base)


def _ecosystem_of(item: dict[str, Any]) -> str | None:
    purl = item.get("purl")
    if not isinstance(purl, str) or not purl.startswith("pkg:"):
        return None
    return PURL_TO_OSV.get(purl[4:].split("/", 1)[0])


async def critical_names(client: PoliteClient, ecosystem: str) -> list[str]:
    """Names ecosyste.ms flags `critical` in one registry (pages of 1,000)."""
    registry = CRITICAL_REGISTRIES[ecosystem]
    names: list[str] = []
    page = 1
    while True:
        url = (
            f"{ECOSYSTEMS}/registries/{registry}/package_names"
            f"?critical=true&per_page=1000&page={page}"
        )
        async with client.request("GET", url) as response:
            if response.status_code != 200:
                raise FetchError(f"GET {url}: HTTP {response.status_code}")
            batch = json.loads(await response.aread())
        names.extend(n for n in batch if isinstance(n, str))
        if len(batch) < 1000:
            return names
        page += 1


async def bulk_lookup(
    client: PoliteClient,
    wanted: Sequence[Wanted],
    stats: FetchStats,
    on_batch: Any,
) -> None:
    """Look packages up 100 at a time; `on_batch(packages, names, quarantine)` after each."""
    url = f"{ECOSYSTEMS}/packages/bulk_lookup"
    for start in range(0, len(wanted), BULK_MAX):
        batch = list(wanted[start : start + BULK_MAX])
        async with client.request(
            "POST", url, json_body={"purls": [w.purl for w in batch]}
        ) as response:
            body = await response.aread()
            status = response.status_code
        stats.requests += 1
        stats.bytes += len(body)
        if status != 200:
            raise FetchError(f"POST {url}: HTTP {status}")
        lineage = Lineage(
            source_file=f"{url}#{start // BULK_MAX}",
            source_sha256=_sha(body),
            fetched_at=datetime.now(UTC),
        )
        by_key = {w.key: w for w in batch}
        packages: dict[str, RegistryPackageRow] = {}
        names: list[RegistryRepoNameRow] = []
        quarantine: list[RegistryQuarantineRow] = []
        items = json.loads(body)
        for item in items if isinstance(items, list) else []:
            eco = _ecosystem_of(item) if isinstance(item, dict) else None
            name = item.get("name") if isinstance(item, dict) else None
            if eco is None or not isinstance(name, str):
                quarantine.append(
                    RegistryQuarantineRow(
                        **lineage, source="ecosystems", package_key=None, reason="no_purl_or_name"
                    )
                )
                continue
            w = by_key.get(package_key(eco, name))
            if w is None or w.key in packages:
                continue  # a record we did not ask for, or a second registry for one name
            try:
                row, repo_names = ecosystems_rows(item, w, lineage)
            except (ValueError, TypeError) as exc:
                quarantine.append(
                    RegistryQuarantineRow(
                        **lineage, source="ecosystems", package_key=w.key, reason=type(exc).__name__
                    )
                )
                continue
            packages[w.key] = row
            names.extend(repo_names)
        # An unusable record we cannot tie to a package might have been any unmatched one:
        # then no absence is recorded for this batch (they are asked again next run).
        keyless = any(q.package_key is None for q in quarantine)
        for w in batch:
            if w.key in packages or keyless:
                continue
            if not any(q.package_key == w.key for q in quarantine):
                packages[w.key] = missing_row("ecosystems", w, lineage)
        stats.found["ecosystems"] = stats.found.get("ecosystems", 0) + sum(
            r.found for r in packages.values()
        )
        on_batch(list(packages.values()), names, quarantine)


def npm_path(name: str) -> str:
    return quote(name, safe="@") if not name.startswith("@") else "@" + quote(name[1:], safe="")


async def npm_lookup(
    client: PoliteClient,
    wanted: Sequence[Wanted],
    etags: dict[str, tuple[str, RegistryPackageRow]],
    stats: FetchStats,
    on_rows: Any,
    flush_every: int = 200,
) -> None:
    """`/<name>/latest` per package; a 304 keeps the previous claim with a new `fetched_at`."""
    rows: list[RegistryPackageRow] = []
    for w in wanted:
        url = f"{NPM}/{npm_path(w.name)}/latest"
        prev = etags.get(w.key)
        headers = {"If-None-Match": prev[0]} if prev else {}
        async with client.request("GET", url, headers) as response:
            body = await response.aread()
            status, etag = response.status_code, response.headers.get("etag")
        stats.requests += 1
        stats.bytes += len(body)
        now = datetime.now(UTC)
        if status == 304 and prev:
            stats.not_modified += 1
            rows.append(prev[1].model_copy(update={"fetched_at": now}))
        elif status == 404:
            lineage = Lineage(source_file=url, source_sha256=_sha(body), fetched_at=now)
            rows.append(missing_row("npm_registry", w, lineage))
        elif status == 200:
            lineage = Lineage(source_file=url, source_sha256=_sha(body), fetched_at=now)
            try:
                doc = json.loads(body)
                if not isinstance(doc, dict):
                    raise ValueError("not an object")
                rows.append(npm_row(doc, w, lineage, etag))
                stats.found["npm_registry"] = stats.found.get("npm_registry", 0) + 1
            except ValueError as exc:
                on_rows(
                    [],
                    [
                        RegistryQuarantineRow(
                            **lineage,
                            source="npm_registry",
                            package_key=w.key,
                            reason=str(exc)[:80],
                        )
                    ],
                )
        else:
            raise FetchError(f"GET {url}: HTTP {status}")
        if len(rows) >= flush_every:
            on_rows(rows, [])
            rows = []
    if rows:
        on_rows(rows, [])


@dataclass(frozen=True)
class RegistryPaths:
    root: Path

    @property
    def silver(self) -> Path:
        return self.root / "silver" / "registry"

    def table(self, name: str) -> Path:
        return self.silver / name

    @property
    def quarantine(self) -> Path:
        return self.root / "quarantine" / "registry"


def ensure_tables(paths: RegistryPaths) -> None:
    """Zero-row files so the warehouse can read a source that was never fetched (CI, fixtures).
    An empty table is a measured "nothing fetched", never a stand-in for data."""
    for name, schema in (("packages", PACKAGE_SCHEMA), ("repo_names", NAME_SCHEMA)):
        dest = paths.table(name) / "_empty.parquet"
        if not dest.exists():
            write_rows([], schema, dest)


def latest_rows(paths: RegistryPaths, source: RegistrySource) -> dict[str, RegistryPackageRow]:
    """The newest row per package of one source, from silver."""
    files = list(paths.table("packages").glob("*.parquet"))
    if not files:
        return {}
    con = duckdb.connect()
    try:
        rel = con.execute(
            "SELECT * FROM read_parquet(?) WHERE source = ? "
            "QUALIFY row_number() OVER (PARTITION BY package_key ORDER BY fetched_at DESC) = 1",
            [[str(f) for f in files], source],
        )
        cols = [d[0] for d in rel.description]
        return {
            r["package_key"]: RegistryPackageRow(**r)
            for r in (dict(zip(cols, row, strict=True)) for row in rel.fetchall())
        }
    finally:
        con.close()


def universe(warehouse_db: Path, critical: dict[str, list[str]]) -> list[Wanted]:
    """Packages to resolve: OSV packages that are not only malware reports (dim_package) plus
    the ecosyste.ms critical sets. One entry per normalised key."""
    out: dict[str, Wanted] = {}
    for eco, names in critical.items():
        for name in names:
            purl = purl_for(eco, name)
            if purl:
                out.setdefault(package_key(eco, name), Wanted(eco, name, purl))
    if warehouse_db.exists():
        con = duckdb.connect(str(warehouse_db), read_only=True)
        try:
            rows = con.execute(
                "SELECT ecosystem, package_name FROM marts.dim_package "
                "WHERE vuln_count > malicious_vuln_count ORDER BY package_key"
            ).fetchall()
        finally:
            con.close()
        for eco, name in rows:
            purl = purl_for(eco, name)
            if purl:
                out.setdefault(package_key(eco, name), Wanted(eco, name, purl))
    return sorted(out.values(), key=lambda w: w.key)


def _stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


async def fetch(
    client: PoliteClient,
    paths: RegistryPaths,
    warehouse_db: Path,
    *,
    ecosystems: Iterable[str] = ("PyPI", "npm"),
    refresh_days: int = 7,
    npm: bool = True,
    limit: int | None = None,
) -> FetchStats:
    """Fetch what is missing or stale; write each batch as it arrives (a stop keeps the work)."""
    stats = FetchStats()
    ensure_tables(paths)
    cutoff = datetime.now(UTC) - timedelta(days=refresh_days)

    def stale(source: RegistrySource, items: list[Wanted]) -> list[Wanted]:
        have = latest_rows(paths, source)
        # A bulk lookup is cheap (100 per request): ecosyste.ms absences are asked again
        # every run, since its index grows; everything else waits refresh_days.
        todo = [
            w
            for w in items
            if w.key not in have
            or have[w.key].fetched_at < cutoff
            or (source == "ecosystems" and not have[w.key].found)
        ]
        stats.requested[source] = len(todo)
        stats.skipped_fresh[source] = len(items) - len(todo)
        return todo

    def write_batch(
        packages: list[RegistryPackageRow],
        names: list[RegistryRepoNameRow],
        quarantine: list[RegistryQuarantineRow],
    ) -> None:
        stamp = _stamp()
        if packages:
            write_rows(packages, PACKAGE_SCHEMA, paths.table("packages") / f"eco-{stamp}.parquet")
        if names:
            write_rows(names, NAME_SCHEMA, paths.table("repo_names") / f"eco-{stamp}.parquet")
        if quarantine:
            write_rows(quarantine, QUARANTINE_SCHEMA, paths.quarantine / f"eco-{stamp}.parquet")

    try:
        critical = {eco: await critical_names(client, eco) for eco in ecosystems}
        wanted = universe(warehouse_db, critical)
        if limit is not None:
            wanted = wanted[:limit]
        await bulk_lookup(client, stale("ecosystems", wanted), stats, write_batch)
        if npm:
            npm_wanted = [w for w in wanted if w.ecosystem == "npm"]
            todo = stale("npm_registry", npm_wanted)
            have = latest_rows(paths, "npm_registry")
            etags = {k: (r.etag, r) for k, r in have.items() if r.etag is not None and r.found}

            def write_npm(rows: list[RegistryPackageRow], q: list[RegistryQuarantineRow]) -> None:
                stamp = _stamp()
                if rows:
                    write_rows(
                        rows, PACKAGE_SCHEMA, paths.table("packages") / f"npm-{stamp}.parquet"
                    )
                if q:
                    write_rows(q, QUARANTINE_SCHEMA, paths.quarantine / f"npm-{stamp}.parquet")

            await npm_lookup(client, todo, etags, stats, write_npm)
    except QuotaExhausted as exc:
        stats.stopped = str(exc)
    return stats


def run_fetch(
    client_factory: Any, paths: RegistryPaths, warehouse_db: Path, **kw: Any
) -> FetchStats:
    async def go() -> FetchStats:
        async with client_factory() as client:
            stats: FetchStats = await fetch(client, paths, warehouse_db, **kw)
            stats.requests = client.requests
            stats.quota_left, stats.quota_limit = client.quota("packages.ecosyste.ms")
            return stats

    return asyncio.run(go())


def replay_handler(folder: Path) -> Callable[[httpx.Request], httpx.Response]:
    """Serve recorded responses (tests/fixtures/registry) for CI and tests: the same parsing
    and projection run, with no network. Layout: `package_names-<registry>.json`,
    `bulk_lookup.json` (filtered to the requested purls), `npm/<name>.json`."""

    def handle(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/robots.txt":
            return httpx.Response(404)
        if path.endswith("/package_names"):
            registry_name = path.split("/registries/", 1)[1].split("/", 1)[0]
            page = int(request.url.params.get("page", "1"))
            file = folder / f"package_names-{registry_name}.json"
            if page > 1 or not file.exists():
                return httpx.Response(200, json=[])
            return httpx.Response(200, content=file.read_bytes())
        if path.endswith("/packages/bulk_lookup"):
            wanted = set(json.loads(request.content)["purls"])
            items = json.loads((folder / "bulk_lookup.json").read_text("utf-8"))
            # records without a purl are served to every batch: they exercise the quarantine
            served = [i for i in items if i.get("purl") in wanted or "purl" not in i]
            return httpx.Response(200, json=served)
        if request.url.host == "registry.npmjs.org" and path.endswith("/latest"):
            name = path[1 : -len("/latest")].replace("%2F", "/").replace("%2f", "/")
            file = folder / "npm" / f"{name}.json"
            if file.exists():
                return httpx.Response(200, content=file.read_bytes())
        return httpx.Response(404)

    return handle
