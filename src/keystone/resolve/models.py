"""Silver rows of registry claims (Phase 7): which repository a registry says a package lives in.

Allow-list projection as for GH Archive and OSV: package and repository facts only.
Maintainers, authors, emails, READMEs and funding links are never read into a row.
"""

from typing import Literal

from keystone.parsers.models import Sha256, Utc, _Row

RegistrySource = Literal["ecosystems", "npm_registry"]


class _RegistryRow(_Row):
    source_file: str
    source_sha256: Sha256
    fetched_at: Utc


class RegistryPackageRow(_RegistryRow):
    """One lookup of one package in one source; `found` false when the source has no record."""

    source: RegistrySource
    ecosystem: str  # OSV spelling: PyPI, npm, crates.io, ...
    package_name: str
    package_key: str  # ecosystem/name, normalised as the registry compares names
    found: bool
    critical: bool | None
    repository_url: str | None  # as the source spells it
    repo_host: str | None
    repo_full_name: str | None  # owner/name, lower case
    repo_subpath: str | None  # a directory inside the repository (monorepo)
    url_rejected: str | None  # why repository_url names no repository
    repo_github_id: int | None  # ecosyste.ms repo_metadata.uuid on GitHub (the GitHub id)
    repo_current_name: str | None  # ecosyste.ms repo_metadata.full_name, lower case
    repo_fork: bool | None
    repo_archived: bool | None
    dependent_packages: int | None
    dependent_repos: int | None
    downloads: int | None
    downloads_period: str | None
    latest_release_at: Utc | None
    etag: str | None


class RegistryRepoNameRow(_RegistryRow):
    """A name a GitHub repository has or had, per ecosyste.ms (`previous_names`)."""

    repo_github_id: int
    repo_name_lower: str
    kind: Literal["current", "previous"]


class RegistryQuarantineRow(_RegistryRow):
    """An unusable record; no record content."""

    source: RegistrySource
    package_key: str | None
    reason: str
