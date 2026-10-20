"""Typed silver rows for GH Archive (one per event), quarantine and per-hour type counts.

The event row is an allow-list projection: only repository-level facts, event facts and salted
person keys. Logins, emails, names, bodies, titles and profile fields are never read into it.
Every row keeps its lineage (`source_file`, `source_sha256`, `fetched_at`, `source_line`).
"""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

Era = Literal["E0", "E1", "E2", "E3"]
KeyKind = Literal["id", "login"]


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp without an offset")  # never assume a zone
    return value.astimezone(UTC)


Utc = Annotated[datetime, AfterValidator(_aware_utc)]
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Key = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]


class _Row(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class EventRow(_Row):
    source_file: str
    source_sha256: Sha256
    fetched_at: Utc
    source_line: int = Field(ge=1)
    era: Era
    period: str
    event_id: str | None
    event_type: str
    created_at: Utc
    created_at_offset_min: int = Field(ge=-14 * 60, le=14 * 60)
    public: bool | None
    actor_key: Key | None
    actor_key_kind: KeyKind | None
    actor_is_bot: bool
    repo_id: int | None
    repo_name: str | None
    action: str | None
    number: int | None
    ref_type: str | None
    ref: str | None
    push_head: str | None
    push_before: str | None
    push_size: int | None
    push_distinct_size: int | None
    payload_repository_id: int | None
    pr_merged: bool | None
    pr_created_at: Utc | None
    pr_merged_at: Utc | None
    pr_closed_at: Utc | None
    pr_author_key: Key | None
    issue_id: int | None
    issue_created_at: Utc | None
    issue_closed_at: Utc | None
    issue_author_key: Key | None
    issue_is_pr: bool | None
    comment_id: int | None
    comment_created_at: Utc | None
    release_tag: str | None
    release_published_at: Utc | None
    release_prerelease: bool | None


class QuarantineRow(_Row):
    """An unparseable record. Holds no record content (it may contain personal data): the
    source file is identified by its SHA-256 and the line by number and hash."""

    source_file: str
    source_sha256: Sha256
    fetched_at: Utc
    source_line: int = Field(ge=1)
    era: str
    reason: str
    line_sha256: Sha256
    line_bytes: int = Field(ge=0)


class HourTypeRow(_Row):
    """Per source file and event type: parsed counts and the created_at range (UTC)."""

    source_file: str
    source_sha256: Sha256
    fetched_at: Utc
    hour: Utc
    era: Era
    period: str
    event_type: str
    events: int = Field(ge=0)
    created_at_min: Utc
    created_at_max: Utc


# OSV (one zip per ecosystem). Allow-list again: no `details`, `summary`, `credits` (people) or
# `database_specific.iocs`; only identifiers, dates, severity labels and package/repo pointers.
RepoRefKind = Literal["git_range", "fix_reference"]


class _OsvRow(_Row):
    source_file: str
    source_sha256: Sha256
    fetched_at: Utc
    source_member: str


class OsvVulnRow(_OsvRow):
    """One OSV record."""

    vuln_id: str
    schema_version: str | None
    published_at: Utc | None
    modified_at: Utc
    withdrawn_at: Utc | None
    alias_count: int = Field(ge=0)
    affected_count: int = Field(ge=0)
    is_malicious: bool
    severity_label: str | None
    cvss_v3_vector: str | None
    cvss_v4_vector: str | None
    github_reviewed: bool | None


class OsvAffectedRow(_OsvRow):
    """One `affected[]` entry: the package and whether a fixed version is known."""

    vuln_id: str
    affected_index: int = Field(ge=0)
    ecosystem: str
    package_name: str
    purl: str | None
    range_count: int = Field(ge=0)
    version_count: int = Field(ge=0)
    has_fixed: bool


class OsvRepoRefRow(_OsvRow):
    """A pointer from a record to a source repository: a GIT range of one affected package, or
    a FIX reference of the whole record (`affected_index` null)."""

    vuln_id: str
    affected_index: int | None
    kind: RepoRefKind
    url: str
    repo_host: str
    repo_full_name: str  # owner/name, lower case (GitHub names are case-insensitive)


class OsvQuarantineRow(_OsvRow):
    """An unparseable OSV record; no record content (credits may name people)."""

    reason: str
    member_sha256: Sha256
    member_bytes: int = Field(ge=0)
