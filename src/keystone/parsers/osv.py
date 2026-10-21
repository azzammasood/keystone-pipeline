"""OSV `<ecosystem>/all.zip` to typed silver rows (vulns, affected packages, repo pointers).

Each zip member is one OSV record (JSON). The parser reads only the allow-listed fields of
`models.OsvVulnRow` and friends; free text and `credits` (people) are never read. A record that
does not validate goes to quarantine with a reason and no content. Repository pointers are only
GIT ranges (they belong to one affected package) and FIX references (they belong to the record);
advisory, web and package links point at databases and registries, not at the source repo.
"""

import hashlib
import re
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict

import orjson
from pydantic import ValidationError

from keystone.parsers.models import (
    OsvAffectedRow,
    OsvQuarantineRow,
    OsvRepoRefRow,
    OsvVulnRow,
    RepoRefKind,
)

_TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$")
# host/owner/name, then nothing, `.git`, or a path (commit, pull, tree, ...). GitLab subgroups
# (group/sub/proj) are cut to group/sub; harmless while only github.com rows get a repo id.
_REPO = re.compile(
    r"^(?:https?|git)://(?:www\.)?(github\.com|gitlab\.com|bitbucket\.org)/"
    r"([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+?)(?:\.git)?(?:[/#?].*)?$"
)
# GitHub paths that are not repositories
_NOT_OWNERS = frozenset({"advisories", "orgs", "users", "topics", "marketplace", "sponsors"})


@dataclass(frozen=True)
class ZipContext:
    source_file: str
    source_sha256: str
    fetched_at: datetime


class _Lineage(TypedDict):
    source_file: str
    source_sha256: str
    fetched_at: datetime
    source_member: str


@dataclass
class OsvResult:
    vulns: list[OsvVulnRow] = field(default_factory=list)
    affected: list[OsvAffectedRow] = field(default_factory=list)
    repo_refs: list[OsvRepoRefRow] = field(default_factory=list)
    quarantine: list[OsvQuarantineRow] = field(default_factory=list)


def parse_ts(raw: object) -> datetime | None:
    """RFC 3339 with up to nanoseconds; fractions beyond microseconds are cut, never rounded."""
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError(f"timestamp is {type(raw).__name__}")
    match = _TS.match(raw)
    if not match:
        raise ValueError(f"unparseable timestamp {raw!r}")
    frac = (match.group(2) or "")[:6].ljust(6, "0")
    zone = "+00:00" if match.group(3) == "Z" else match.group(3)
    return datetime.fromisoformat(f"{match.group(1)}.{frac}{zone}")


def repo_of(url: str) -> tuple[str, str] | None:
    """(host, owner/name in lower case) for a GitHub, GitLab or Bitbucket URL, else None."""
    match = _REPO.match(url.strip())
    if not match:
        return None
    host, owner, name = match.groups()
    if host == "github.com" and owner.lower() in _NOT_OWNERS:
        return None
    return host, f"{owner}/{name}".lower()


def _cvss(record: dict[str, Any], kind: str) -> str | None:
    vectors = [s.get("score") for s in record.get("severity") or [] if s.get("type") == kind]
    return vectors[0] if vectors and isinstance(vectors[0], str) else None


def parse_record(
    data: dict[str, Any], member: str, ctx: ZipContext
) -> tuple[OsvVulnRow, list[OsvAffectedRow], list[OsvRepoRefRow]]:
    lineage: _Lineage = {
        "source_file": ctx.source_file,
        "source_sha256": ctx.source_sha256,
        "fetched_at": ctx.fetched_at,
        "source_member": member,
    }
    vuln_id = data["id"]
    db = data.get("database_specific") or {}
    affected_raw = data.get("affected") or []
    modified = parse_ts(data["modified"])
    if modified is None:
        raise ValueError("modified is null")
    vuln = OsvVulnRow(
        **lineage,
        vuln_id=vuln_id,
        schema_version=data.get("schema_version"),
        published_at=parse_ts(data.get("published")),
        modified_at=modified,
        withdrawn_at=parse_ts(data.get("withdrawn")),
        alias_count=len(data.get("aliases") or []),
        affected_count=len(affected_raw),
        # MAL- ids come from the OpenSSF malicious-packages feed (also marked by its origins key)
        is_malicious=vuln_id.startswith("MAL-") or "malicious-packages-origins" in db,
        severity_label=db.get("severity") if isinstance(db.get("severity"), str) else None,
        cvss_v3_vector=_cvss(data, "CVSS_V3"),
        cvss_v4_vector=_cvss(data, "CVSS_V4"),
        github_reviewed=db.get("github_reviewed"),
    )
    affected: list[OsvAffectedRow] = []
    refs: list[OsvRepoRefRow] = []
    seen: set[tuple[int | None, RepoRefKind, str]] = set()

    def add_ref(index: int | None, kind: RepoRefKind, url: str) -> None:
        repo = repo_of(url)
        if repo is None or (index, kind, repo[1]) in seen:
            return
        seen.add((index, kind, repo[1]))
        refs.append(
            OsvRepoRefRow(
                **lineage,
                vuln_id=vuln_id,
                affected_index=index,
                kind=kind,
                url=url,
                repo_host=repo[0],
                repo_full_name=repo[1],
            )
        )

    for index, entry in enumerate(affected_raw):
        package = entry.get("package")
        if package is None:  # OSV allows affected entries with ranges only (GIT, no package)
            for rng in entry.get("ranges") or []:
                if rng.get("type") == "GIT" and isinstance(rng.get("repo"), str):
                    add_ref(index, "git_range", rng["repo"])
            continue
        ranges = entry.get("ranges") or []
        affected.append(
            OsvAffectedRow(
                **lineage,
                vuln_id=vuln_id,
                affected_index=index,
                ecosystem=package["ecosystem"],
                package_name=package["name"],
                purl=package.get("purl"),
                range_count=len(ranges),
                version_count=len(entry.get("versions") or []),
                has_fixed=any("fixed" in ev for r in ranges for ev in r.get("events") or []),
            )
        )
        for rng in ranges:
            if rng.get("type") == "GIT" and isinstance(rng.get("repo"), str):
                add_ref(index, "git_range", rng["repo"])
    for ref in data.get("references") or []:
        if ref.get("type") == "FIX" and isinstance(ref.get("url"), str):
            add_ref(None, "fix_reference", ref["url"])
    return vuln, affected, refs


def parse_zip(path: Path, ctx: ZipContext) -> OsvResult:
    result = OsvResult()
    with zipfile.ZipFile(path) as archive:
        for member in sorted(archive.namelist()):
            if not member.endswith(".json"):
                continue
            raw = archive.read(member)
            try:
                data = orjson.loads(raw)
                if not isinstance(data, dict):
                    raise ValueError("record is not an object")
                vuln, affected, refs = parse_record(data, member, ctx)
            except (orjson.JSONDecodeError, ValidationError, ValueError, KeyError, TypeError) as e:
                result.quarantine.append(
                    OsvQuarantineRow(
                        source_file=ctx.source_file,
                        source_sha256=ctx.source_sha256,
                        fetched_at=ctx.fetched_at,
                        source_member=member,
                        reason=_reason(e),
                        member_sha256=hashlib.sha256(raw).hexdigest(),
                        member_bytes=len(raw),
                    )
                )
                continue
            result.vulns.append(vuln)
            result.affected.extend(affected)
            result.repo_refs.extend(refs)
    return result


def _reason(exc: Exception) -> str:
    """Error class and field locations only: messages may quote record content."""
    if isinstance(exc, ValidationError):
        locs = sorted({".".join(str(p) for p in err["loc"]) for err in exc.errors()})
        return f"validation: {', '.join(locs)}"[:200]
    if isinstance(exc, KeyError):
        return f"missing field {exc.args[0]!r}"[:200]
    return type(exc).__name__
