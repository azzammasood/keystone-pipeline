"""Write tests/fixtures/osv/PyPI/all.zip: synthetic OSV records (no real people or advisories).

Run with `uv run python tests/fixtures/osv/make_fixture.py`; the zip is committed. Covers a GIT
range, FIX references (one record about one package, one about two), a malicious report, a
withdrawn record, nanosecond timestamps, credits (must be dropped) and two bad records.
"""

import json
import zipfile
from pathlib import Path

OUT = Path(__file__).parent / "PyPI" / "all.zip"
T0 = "2026-01-02T03:04:05Z"

RECORDS: dict[str, object] = {
    "GHSA-aaaa-0001-0001.json": {
        "schema_version": "1.6.0",
        "id": "GHSA-aaaa-0001-0001",
        "published": T0,
        "modified": "2026-02-03T04:05:06.123456789Z",
        "aliases": ["CVE-2026-0001"],
        "summary": "free text that must not be read",
        "details": "free text that must not be read",
        "credits": [{"name": "Some Person", "contact": ["mailto:person@example.invalid"]}],
        "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H"}],
        "affected": [
            {
                "package": {"ecosystem": "PyPI", "name": "pkg-one", "purl": "pkg:pypi/pkg-one"},
                "ranges": [
                    {
                        "type": "GIT",
                        "repo": "https://github.com/Owner-1/Repo-1.git",
                        "events": [{"introduced": "0"}, {"fixed": "abc123"}],
                    },
                    {"type": "ECOSYSTEM", "events": [{"introduced": "0"}, {"fixed": "1.2.3"}]},
                ],
                "versions": ["1.0.0", "1.2.0"],
            }
        ],
        "references": [
            {"type": "FIX", "url": "https://github.com/owner-1/repo-1/commit/abc123"},
            {"type": "ADVISORY", "url": "https://github.com/advisories/GHSA-aaaa-0001-0001"},
            {"type": "WEB", "url": "https://example.invalid/post"},
        ],
        "database_specific": {"severity": "HIGH", "github_reviewed": True},
    },
    "PYSEC-2026-2.json": {
        "id": "PYSEC-2026-2",
        "published": T0,
        "modified": T0,
        "affected": [
            {
                "package": {"ecosystem": "PyPI", "name": "pkg-two"},
                "ranges": [{"type": "ECOSYSTEM", "events": [{"introduced": "0"}]}],
            },
            {
                "package": {"ecosystem": "PyPI", "name": "pkg-three"},
                "ranges": [{"type": "ECOSYSTEM", "events": [{"introduced": "0"}]}],
            },
        ],
        "references": [{"type": "FIX", "url": "https://github.com/owner-3/repo-3/pull/7"}],
    },
    "PYSEC-2026-3.json": {
        "id": "PYSEC-2026-3",
        "published": T0,
        "modified": T0,
        "withdrawn": "2026-03-01T00:00:00Z",
        "affected": [{"package": {"ecosystem": "PyPI", "name": "pkg-two"}}],
        "references": [{"type": "FIX", "url": "https://gitlab.com/group/proj/-/commit/1"}],
    },
    "MAL-2026-4.json": {
        "id": "MAL-2026-4",
        "published": T0,
        "modified": T0,
        "affected": [{"package": {"ecosystem": "PyPI", "name": "evil-pkg"}, "versions": ["0.1"]}],
        "database_specific": {"malicious-packages-origins": [{"source": "test"}], "iocs": {}},
    },
    "BAD-no-modified.json": {"id": "BAD-1", "affected": []},
    "BAD-ts.json": {"id": "BAD-2", "modified": "yesterday", "affected": []},
}


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, record in sorted(RECORDS.items()):
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))  # reproducible bytes
            archive.writestr(info, json.dumps(record, sort_keys=True), zipfile.ZIP_DEFLATED)


if __name__ == "__main__":
    main()
