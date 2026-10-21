"""Repository URLs as registries spell them, reduced to `host` + `owner/name` (Phase 7).

Registries accept many spellings of one repository: `git+https://github.com/o/n.git`,
`git://github.com/o/n`, `git+ssh://git@github.com/o/n.git`, `git@github.com:o/n.git`,
`github:o/n` and the npm shorthand `o/n`. A path below the repository (`/tree/main/pkg`) is
kept as `subpath`: the package lives in a monorepo directory, which is a weaker claim about
the repository. Anything that is not a repository on a known forge is rejected with a reason,
never guessed.
"""

import re
from dataclasses import dataclass

FORGES = ("github.com", "gitlab.com", "bitbucket.org")
_SHORTHAND = {"github": "github.com", "gitlab": "gitlab.com", "bitbucket": "bitbucket.org"}
# GitHub first path segments that are not owners (pages of the site, not repositories).
_NOT_OWNERS = frozenset(
    {
        "advisories", "apps", "collections", "features", "login", "marketplace", "orgs",
        "settings", "sponsors", "topics", "users", "pulls", "issues", "notifications",
        "about", "enterprise", "explore", "site", "security",
    }
)  # fmt: skip
_SEGMENT = re.compile(r"^[A-Za-z0-9_.-]+$")
_SCP = re.compile(r"^(?:[\w.-]+@)?([\w.-]+):(?!//)(.+)$")  # git@github.com:o/n.git
_SCHEME = re.compile(
    r"^(?:git\+)?(?:https?|git|ssh|git\+ssh)://(?:[^@/]+@)?([^/:]+)(?::\d+)?(/.*)?$"
)


@dataclass(frozen=True)
class RepoUrl:
    host: str
    full_name: str  # owner/name, lower case (forge names are case-insensitive)
    subpath: str | None = None


@dataclass(frozen=True)
class Rejected:
    reason: str


def normalize(raw: str | None) -> RepoUrl | Rejected:
    """The repository a URL points at, or why it does not point at one."""
    if raw is None or not raw.strip():
        return Rejected("empty")
    url = raw.strip()
    host: str
    path: str
    short = re.match(r"^(github|gitlab|bitbucket):([^/].*)$", url)
    if short:
        host, path = _SHORTHAND[short.group(1)], short.group(2)
    elif (scheme := _SCHEME.match(url)) is not None:
        host, path = scheme.group(1), scheme.group(2) or ""
    elif (scp := _SCP.match(url)) is not None and "." in scp.group(1):
        host, path = scp.group(1), scp.group(2)
    elif re.match(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", url):
        host, path = "github.com", url  # npm `owner/name` shorthand means GitHub
    elif url.lower().startswith(tuple(f"{f}/" for f in FORGES + ("www.github.com",))):
        host, _, path = url.partition("/")
    else:
        return Rejected("not_a_url")
    host = host.lower().removeprefix("www.")
    if host not in FORGES:
        return Rejected("other_host")
    path = re.split(r"[?#]", path, maxsplit=1)[0]
    parts = [p for p in path.split("/") if p]
    if len(parts) < 2:
        return Rejected("no_repository_path")
    owner, name = parts[0], parts[1].removesuffix(".git")
    if not (_SEGMENT.match(owner) and _SEGMENT.match(name)) or name in (".", ".."):
        return Rejected("bad_segment")
    if host == "github.com" and owner.lower() in _NOT_OWNERS:
        return Rejected("not_a_repository")
    if host == "github.com" and name.lower().endswith(".wiki"):
        return Rejected("not_a_repository")
    rest = parts[2:]
    subpath = None
    if len(rest) >= 3 and rest[0] in ("tree", "blob", "-"):
        subpath = "/".join(rest[2:]) if rest[0] != "-" else "/".join(rest[3:]) or None
    return RepoUrl(host, f"{owner}/{name}".lower(), subpath)


def pypi_name(name: str) -> str:
    """PEP 503 normalised project name: lower case, runs of `-`, `_`, `.` become `-`."""
    return re.sub(r"[-_.]+", "-", name).lower()


def package_key(ecosystem: str, name: str) -> str:
    """`Ecosystem/name` with the name normalised as the registry compares it: PEP 503 for
    PyPI, lower case for NuGet (ids are case-insensitive), as spelled elsewhere."""
    if ecosystem == "PyPI":
        return f"PyPI/{pypi_name(name)}"
    if ecosystem == "NuGet":
        return f"NuGet/{name.lower()}"
    return f"{ecosystem}/{name}"
