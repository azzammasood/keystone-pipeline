"""Versions of the services a stage used, read from the running service where it exposes one;
otherwise from the pinned image tag in compose (the version that ran, by construction)."""

import re
from functools import cache
from importlib.metadata import PackageNotFoundError, version

import httpx

from keystone.paths import REPO_ROOT

COMPOSE_FILE = REPO_ROOT / "compose" / "docker-compose.yml"


def package(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "unknown"


@cache
def compose_tag(image: str) -> str:
    """Tag of `image` pinned in compose/docker-compose.yml."""
    text = COMPOSE_FILE.read_text(encoding="utf-8")
    m = re.search(rf"image:\s*(?:&\w+\s+)?\S*{re.escape(image)}:([\w.\-]+)", text)
    return m.group(1) if m else "unknown"


def http_json(url: str, timeout: float = 5.0) -> object | None:
    try:
        r = httpx.get(url, timeout=timeout)
        r.raise_for_status()
        data: object = r.json()
        return data
    except (httpx.HTTPError, ValueError):
        return None


def flink() -> str:
    cfg = http_json("http://127.0.0.1:8081/config")
    if isinstance(cfg, dict) and cfg.get("flink-version"):
        return str(cfg["flink-version"])
    return compose_tag("keystone-flink")


def flink_last_job_records(name_prefix: str) -> tuple[int | None, int | None]:
    """(records read by the sources, records written by the sinks) of the newest finished job
    whose name starts with `name_prefix`, from the Flink REST API; (None, None) if unknown."""
    jobs = http_json("http://127.0.0.1:8081/jobs/overview")
    if not isinstance(jobs, dict):
        return None, None
    done = [j for j in jobs.get("jobs", []) if str(j.get("name", "")).startswith(name_prefix)]
    if not done:
        return None, None
    job = max(done, key=lambda j: j.get("end-time", 0))
    detail = http_json(f"http://127.0.0.1:8081/jobs/{job['jid']}")
    if not isinstance(detail, dict):
        return None, None
    vertices = detail.get("vertices", [])
    read = sum(
        int(v["metrics"].get("write-records", 0))
        for v in vertices
        if "Source" in str(v.get("name", ""))
    )
    written = sum(
        int(v["metrics"].get("read-records", 0))
        for v in vertices
        if "Sink" in str(v.get("name", "")) or "Writer" in str(v.get("name", ""))
    )
    return read or None, written or None
