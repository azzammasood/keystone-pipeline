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
