"""Health probes of the `full` profile's services, from the host (the ports compose publishes).

A probe is a TCP connect (Kafka API) or an HTTP GET. Each answers within a short timeout, so a
missing stack costs seconds, not minutes. RustFS answers an anonymous S3 request with an error
status (403), which still proves the server is up; any status below 500 counts there.
"""

import socket
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Literal

import httpx

from keystone.rollup.compare import CATALOG_URL, S3_ENDPOINT
from keystone.rollup.stream import DEFAULT_BOOTSTRAP, DEFAULT_REGISTRY

FLINK_URL = "http://127.0.0.1:8081"
TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class Probe:
    name: str
    kind: Literal["tcp", "http"]
    target: str
    max_status: int = 399  # highest HTTP status that counts as healthy


@dataclass(frozen=True)
class ProbeResult:
    name: str
    target: str
    ok: bool
    seconds: float
    detail: str


FULL_PROBES: tuple[Probe, ...] = (
    Probe("redpanda", "tcp", DEFAULT_BOOTSTRAP),
    Probe("redpanda-registry", "http", f"{DEFAULT_REGISTRY}/subjects"),
    Probe("flink", "http", f"{FLINK_URL}/overview"),
    Probe("lakekeeper", "http", f"{CATALOG_URL.removesuffix('/catalog')}/health"),
    Probe("rustfs", "http", f"http://{S3_ENDPOINT}/", max_status=499),
)


def _tcp(target: str, timeout: float) -> str:
    host, _, port = target.rpartition(":")
    with socket.create_connection((host, int(port)), timeout=timeout):
        return "connected"


def _http(target: str, timeout: float, max_status: int) -> str:
    r = httpx.get(target, timeout=timeout)
    if r.status_code > max_status:
        raise RuntimeError(f"HTTP {r.status_code}")
    return f"HTTP {r.status_code}"


def run_probe(probe: Probe, timeout: float = TIMEOUT_SECONDS) -> ProbeResult:
    t0 = time.perf_counter()
    try:
        if probe.kind == "tcp":
            detail = _tcp(probe.target, timeout)
        else:
            detail = _http(probe.target, timeout, probe.max_status)
        ok = True
    except (OSError, httpx.HTTPError, RuntimeError, ValueError) as exc:
        ok, detail = False, f"{type(exc).__name__}: {exc}"[:200]
    return ProbeResult(probe.name, probe.target, ok, round(time.perf_counter() - t0, 3), detail)


def check(
    probes: Sequence[Probe] = FULL_PROBES, timeout: float = TIMEOUT_SECONDS
) -> list[ProbeResult]:
    """All probes at once (a down stack costs one timeout, not one per service), in order."""
    if not probes:
        return []
    with ThreadPoolExecutor(max_workers=len(probes)) as pool:
        return list(pool.map(lambda p: run_probe(p, timeout), probes))
