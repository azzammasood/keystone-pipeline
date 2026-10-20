"""Polite async HTTP client: per-host limits from policy.yml, robots.txt, retries, verification.

Every request goes through `PoliteClient`, which
- sends the policy user agent,
- holds a per-host concurrency slot for the whole request (including a streamed body),
- spaces request starts per host at `1 / requests_per_second`,
- checks robots.txt once per host (RFC 9309: 4xx means allow all, 5xx means disallow all),
- retries 429/5xx and transport errors with backoff, honouring `Retry-After`,
- streams downloads to `<dest>.part`, hashing MD5 and SHA-256 on the way, verifies the MD5
  against the ETag (GH Archive, GCS) or `x-goog-hash`, then renames atomically.
"""

import asyncio
import base64
import hashlib
import re
import time
import urllib.robotparser
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

from keystone.policy import Policy

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_MD5_HEX = re.compile(r"^[0-9a-f]{32}$")


class FetchError(RuntimeError):
    """A request failed after retries, or a response could not be verified."""


class RobotsDisallowed(FetchError):
    """robots.txt forbids this URL for our user agent."""


class IntegrityError(FetchError):
    """Downloaded bytes do not match the server's MD5 or Content-Length."""


class DiskBudgetExceeded(RuntimeError):
    """Writing this file would exceed the bulk-data disk budget: a hard stop, not a file error."""


class DiskGuard:
    """Tracks bytes on disk held by in-flight and kept downloads against a budget."""

    def __init__(self, budget_bytes: int) -> None:
        self.budget_bytes = budget_bytes
        self.used_bytes = 0

    def reserve(self, n: int) -> None:
        if self.used_bytes + n > self.budget_bytes:
            raise DiskBudgetExceeded(
                f"need {n:,} B, {self.used_bytes:,} of {self.budget_bytes:,} B budget in use"
            )
        self.used_bytes += n

    def release(self, n: int) -> None:
        self.used_bytes = max(0, self.used_bytes - n)


def etag_md5(etag: str | None) -> str | None:
    """The MD5 hex digest an ETag carries, if it is a plain (non-multipart) MD5."""
    if not etag:
        return None
    value = etag.removeprefix("W/").strip('"').lower()
    return value if _MD5_HEX.match(value) else None


def goog_md5(headers: httpx.Headers) -> str | None:
    """MD5 hex digest from GCS `x-goog-hash: md5=<base64>` headers, if present."""
    for item in headers.get_list("x-goog-hash", split_commas=True):
        key, _, value = item.strip().partition("=")
        if key == "md5" and value:
            return base64.b64decode(value).hex()
    return None


def parse_http_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).astimezone(UTC)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class Download:
    """Outcome of a (conditional) GET. `path` is set only for a verified 200 body."""

    url: str
    http_status: int
    etag: str | None
    last_modified: datetime | None
    bytes: int | None
    md5: str | None
    sha256: str | None
    path: Path | None


class _HostGate:
    def __init__(self, max_concurrent: int, requests_per_second: float) -> None:
        self.slots = asyncio.Semaphore(max_concurrent)
        self.interval = 1.0 / requests_per_second
        self.lock = asyncio.Lock()
        self.next_start = 0.0

    async def pace(self) -> None:
        async with self.lock:
            now = time.monotonic()
            wait = self.next_start - now
            self.next_start = max(now, self.next_start) + self.interval
        if wait > 0:
            await asyncio.sleep(wait)


class PoliteClient:
    def __init__(
        self,
        policy: Policy,
        transport: httpx.AsyncBaseTransport | None = None,
        *,
        max_retries: int = 3,
        backoff_seconds: float = 2.0,
        timeout_seconds: float = 300.0,
    ) -> None:
        self.policy = policy
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.requests = 0
        self._gates: dict[str, _HostGate] = {}
        self._robots: dict[str, urllib.robotparser.RobotFileParser | bool] = {}
        self._robots_lock = asyncio.Lock()
        self._http = httpx.AsyncClient(
            # identity: hashes and Content-Length refer to the stored bytes, not a decoded body.
            headers={"User-Agent": policy.user_agent, "Accept-Encoding": "identity"},
            transport=transport,
            timeout=httpx.Timeout(timeout_seconds, connect=30.0),
            follow_redirects=False,
        )

    async def __aenter__(self) -> "PoliteClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self._http.aclose()

    def _gate(self, host: str) -> _HostGate:
        if host not in self._gates:
            limits = self.policy.limits_for(host)
            self._gates[host] = _HostGate(limits.max_concurrent, limits.requests_per_second)
        return self._gates[host]

    async def _allowed(self, url: httpx.URL) -> bool:
        origin = f"{url.scheme}://{url.host}"
        async with self._robots_lock:
            if origin not in self._robots:
                self._robots[origin] = await self._fetch_robots(origin)
        rules = self._robots[origin]
        if isinstance(rules, bool):
            return rules
        return rules.can_fetch(self.policy.user_agent, str(url))

    async def _fetch_robots(self, origin: str) -> urllib.robotparser.RobotFileParser | bool:
        async with self._send("GET", httpx.URL(f"{origin}/robots.txt"), {}) as response:
            if response.status_code >= 500:
                raise FetchError(f"{origin}/robots.txt: HTTP {response.status_code}")
            if response.status_code >= 400:
                return True  # RFC 9309: unavailable robots.txt allows everything
            text = (await response.aread()).decode("utf-8", errors="replace")
        parser = urllib.robotparser.RobotFileParser()
        parser.parse(text.splitlines())
        return parser

    @asynccontextmanager
    async def _send(
        self, method: str, url: httpx.URL, headers: dict[str, str]
    ) -> AsyncIterator[httpx.Response]:
        gate = self._gate(url.host)
        attempt = 0
        while True:
            delay = self.backoff_seconds * 2**attempt
            async with gate.slots:
                await gate.pace()
                self.requests += 1
                try:
                    request = self._http.build_request(method, url, headers=headers)
                    response = await self._http.send(request, stream=True)
                except httpx.TransportError as exc:
                    if attempt >= self.max_retries:
                        raise FetchError(f"{method} {url}: {exc!r}") from exc
                    response = None
                if response is not None:
                    if response.status_code not in RETRY_STATUSES or attempt >= self.max_retries:
                        try:
                            yield response
                        finally:
                            await response.aclose()
                        return
                    delay = _retry_after(response.headers.get("retry-after"), delay)
                    # Slow the whole host, not just this request.
                    gate.next_start = max(gate.next_start, time.monotonic() + delay)
                    await response.aclose()
            attempt += 1
            await asyncio.sleep(delay)

    @asynccontextmanager
    async def request(
        self, method: str, url: str, headers: dict[str, str] | None = None
    ) -> AsyncIterator[httpx.Response]:
        """A polite, robots-checked request; the body streams inside the context."""
        target = httpx.URL(url)
        if not await self._allowed(target):
            raise RobotsDisallowed(f"robots.txt disallows {url}")
        async with self._send(method, target, headers or {}) as response:
            yield response

    async def head(self, url: str) -> httpx.Response:
        async with self.request("HEAD", url) as response:
            return response

    async def get_range(
        self, url: str, start: int, end: int, if_range: str | None = None
    ) -> tuple[int, bytes, httpx.Headers]:
        """Bytes `start..end` (inclusive). With `if_range` (an ETag) a changed file returns 200."""
        headers = {"Range": f"bytes={start}-{end}"}
        if if_range:
            headers["If-Range"] = if_range
        try:
            return await self._get_range(url, start, end, headers)
        except httpx.HTTPError as exc:
            raise FetchError(f"GET {url} (range): {exc!r}") from exc

    async def _get_range(
        self, url: str, start: int, end: int, headers: dict[str, str]
    ) -> tuple[int, bytes, httpx.Headers]:
        async with self.request("GET", url, headers) as response:
            if response.status_code == 200:
                # Server ignored the range or the file changed: read no more than we asked for.
                buf = bytearray()
                async for chunk in response.aiter_bytes():
                    buf.extend(chunk)
                    if len(buf) > end - start:
                        break
                return 200, bytes(buf[: end - start + 1]), response.headers
            body = await response.aread()
            return response.status_code, body, response.headers

    async def download(
        self,
        url: str,
        dest: Path,
        *,
        etag: str | None = None,
        last_modified: datetime | None = None,
        disk: DiskGuard | None = None,
        method: str = "GET",
    ) -> Download:
        """Conditional GET (or HEAD) to `dest`. 304 and non-200 return without a file.

        The body is verified as `<dest>.part` and only then replaces `dest`, so a failed
        re-download never destroys a verified copy. Stream errors become `FetchError`.
        """
        try:
            return await self._download(url, dest, etag, last_modified, disk, method)
        except httpx.HTTPError as exc:
            raise FetchError(f"{method} {url}: {exc!r}") from exc

    async def _download(
        self,
        url: str,
        dest: Path,
        etag: str | None,
        last_modified: datetime | None,
        disk: DiskGuard | None,
        method: str,
    ) -> Download:
        headers: dict[str, str] = {}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT")
        async with self.request(method, url, headers) as response:
            got_etag = response.headers.get("etag")
            got_lm = parse_http_date(response.headers.get("last-modified"))
            length_header = response.headers.get("content-length")
            length = int(length_header) if length_header and length_header.isdigit() else None
            if response.status_code != 200 or method == "HEAD":
                return Download(
                    url, response.status_code, got_etag, got_lm, length, None, None, None
                )
            expected_md5 = etag_md5(got_etag) or goog_md5(response.headers)
            part = dest.with_name(dest.name + ".part")
            size, md5, sha256 = await _stream_to(response, part, disk, length or 0)
        problem = None
        if length is not None and size != length:
            problem = f"got {size:,} B, Content-Length {length:,}"
        elif expected_md5 and md5 != expected_md5:
            problem = f"MD5 {md5} does not match server {expected_md5}"
        if problem:
            part.unlink(missing_ok=True)
            if disk:
                disk.release(size)
            raise IntegrityError(f"{url}: {problem}")
        if disk and dest.exists():
            disk.release(dest.stat().st_size)  # the old copy is replaced
        part.replace(dest)
        return Download(url, 200, got_etag, got_lm, size, md5, sha256, dest)


def _retry_after(value: str | None, default: float) -> float:
    """Seconds from `Retry-After` (delta or HTTP date), capped at 5 minutes."""
    if not value:
        return default
    if value.strip().isdigit():
        return min(float(value), 300.0)
    when = parse_http_date(value)
    if when is None:
        return default
    return min(max((when - datetime.now(UTC)).total_seconds(), 0.0), 300.0)


async def _stream_to(
    response: httpx.Response, part: Path, disk: DiskGuard | None, expected: int
) -> tuple[int, str, str]:
    """Stream raw body bytes to `part`; on return the guard holds exactly `size` bytes."""
    part.parent.mkdir(parents=True, exist_ok=True)
    md5, sha256 = hashlib.md5(usedforsecurity=False), hashlib.sha256()
    size = reserved = 0
    try:
        if disk:
            disk.reserve(expected)
            reserved = expected
        with part.open("wb") as fh:
            async for chunk in response.aiter_raw(1 << 20):
                size += len(chunk)
                if disk and size > reserved:
                    disk.reserve(size - reserved)  # body longer than advertised
                    reserved = size
                fh.write(chunk)
                md5.update(chunk)
                sha256.update(chunk)
    except BaseException:
        part.unlink(missing_ok=True)
        if disk:
            disk.release(reserved)
        raise
    if disk and reserved > size:
        disk.release(reserved - size)
    return size, md5.hexdigest(), sha256.hexdigest()
