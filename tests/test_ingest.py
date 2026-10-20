"""Phase 2 ingestion with mocked HTTP (httpx.MockTransport): no network."""

import asyncio
import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path

import duckdb
import httpx
import pytest
from typer.testing import CliRunner

from keystone import cli
from keystone.fetch.client import (
    DiskBudgetExceeded,
    DiskGuard,
    FetchError,
    IntegrityError,
    PoliteClient,
    RobotsDisallowed,
)
from keystone.fetch.manifest import Manifest
from keystone.policy import HostLimits, Policy
from keystone.sources import gharchive as gha
from keystone.sources import load_sources, osv

SOURCES = load_sources()
CFG = SOURCES.gharchive
DAY = datetime(2026, 10, 19, tzinfo=UTC)
NOW = DAY + timedelta(days=2)


def policy(concurrent: int = 2, rps: float = 1000.0) -> Policy:
    return Policy(
        user_agent="keystone-test/0",
        default_limits=HostLimits(max_concurrent=concurrent, requests_per_second=rps),
        disk_budget_fraction=0.7,
    )


class FakeHost:
    """A static file host with GCS-like headers and conditional GET."""

    def __init__(self, files: dict[str, bytes], robots: str = "") -> None:
        self.files = dict(files)
        self.robots = robots
        self.last_modified: dict[str, datetime] = {}
        self.calls: list[tuple[str, str]] = []
        self.corrupt: set[str] = set()
        self.fail_first: dict[str, int] = {}
        self.in_flight = 0
        self.max_in_flight = 0
        self.delay = 0.0
        self.on_request: Callable[[str], None] | None = None

    @staticmethod
    def etag(body: bytes) -> str:
        return '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"'

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.lstrip("/")
        self.calls.append((request.method, path))
        if self.on_request:
            self.on_request(path)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            return self._respond(request, path)
        finally:
            self.in_flight -= 1

    def _respond(self, request: httpx.Request, path: str) -> httpx.Response:
        if path == "robots.txt":
            return httpx.Response(200, text=self.robots) if self.robots else httpx.Response(404)
        if self.fail_first.get(path, 0) > 0:
            self.fail_first[path] -= 1
            return httpx.Response(503, headers={"Retry-After": "0"})
        if path not in self.files:
            return httpx.Response(404)
        body = self.files[path]
        lm = self.last_modified.get(path, DAY)
        headers = {"ETag": self.etag(body), "Last-Modified": format_datetime(lm, usegmt=True)}
        if request.headers.get("if-none-match") == headers["ETag"]:
            return httpx.Response(304, headers=headers)
        rng = request.headers.get("range")
        if rng and (
            "if-range" not in request.headers or request.headers["if-range"] == headers["ETag"]
        ):
            first, last = (int(x) for x in rng.removeprefix("bytes=").split("-"))
            part = body[first : last + 1]
            headers["Content-Range"] = f"bytes {first}-{first + len(part) - 1}/{len(body)}"
            return httpx.Response(206, content=part, headers=headers)
        headers["Content-Length"] = str(len(body))
        if request.method == "HEAD":
            return httpx.Response(200, headers=headers)
        if path in self.corrupt:
            body = body[:-1] + bytes([body[-1] ^ 1])
        return httpx.Response(200, stream=httpx.ByteStream(body), headers=headers)


def day_files(size: int = 2_000_000) -> dict[str, bytes]:
    return {
        gha.hour_name(h): f"{h:%Y-%m-%dT%H}".encode() * (size // 13)
        for h in gha.hours(DAY, DAY + timedelta(days=1))
    }


def run_day(host: FakeHost, manifest: Manifest, work: Path, **kwargs: object) -> gha.RunSummary:
    async def go() -> gha.RunSummary:
        async with PoliteClient(policy(), httpx.MockTransport(host), backoff_seconds=0) as c:
            return await gha.ingest_hours(
                c,
                manifest,
                CFG,
                DAY,
                DAY + timedelta(days=1),
                work,
                DiskGuard(10**10),
                now=NOW,
                **kwargs,  # type: ignore[arg-type]
            )

    return asyncio.run(go())


@pytest.fixture
def manifest(tmp_path: Path) -> Manifest:
    m = Manifest(tmp_path / "manifest.duckdb")
    yield m  # type: ignore[misc]
    m.close()


def test_hour_names_and_periods() -> None:
    assert gha.hour_name(datetime(2015, 1, 1, 0, tzinfo=UTC)) == "2015-01-01-0.json.gz"
    assert gha.hour_name(datetime(2015, 1, 1, 15, tzinfo=UTC)) == "2015-01-01-15.json.gz"
    p = CFG.period_for
    assert p(datetime(2025, 5, 23, 17, tzinfo=UTC)) == "C1"
    assert p(datetime(2025, 5, 23, 18, tzinfo=UTC)) == "C2"
    assert p(datetime(2025, 10, 8, 16, tzinfo=UTC)) == "C2"
    assert p(datetime(2025, 10, 8, 17, tzinfo=UTC)) == "GAP"  # measured in the Phase 2 gate
    assert p(datetime(2025, 10, 15, 0, tzinfo=UTC)) == "C3"
    assert p(datetime(2026, 10, 19, tzinfo=UTC)) == "C5"


def test_clean_day_then_rerun_is_unchanged(manifest: Manifest, tmp_path: Path) -> None:
    host = FakeHost(day_files())
    work = tmp_path / "raw"
    seen: list[str] = []
    first = run_day(host, manifest, work, on_file=lambda p, r: seen.append(p.name))
    assert first.counts == {"new": 24}
    assert len(seen) == 24
    assert not any(work.rglob("*.gz")), "stream and delete leaves no files"
    assert gha.find_gaps(manifest, CFG, DAY, DAY + timedelta(days=1), now=NOW) == []

    second = run_day(host, manifest, work, on_file=lambda p, r: seen.append(p.name))
    assert second.counts == {"unchanged": 24}
    assert second.downloaded_bytes == 0
    assert len(seen) == 24, "unchanged files are not handed on again"
    gets = [c for c in host.calls if c[0] == "GET" and c[1].endswith(".gz")]
    assert len(gets) == 48  # 24 downloads + 24 conditional GETs answered 304
    rec = manifest.get(gha.hour_url(CFG, DAY))
    assert rec is not None and rec.checks == 2 and rec.period == "C5"
    assert rec.sha256 == hashlib.sha256(host.files[gha.hour_name(DAY)]).hexdigest()


def test_injected_gap_near_empty_and_unchecked_hours(manifest: Manifest, tmp_path: Path) -> None:
    files = day_files()
    del files[gha.hour_name(DAY + timedelta(hours=5))]  # injected gap
    files[gha.hour_name(DAY + timedelta(hours=9))] = b"x" * 200_000  # like 2025-10-09
    summary = run_day(FakeHost(files), manifest, tmp_path / "raw")
    assert summary.counts == {"new": 22, "missing": 1, "empty": 1}
    manifest.delete(gha.hour_url(CFG, DAY + timedelta(hours=13)))  # never checked
    found = {
        (f.hour.hour, f.kind)
        for f in gha.find_gaps(manifest, CFG, DAY, DAY + timedelta(days=1), now=NOW)
    }
    assert found == {(5, "missing"), (9, "near_empty"), (13, "unchecked")}


def test_missing_hour_that_appears_later_is_new_and_late(
    manifest: Manifest, tmp_path: Path
) -> None:
    files = day_files()
    name = gha.hour_name(DAY + timedelta(hours=3))
    body = files.pop(name)
    host = FakeHost(files)
    run_day(host, manifest, tmp_path / "raw")
    host.files[name] = body
    host.last_modified[name] = DAY + timedelta(days=3)  # far past the 6 h grace
    summary = run_day(host, manifest, tmp_path / "raw")
    assert summary.counts == {"new": 1, "unchanged": 23}
    kinds = {f.kind for f in gha.find_gaps(manifest, CFG, DAY, DAY + timedelta(days=1), now=NOW)}
    assert kinds == {"late"}


def test_rewritten_file_is_changed(manifest: Manifest, tmp_path: Path) -> None:
    host = FakeHost(day_files())
    run_day(host, manifest, tmp_path / "raw")
    name = gha.hour_name(DAY + timedelta(hours=7))
    host.files[name] = host.files[name] + b"more"
    summary = run_day(host, manifest, tmp_path / "raw")
    assert summary.counts == {"changed": 1, "unchanged": 23}
    kinds = [f.kind for f in gha.find_gaps(manifest, CFG, DAY, DAY + timedelta(days=1), now=NOW)]
    assert kinds == ["changed"]


def test_md5_mismatch_is_error_and_leaves_no_file(manifest: Manifest, tmp_path: Path) -> None:
    host = FakeHost(day_files())
    host.corrupt.add(gha.hour_name(DAY))
    summary = run_day(host, manifest, tmp_path / "raw", keep=True)
    assert summary.counts == {"new": 23, "error": 1}
    rec = manifest.get(gha.hour_url(CFG, DAY))
    assert rec is not None and rec.error is not None and "MD5" in rec.error
    assert not (tmp_path / "raw" / "2026" / "10" / gha.hour_name(DAY)).exists()
    assert not list((tmp_path / "raw").rglob("*.part"))
    host.corrupt.clear()
    assert run_day(host, manifest, tmp_path / "raw", keep=True).counts == {
        "new": 1,
        "unchanged": 23,
    }


def test_unpublished_hours_are_skipped(manifest: Manifest, tmp_path: Path) -> None:
    host = FakeHost(day_files())

    async def go() -> gha.RunSummary:
        async with PoliteClient(policy(), httpx.MockTransport(host)) as c:
            return await gha.ingest_hours(
                c,
                manifest,
                CFG,
                DAY,
                DAY + timedelta(days=1),
                tmp_path,
                DiskGuard(10**10),
                now=DAY + timedelta(hours=12, minutes=30),
            )

    summary = asyncio.run(go())
    assert summary.counts == {"new": 11}  # 00h..10h; 11h+ are within the publish lag
    assert summary.skipped_unpublished == 13


def test_head_only_records_sizes_without_download(manifest: Manifest, tmp_path: Path) -> None:
    host = FakeHost(day_files())
    summary = run_day(host, manifest, tmp_path / "raw", head_only=True)
    assert summary.counts == {"new": 24} and summary.downloaded_bytes == 0
    assert all(m == "HEAD" for m, p in host.calls if p.endswith(".gz"))
    assert run_day(host, manifest, tmp_path / "raw", head_only=True).counts == {"unchanged": 24}
    # The first verified copy is `new`; the next run is conditional.
    assert run_day(host, manifest, tmp_path / "raw").counts == {"new": 24}
    assert run_day(host, manifest, tmp_path / "raw").counts == {"unchanged": 24}
    rec = manifest.get(gha.hour_url(CFG, DAY))
    assert rec is not None and rec.sha256 is not None


def test_concurrency_and_rate_limits(tmp_path: Path) -> None:
    host = FakeHost({f"f{i}": b"x" for i in range(8)})
    host.delay = 0.02

    async def go(pol: Policy) -> float:
        async with PoliteClient(pol, httpx.MockTransport(host)) as c:
            started = asyncio.get_running_loop().time()
            await asyncio.gather(
                *(c.download(f"https://h.test/f{i}", tmp_path / f"f{i}") for i in range(8))
            )
            return asyncio.get_running_loop().time() - started

    asyncio.run(go(policy(concurrent=2)))
    assert host.max_in_flight == 2
    host.calls.clear()
    elapsed = asyncio.run(go(policy(concurrent=8, rps=20.0)))
    assert elapsed >= 7 / 20 * 0.9  # 8 starts spaced 50 ms apart


def test_robots_disallow_and_retry(tmp_path: Path) -> None:
    host = FakeHost({"ok": b"data", "private/x": b"no"}, robots="User-agent: *\nDisallow: /private")
    host.fail_first["ok"] = 2

    async def go() -> None:
        async with PoliteClient(policy(), httpx.MockTransport(host), backoff_seconds=0) as c:
            got = await c.download("https://h.test/ok", tmp_path / "ok")
            assert got.http_status == 200 and got.md5 == hashlib.md5(b"data").hexdigest()
            with pytest.raises(RobotsDisallowed):
                await c.download("https://h.test/private/x", tmp_path / "x")

    asyncio.run(go())
    assert [p for m, p in host.calls].count("robots.txt") == 1
    assert [p for m, p in host.calls].count("ok") == 3


def test_disk_budget_and_integrity(tmp_path: Path) -> None:
    host = FakeHost({"big": b"x" * 1000})

    async def go() -> None:
        async with PoliteClient(policy(), httpx.MockTransport(host)) as c:
            guard = DiskGuard(999)
            with pytest.raises(DiskBudgetExceeded):
                await c.download("https://h.test/big", tmp_path / "big", disk=guard)
            assert guard.used_bytes == 0 and not list(tmp_path.iterdir())
            host.corrupt.add("big")
            with pytest.raises(IntegrityError):
                await c.download("https://h.test/big", tmp_path / "big", disk=DiskGuard(10**6))

    asyncio.run(go())


# --- OSV ------------------------------------------------------------------------------------


def csv_lines(start: datetime, n: int, eco: str = "npm") -> list[str]:
    """Newest first, like modified_id.csv."""
    return [
        f"{(start - timedelta(seconds=i)):%Y-%m-%dT%H:%M:%S}.{i:07d}Z,{eco}/MAL-{n - i}"
        for i in range(n)
    ]


def osv_cfg(chunk: int = 2048, first: int = 4096) -> object:
    return SOURCES.osv.model_copy(
        update={
            "base_url": "https://osv.test/",
            "head_chunk_bytes": chunk,
            "first_run_max_bytes": first,
        }
    )


def run_osv(host: FakeHost, manifest: Manifest, out: Path, cfg: object) -> osv.IncrementResult:
    async def go() -> osv.IncrementResult:
        async with PoliteClient(policy(), httpx.MockTransport(host)) as c:
            return await osv.snapshot_increment(c, manifest, cfg, out)  # type: ignore[arg-type]

    return asyncio.run(go())


def test_osv_increment_reads_only_the_head(manifest: Manifest, tmp_path: Path) -> None:
    t0 = datetime(2026, 10, 20, 6, tzinfo=UTC)
    old = csv_lines(t0, 2000)
    host = FakeHost({"modified_id.csv": ("\n".join(old) + "\n").encode()})
    cfg = osv_cfg()
    first = run_osv(host, manifest, tmp_path, cfg)
    assert first.bytes_read <= 4096 + 2048 and first.new_rows > 0
    assert first.cursor == osv.normalize_ts(old[0].split(",")[0])

    new = csv_lines(t0 + timedelta(hours=1), 5, eco="PyPI")
    host.files["modified_id.csv"] = ("\n".join(new + ["garbage line", *old]) + "\n").encode()
    second = run_osv(host, manifest, tmp_path, cfg)
    assert second.new_rows == 5 and second.quarantined == 1 and second.chunks == 1
    assert second.output is not None
    rows = duckdb.sql(
        f"SELECT ecosystem, source_sha256, modified FROM '{second.output}'"
    ).fetchall()
    assert {r[0] for r in rows} == {"PyPI"} and all(len(r[1]) == 64 for r in rows)
    q = next((tmp_path / "quarantine").glob("*.jsonl")).read_text(encoding="utf-8")
    assert json.loads(q.splitlines()[0])["reason"] == "no comma"

    third = run_osv(host, manifest, tmp_path, cfg)
    assert third.new_rows == 0 and third.output is None


def test_osv_restarts_when_file_changes_mid_read(manifest: Manifest, tmp_path: Path) -> None:
    t0 = datetime(2026, 10, 20, 6, tzinfo=UTC)
    host = FakeHost({"modified_id.csv": ("\n".join(csv_lines(t0, 2000)) + "\n").encode()})
    cfg = osv_cfg(chunk=1024, first=8192)
    gets = {"n": 0}

    def regenerate(path: str) -> None:
        if path == "modified_id.csv":
            gets["n"] += 1
            if gets["n"] == 2:  # between chunk 1 and 2
                host.files[path] = ("\n".join(csv_lines(t0 + timedelta(1), 2000)) + "\n").encode()

    host.on_request = regenerate
    result = run_osv(host, manifest, tmp_path, cfg)
    assert result.restarts == 1
    assert result.cursor is not None and result.cursor.startswith("2026-10-21")


def test_osv_zips_are_conditional(manifest: Manifest, tmp_path: Path) -> None:
    host = FakeHost({"PyPI/all.zip": b"PK" + b"z" * 5000, "crates.io/all.zip": b"PK" + b"c" * 9})
    cfg = osv_cfg()

    def go() -> list[osv.ZipResult]:
        async def inner() -> list[osv.ZipResult]:
            async with PoliteClient(policy(), httpx.MockTransport(host)) as c:
                return await osv.fetch_zips(
                    c,
                    manifest,
                    cfg,
                    ["PyPI", "crates.io", "Nope"],
                    tmp_path,  # type: ignore[arg-type]
                    DiskGuard(10**6),
                )

        return asyncio.run(inner())

    assert [z.status for z in go()] == ["new", "new", "missing"]
    assert [z.status for z in go()] == ["unchanged", "unchanged", "missing"]
    assert (tmp_path / "zips" / "PyPI" / "all.zip").stat().st_size == 5002


def test_cli_gaps_on_empty_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "MANIFEST_PATH", tmp_path / "m.duckdb")
    result = CliRunner().invoke(
        cli.app, ["ingest", "gaps", "--start", "2015-01-01T00", "--end", "2015-01-01T03", "--json"]
    )
    assert result.exit_code == 0, result.output
    rows = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    assert [(r["kind"], r["period"]) for r in rows] == [("unchecked", "C1")] * 3


def test_disk_budget_is_a_hard_stop(manifest: Manifest, tmp_path: Path) -> None:
    host = FakeHost(day_files())

    async def go() -> None:
        async with PoliteClient(policy(), httpx.MockTransport(host)) as c:
            await gha.ingest_hours(
                c,
                manifest,
                CFG,
                DAY,
                DAY + timedelta(days=1),
                tmp_path,
                DiskGuard(10),
                now=NOW,
            )

    with pytest.raises(DiskBudgetExceeded):
        asyncio.run(go())
    assert len([p for m, p in host.calls if p.endswith(".gz")]) <= 2  # one per worker, then stop


def test_stream_error_is_an_hour_error_not_a_crash(manifest: Manifest, tmp_path: Path) -> None:
    host = FakeHost(day_files())
    bad = gha.hour_name(DAY + timedelta(hours=2))

    class Broken(httpx.AsyncByteStream):
        async def __aiter__(self):  # type: ignore[no-untyped-def]
            yield b"partial"
            raise httpx.ReadError("connection reset")

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(bad):
            return httpx.Response(200, stream=Broken(), headers={"Content-Length": "99"})
        return await host(request)

    async def go() -> gha.RunSummary:
        async with PoliteClient(policy(), httpx.MockTransport(handler)) as c:
            return await gha.ingest_hours(
                c,
                manifest,
                CFG,
                DAY,
                DAY + timedelta(days=1),
                tmp_path,
                DiskGuard(10**10),
                now=NOW,
            )

    summary = asyncio.run(go())
    assert summary.counts == {"new": 23, "error": 1}
    assert not list(tmp_path.rglob("*.part"))


def test_failed_redownload_keeps_the_verified_copy(tmp_path: Path) -> None:
    host = FakeHost({"z": b"good" * 100})
    dest = tmp_path / "z"

    async def go() -> None:
        async with PoliteClient(policy(), httpx.MockTransport(host)) as c:
            await c.download("https://h.test/z", dest)
            host.files["z"] = b"newer" * 100
            host.corrupt.add("z")
            with pytest.raises(FetchError):
                await c.download("https://h.test/z", dest)

    asyncio.run(go())
    assert dest.read_bytes() == b"good" * 100


def test_on_file_failure_is_recorded_and_retried(manifest: Manifest, tmp_path: Path) -> None:
    host = FakeHost(day_files())

    def explode(path: Path, rec: object) -> None:
        if path.name == gha.hour_name(DAY):
            raise ValueError("parser broke")

    assert run_day(host, manifest, tmp_path, on_file=explode).counts == {"new": 23, "error": 1}
    assert run_day(host, manifest, tmp_path).counts == {"new": 1, "unchanged": 23}
