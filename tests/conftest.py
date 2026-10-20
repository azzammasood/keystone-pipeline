"""Shared helpers: parse the scrubbed GH Archive fixtures into a temporary data directory."""

import gzip
import hashlib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from keystone.fetch.manifest import FileRecord
from keystone.parsers.drift import load_registry
from keystone.parsers.parquet import GhArchiveSink
from keystone.parsers.privacy import SALT_ENV, ActorHasher
from keystone.paths import REPO_ROOT
from keystone.sources import load_sources

FIXTURES = REPO_ROOT / "tests" / "fixtures" / "gharchive"
TEST_SALT = "test-salt-not-a-secret-0123456789"
FETCHED = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


def fixture_hours() -> list[str]:
    return sorted(p.name.removesuffix(".json.gz") for p in FIXTURES.glob("*.json.gz"))


def hour_of(name: str) -> datetime:
    day, h = name.rsplit("-", 1)
    return datetime.fromisoformat(day).replace(hour=int(h), tzinfo=UTC)


def record_for(path: Path, name: str) -> FileRecord:
    return FileRecord(
        url=f"https://data.gharchive.org/{name}.json.gz",
        source="gharchive",
        fetched_at=FETCHED,
        status="new",
        hour=hour_of(name),
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
    )


def write_gz(path: Path, lines: list[bytes]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.GzipFile(path, "wb", mtime=0) as fh:
        fh.write(b"\n".join(lines) + b"\n")
    return path


@dataclass
class DataDir:
    root: Path
    sink: GhArchiveSink

    def parse(self, path: Path, name: str | None = None) -> None:
        name = name or path.name.removesuffix(".json.gz")
        self.sink(path, record_for(path, name))

    @property
    def events(self) -> Path:
        return self.root / "silver" / "gharchive" / "events"


@pytest.fixture(autouse=True)
def _salt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(SALT_ENV, TEST_SALT)


@pytest.fixture
def data_dir(tmp_path: Path) -> Iterator[DataDir]:
    root = tmp_path / "data"
    sink = GhArchiveSink(
        load_sources().gharchive,
        root / "silver" / "gharchive",
        root / "quarantine" / "gharchive",
        root / "quality" / "drift",
        ActorHasher(TEST_SALT.encode()),
        load_registry(),
    )
    yield DataDir(root, sink)


@pytest.fixture
def parsed_fixtures(data_dir: DataDir) -> DataDir:
    for name in fixture_hours():
        data_dir.parse(FIXTURES / f"{name}.json.gz")
    return data_dir


ParseFn = Callable[[Path, str | None], None]
