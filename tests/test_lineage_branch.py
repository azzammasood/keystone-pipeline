"""`.github/scripts/lineage-branch.sh`: the compacted OpenLineage log on a data branch (ADR-085)."""

import gzip
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from keystone.paths import REPO_ROOT

SCRIPT = REPO_ROOT / ".github" / "scripts" / "lineage-branch.sh"

pytestmark = pytest.mark.skipif(
    sys.platform == "win32" or not shutil.which("bash") or not shutil.which("git"),
    reason="needs bash and git",
)


def _run(src: Path, remote: Path, cap_mb: int) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "LINEAGE_REMOTE": str(remote)}
    return subprocess.run(
        ["bash", str(SCRIPT), str(src), str(cap_mb)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _files(remote: Path) -> list[str]:
    out = subprocess.run(
        ["git", "--git-dir", str(remote), "ls-tree", "-r", "--name-only", "lineage-log"],
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.split()


def test_pushes_once_skips_no_change_and_refuses_over_the_cap(tmp_path: Path) -> None:
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(remote)], check=True)
    src = tmp_path / "lineage"
    (src / "events").mkdir(parents=True)
    (src / "events" / "2026-10-01.jsonl.gz").write_bytes(
        gzip.compress(b'{"eventType":"COMPLETE"}\n')
    )
    (src / "events" / "2026-10-02.jsonl").write_text("{}\n")  # today's open file: not pushed
    (src / "summary.jsonl").write_text('{"job":"keystone/export"}\n')

    first = _run(src, remote, 1)
    assert first.returncode == 0 and "pushed" in first.stdout
    assert _files(remote) == ["events/2026-10-01.jsonl.gz", "summary.jsonl"]
    again = _run(src, remote, 1)
    assert again.returncode == 0 and "no change" in again.stdout

    (src / "events" / "2026-10-03.jsonl.gz").write_bytes(os.urandom(1_200_000))
    over = _run(src, remote, 1)
    assert over.returncode == 1 and "over the 1 MB cap" in over.stdout
    assert "events/2026-10-03.jsonl.gz" not in _files(remote)  # nothing pushed
