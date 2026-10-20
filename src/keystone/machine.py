"""Measure the local machine (disk, memory, CPUs, Docker) for the resource rule."""

import shutil
import subprocess
from pathlib import Path

import psutil
from pydantic import BaseModel


class MachineReport(BaseModel):
    disk_path: str
    disk_free_gb: float
    disk_total_gb: float
    disk_budget_gb: float
    ram_total_gb: float
    ram_available_gb: float
    logical_cpus: int
    docker: str | None


def _docker_version() -> str | None:
    exe = shutil.which("docker")
    if exe is None:
        return None
    try:
        out = subprocess.run(
            [exe, "--version"], capture_output=True, text=True, timeout=20, check=True
        )
    except (subprocess.SubprocessError, OSError):
        return None
    return out.stdout.strip() or None


def measure(path: Path, budget_fraction: float) -> MachineReport:
    disk = shutil.disk_usage(path)
    mem = psutil.virtual_memory()
    gb = 1e9
    return MachineReport(
        disk_path=str(path),
        disk_free_gb=round(disk.free / gb, 1),
        disk_total_gb=round(disk.total / gb, 1),
        disk_budget_gb=round(disk.free * budget_fraction / gb, 1),
        ram_total_gb=round(mem.total / gb, 1),
        ram_available_gb=round(mem.available / gb, 1),
        logical_cpus=psutil.cpu_count() or 0,
        docker=_docker_version(),
    )
