"""Run a job in `full` when every `full` probe passes; otherwise, or when the `full` job fails,
run it in `free`. The outcome is one `profile` stage in the run log, whose `detail` carries
`produced_by` (read by `pipeline_metrics.json` and the export envelope).

The `free` job is the production path and its failure is not caught: there is nothing further
to fall back to, so it fails the run.
"""

from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal

from keystone import __version__
from keystone.profile.probe import FULL_PROBES, Probe, ProbeResult, check
from keystone.telemetry.runlog import Stage

ProfileName = Literal["free", "full"]
Job = Callable[[], None]


@dataclass
class Outcome:
    produced_by: ProfileName | None  # None: probes only, no job ran
    full_healthy: bool
    reason: str
    probes: list[ProbeResult] = field(default_factory=list)
    namespace: str | None = None  # the full rollup's Iceberg namespace (warehouse input, ADR-085)

    @property
    def fallback(self) -> bool:
        return self.produced_by == "free" and self.reason != "requested"

    def as_dict(self) -> dict[str, object]:
        return {
            "produced_by": self.produced_by,
            "full_healthy": self.full_healthy,
            "fallback": self.fallback,
            "reason": self.reason,
            "probes": [asdict(p) for p in self.probes],
            "namespace": self.namespace if self.produced_by == "full" else None,
        }


def _unhealthy(results: Sequence[ProbeResult]) -> str:
    down = [r.name for r in results if not r.ok]
    return f"full unhealthy: {', '.join(down)} down" if down else ""


def check_only(root: Path, probes: Sequence[Probe] = FULL_PROBES) -> Outcome:
    with Stage(root, "profile", "keystone", __version__) as st:
        results = check(probes)
        out = Outcome(None, not _unhealthy(results), _unhealthy(results) or "full healthy", results)
        _record(st, out)
    return out


def run(
    root: Path,
    full_job: Job,
    free_job: Job,
    want: ProfileName = "full",
    probes: Sequence[Probe] = FULL_PROBES,
    namespace: str | None = None,
) -> Outcome:
    """Run the job once, in the best profile available, and record which one produced it."""
    with Stage(root, "profile", "keystone", __version__) as st:
        results = check(probes) if want == "full" else []
        down = _unhealthy(results)
        out = Outcome("free", want == "full" and not down, "requested", results, namespace)
        if want == "full" and down:
            out.reason = down
        elif want == "full":
            try:
                full_job()
                out.produced_by, out.reason = "full", "full healthy"
            except (Exception, SystemExit) as exc:  # typer.Exit and SystemExit included
                out.reason = f"full job failed: {type(exc).__name__}: {exc}"[:300]
        if out.produced_by == "free":
            # Recorded before the job, so a failed free run still shows what was attempted.
            _record(st, out)
            free_job()
        _record(st, out)
    return out


def _record(st: Stage, out: Outcome) -> None:
    st.records_in = len(out.probes)
    st.records_out = sum(p.ok for p in out.probes)
    st.detail.update(out.as_dict())
