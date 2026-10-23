"""One measured record per stage run, the feed of `pipeline_metrics.json` (brief section 9c).

`Stage` wraps the work of one CLI command: it measures wall-clock seconds, takes the records
and bytes the command counted, appends a line to `<root>/telemetry/runs.jsonl`, emits the
OpenLineage START/COMPLETE (or FAIL) pair when given datasets, and, when
`OTEL_EXPORTER_OTLP_ENDPOINT` is set (the `obs` profile), records the same numbers as
OpenTelemetry gauges. Only counted numbers are written; a stage that cannot count something
leaves it null. Rate = records out / the stage's wall-clock seconds.
"""

import atexit
import os
import time
from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from types import TracebackType
from typing import Any

import orjson

from keystone.lineage.emit import Ds, LineageRun, pipeline_run_id


def runs_path(root: Path) -> Path:
    return root / "telemetry" / "runs.jsonl"


@dataclass
class StageRecord:
    run_id: str
    stage: str
    service: str
    version: str
    started_at: str
    ended_at: str = ""
    seconds: float = 0.0
    records_in: int | None = None
    records_out: int | None = None
    bytes_in: int | None = None
    bytes_out: int | None = None
    status: str = "running"
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def records_per_second(self) -> float | None:
        if self.records_out is None or self.seconds <= 0:
            return None
        return self.records_out / self.seconds

    @property
    def mb_per_second(self) -> float | None:
        moved = self.bytes_in if self.bytes_in is not None else self.bytes_out
        if moved is None or self.seconds <= 0:
            return None
        return moved / 1e6 / self.seconds


class Stage:
    """`with Stage(root, "rollup", "duckdb", version) as st: st.records_out = n`."""

    def __init__(
        self,
        root: Path,
        stage: str,
        service: str,
        version: str,
        inputs: Sequence[Ds] = (),
        outputs: Sequence[Ds] = (),
        job: str | None = None,
    ) -> None:
        self.root = root
        self.rec = StageRecord(
            run_id=pipeline_run_id(),
            stage=stage,
            service=service,
            version=version,
            started_at=datetime.now(UTC).isoformat(),
        )
        self.inputs, self.outputs = list(inputs), list(outputs)
        self.job = job or stage
        dag = os.environ.get("AIRFLOW_CTX_DAG_ID")  # set by Airflow for a task's command
        if dag:  # the stage ran under Airflow (ADR-063): evidence for engines[]
            ver = os.environ.get("AIRFLOW_VERSION", "unknown")
            self.rec.detail["services"] = {**self.rec.detail.get("services", {}), "airflow": ver}
            self.rec.detail["orchestrator"] = {
                "dag": dag,
                "task": os.environ.get("AIRFLOW_CTX_TASK_ID", ""),
            }
        self._t0 = 0.0
        self._lineage: LineageRun | None = None

    # The command fills these in as it counts.
    @property
    def detail(self) -> dict[str, Any]:
        return self.rec.detail

    def __setattr__(self, name: str, value: Any) -> None:
        if name in ("records_in", "records_out", "bytes_in", "bytes_out"):
            setattr(self.rec, name, value)
        else:
            super().__setattr__(name, value)

    def __enter__(self) -> "Stage":
        self._t0 = time.perf_counter()
        if self.inputs or self.outputs:
            self._lineage = LineageRun(
                self.job,
                self.inputs,
                self.outputs,
                self.root,
                engine=(self.rec.service, self.rec.version),
            )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        ok = exc_type is None or (exc_type is SystemExit and getattr(exc, "code", 1) in (0, None))
        # typer.Exit(1) after a counted failure is also a failed stage.
        if exc_type is not None and exc_type.__name__ == "Exit":
            ok = getattr(exc, "exit_code", 1) == 0
        self.rec.seconds = round(time.perf_counter() - self._t0, 6)
        self.rec.ended_at = datetime.now(UTC).isoformat()
        self.rec.status = "ok" if ok else "failed"
        if self._lineage is None and (self.inputs or self.outputs):
            # Datasets known only after the work (what a publish copied): START now.
            self._lineage = LineageRun(
                self.job, (), (), self.root, engine=(self.rec.service, self.rec.version)
            )
        if self._lineage is not None:
            # The COMPLETE event carries the datasets as finally known (the hours a run read).
            self._lineage.inputs, self._lineage.outputs = self.inputs, self.outputs
            self._lineage.finish(ok)
        append(self.root, self.rec)
        record_otel(self.rec)


def append(root: Path, rec: StageRecord) -> None:
    path = runs_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as fh:
        fh.write(orjson.dumps(asdict(rec)) + b"\n")


def read(root: Path) -> list[StageRecord]:
    path = runs_path(root)
    if not path.exists():
        return []
    out = []
    for line in path.read_bytes().splitlines():
        if line.strip():
            out.append(StageRecord(**orjson.loads(line)))
    return out


def iter_runs(records: Sequence[StageRecord]) -> Iterator[tuple[str, list[StageRecord]]]:
    """Records grouped by pipeline run, in order of each run's first record."""
    groups: dict[str, list[StageRecord]] = {}
    for r in records:
        groups.setdefault(r.run_id, []).append(r)
    yield from groups.items()


# --- OpenTelemetry -----------------------------------------------------------------------
# Gauges, one series per (stage, service): the latest value of a stage is what Grafana shows
# and what `keystone metrics check-grafana` compares with pipeline_metrics.json.
STAGE_GAUGES = {
    "records_in": "keystone_stage_records_in",
    "records_out": "keystone_stage_records_out",
    "bytes_in": "keystone_stage_bytes_in",
    "bytes_out": "keystone_stage_bytes_out",
    "seconds": "keystone_stage_duration_seconds",
    "records_per_second": "keystone_stage_records_per_second",
}


@cache
def _meter() -> Any:
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return None
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource

    from keystone import __version__

    reader = PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=60_000)
    provider = MeterProvider(
        # A fixed instance id: every CLI process writes the same series, so a gauge holds the
        # latest run's value (the SDK would otherwise give each process a new series).
        resource=Resource.create(
            {
                "service.name": "keystone",
                "service.version": __version__,
                "service.instance.id": "keystone-cli",
            }
        ),
        metric_readers=[reader],
    )
    atexit.register(provider.shutdown)  # exports what was recorded before the process ends
    return provider.get_meter("keystone.telemetry")


@cache
def _gauge(name: str) -> Any:
    return _meter().create_gauge(name)


def record_otel(rec: StageRecord) -> None:
    if _meter() is None:
        return
    attrs = {"stage": rec.stage, "service": rec.service, "status": rec.status}
    values = {
        "records_in": rec.records_in,
        "records_out": rec.records_out,
        "bytes_in": rec.bytes_in,
        "bytes_out": rec.bytes_out,
        "seconds": rec.seconds,
        "records_per_second": rec.records_per_second,
    }
    for key, metric in STAGE_GAUGES.items():
        v = values[key]
        if v is not None:
            _gauge(metric).set(v, {k: a for k, a in attrs.items() if k != "status"})


def record_gauges(values: dict[str, float], attrs: dict[str, str] | None = None) -> None:
    """Pipeline-level gauges (`keystone metrics emit`)."""
    if _meter() is None:
        return
    for name, v in values.items():
        _gauge(name).set(v, attrs or {})
