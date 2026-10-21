"""OpenLineage events from the CLI (openlineage-python, spec 2-0-2).

Every event goes to `<root>/lineage/events/YYYY-MM-DD.jsonl` (UTC day of the event), the record
the static export and DataHub are built from. When `OPENLINEAGE_URL` is set (Marquez in the
`lineage` profile), the same event is also posted to its lineage endpoint. A
failure to reach Marquez is reported, never fatal: lineage must not stop the pipeline.
"""

import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

import orjson
from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import (
    column_lineage_dataset,
    job_type_job,
    parent_run,
    processing_engine_run,
    schema_dataset,
    symlinks_dataset,
)
from openlineage.client.serde import Serde
from openlineage.client.uuid import generate_new_uuid

from keystone import __version__
from keystone.lineage.naming import JOB_NS

PRODUCER = f"https://github.com/azzammasood/keystone-pipeline/tree/v{__version__}"
PIPELINE_JOB = "pipeline"


@dataclass(frozen=True)
class Ds:
    """A dataset in an event. `columns`: (name, type). `lineage`: output column -> input
    (namespace, name, column) triples; `identity` marks a copy without transformation."""

    namespace: str
    name: str
    columns: Sequence[tuple[str, str]] = ()
    lineage: dict[str, list[tuple[str, str, str]]] = field(default_factory=dict)
    identity: bool = False
    symlinks: Sequence[tuple[str, str, str]] = ()  # (namespace, name, type)

    def facets(self) -> dict[str, object]:
        out: dict[str, object] = {}
        if self.columns:
            out["schema"] = schema_dataset.SchemaDatasetFacet(
                fields=[
                    schema_dataset.SchemaDatasetFacetFields(name=n, type=t) for n, t in self.columns
                ]
            )
        if self.lineage:
            kind = "IDENTITY" if self.identity else None
            out["columnLineage"] = column_lineage_dataset.ColumnLineageDatasetFacet(
                fields={
                    col: column_lineage_dataset.Fields(
                        inputFields=[
                            column_lineage_dataset.InputField(namespace=ns, name=nm, field=f)
                            for ns, nm, f in src
                        ],
                        transformationType=kind,
                    )
                    for col, src in self.lineage.items()
                }
            )
        if self.symlinks:
            out["symlinks"] = symlinks_dataset.SymlinksDatasetFacet(
                identifiers=[
                    symlinks_dataset.Identifier(namespace=ns, name=nm, type=t)
                    for ns, nm, t in self.symlinks
                ]
            )
        return out


def events_dir(root: Path) -> Path:
    return root / "lineage" / "events"


def pipeline_run_id() -> str:
    """The pipeline run this process belongs to: `KEYSTONE_RUN_ID` (set by an orchestrator,
    a UUID) or one id per process."""
    return os.environ.get("KEYSTONE_RUN_ID") or _process_run_id()


@cache
def _process_run_id() -> str:
    return str(generate_new_uuid())


def write_events(root: Path, events: Sequence[dict[str, object]], forward: bool = True) -> int:
    """Append serialised events to the day files; forward them to Marquez when configured.
    Returns how many were forwarded."""
    out = events_dir(root)
    out.mkdir(parents=True, exist_ok=True)
    for ev in events:
        day = str(ev["eventTime"])[:10]
        with (out / f"{day}.jsonl").open("ab") as fh:
            fh.write(orjson.dumps(ev) + b"\n")
    url = os.environ.get("OPENLINEAGE_URL")
    if not (forward and url):
        return 0
    import httpx

    sent = 0
    with httpx.Client(timeout=10) as client:
        for ev in events:
            try:
                client.post(f"{url.rstrip('/')}/api/v1/lineage", json=ev).raise_for_status()
                sent += 1
            except httpx.HTTPError as exc:
                print(f"lineage: not sent to {url}: {exc}", file=sys.stderr)
                break
    return sent


class LineageRun:
    """START at creation, COMPLETE or FAIL at `finish`, for one CLI job run."""

    def __init__(
        self,
        job: str,
        inputs: Sequence[Ds],
        outputs: Sequence[Ds],
        root: Path,
        engine: tuple[str, str] = ("keystone", __version__),
        processing: str = "BATCH",
    ) -> None:
        self.job, self.root, self.engine, self.processing = job, root, engine, processing
        self.inputs, self.outputs = list(inputs), list(outputs)
        self.run_id = str(generate_new_uuid())
        self._emit(RunState.START)

    def finish(self, ok: bool) -> None:
        self._emit(RunState.COMPLETE if ok else RunState.FAIL)

    def _emit(self, state: RunState) -> None:
        parent = pipeline_run_id()
        run_facets: dict[str, object] = {
            "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                version=self.engine[1], name=self.engine[0], openlineageAdapterVersion=None
            ),
        }
        if parent != self.run_id:
            run_facets["parent"] = parent_run.ParentRunFacet(
                run=parent_run.Run(runId=parent),
                job=parent_run.Job(namespace=JOB_NS, name=PIPELINE_JOB),
            )
        event = RunEvent(
            eventType=state,
            eventTime=datetime.now(UTC).isoformat(),
            run=Run(runId=self.run_id, facets=run_facets),  # type: ignore[arg-type]
            job=Job(
                namespace=JOB_NS,
                name=self.job,
                facets={
                    "jobType": job_type_job.JobTypeJobFacet(
                        processingType=self.processing, integration="KEYSTONE", jobType="JOB"
                    )
                },
            ),
            producer=PRODUCER,
            inputs=[
                InputDataset(namespace=d.namespace, name=d.name, facets=d.facets())  # type: ignore[arg-type]
                for d in self.inputs
            ],
            outputs=[
                OutputDataset(namespace=d.namespace, name=d.name, facets=d.facets())  # type: ignore[arg-type]
                for d in self.outputs
            ],
        )
        write_events(self.root, [orjson.loads(Serde.to_json(event))])
