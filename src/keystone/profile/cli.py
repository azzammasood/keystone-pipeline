"""`keystone profile ...`: probe the `full` stack and run the rollup with fallback to `free`.

Kept out of cli.py (one `add_typer` line there). The jobs call the existing rollup commands, so
the work, its stages and its run-log lines are the same as when they run by hand.
"""

import json as jsonlib
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import typer

from keystone.paths import DATA_DIR
from keystone.profile import run as prun

profile_app = typer.Typer(
    no_args_is_help=True, help="Profiles: full health check, fallback to free (Phase 10)."
)


def _rollup_jobs(root: Path, namespace: str, topic: str) -> tuple[prun.Job, prun.Job]:
    from keystone import cli

    def full() -> None:
        # The command declares `int` with a None default (Typer); None = event-time order.
        cli.stream_produce(root=root, topic=topic, fresh=True, shuffle_seed=cast(int, None))
        cli.rollup_flink(namespace=namespace, topic=topic, mode="batch", unbounded=False, root=root)

    def free() -> None:
        cli.rollup_run(root=root, json=False)

    return full, free


@profile_app.command("check")
def profile_check(
    run: bool = typer.Option(False, "--run", help="Run the rollup in the profile chosen."),
    want: str = typer.Option("full", help="Profile to try first: full or free."),
    root: Path = typer.Option(DATA_DIR, help="Data directory (silver/, rollup/, telemetry/)."),
    namespace: str = typer.Option(
        None, help="Iceberg namespace for a full run (default full_<UTC minute>)."
    ),
    topic: str = typer.Option("keystone.gharchive.events.v1", help="Kafka topic (full)."),
    json: bool = typer.Option(False, "--json", help="Print JSON."),
) -> None:
    """Probe Redpanda, Flink, Lakekeeper and RustFS; with --run, run the rollup in `full` when
    all are healthy, else (or when the full job fails) in `free`, and record `produced_by`."""
    if want not in ("free", "full"):
        raise typer.BadParameter("want is free or full")
    if run:
        ns = namespace or f"full_{datetime.now(UTC):%Y%m%d%H%M}"
        full, free = _rollup_jobs(root, ns, topic)
        out = prun.run(root, full, free, want="full" if want == "full" else "free", namespace=ns)
    else:
        out = prun.check_only(root)
    if json:
        typer.echo(jsonlib.dumps(out.as_dict(), indent=2))
        return
    for p in out.probes:
        typer.echo(f"{'ok  ' if p.ok else 'DOWN'} {p.name:18} {p.target:36} {p.detail}")
    typer.echo(f"produced_by={out.produced_by} fallback={out.fallback} reason={out.reason}")
