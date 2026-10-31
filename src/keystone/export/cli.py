"""`keystone export site`: the site exports in the versioned envelope (Phase 10, ADR-060).

Kept out of cli.py (one `add_typer` line there).
"""

from pathlib import Path

import typer

from keystone import __version__
from keystone.export import site
from keystone.paths import DATA_DIR, METADATA_DIR
from keystone.telemetry.runlog import Stage

export_app = typer.Typer(no_args_is_help=True, help="Site exports in the envelope (Phase 10).")


@export_app.command("site")
def export_site(
    out: Path = typer.Option(site.OUT, help="Output directory."),
    root: Path = typer.Option(DATA_DIR, help="Data directory (telemetry/ holds the run log)."),
    score_dir: Path = typer.Option(site.SCORE_DIR, help="`keystone score` output directory."),
    warehouse: Path = typer.Option(site.WAREHOUSE, help="Warehouse DuckDB (names, packages)."),
    metrics: Path = typer.Option(site.METRICS, help="pipeline_metrics.json."),
    metadata_dir: Path = typer.Option(METADATA_DIR, help="Static metadata export directory."),
) -> None:
    """Radar, project pages, backtests, benchmarks, pipeline_metrics.json and the metadata
    export, each validated by its JSON Schema, plus index.json with checksums."""
    from keystone.lineage import datasets as lds

    with Stage(
        root,
        "export",
        "keystone",
        __version__,
        inputs=lds.site_inputs(),
        outputs=lds.site_outputs(),
    ) as st:
        index = site.build(
            out,
            root=root,
            score_dir=score_dir,
            warehouse=warehouse,
            metrics=metrics,
            metadata_dir=metadata_dir,
        )
        st.records_out = len(index)
        st.bytes_out = sum(r["bytes"] for r in index)
    for r in index:
        typer.echo(f"{r['path']:<34} {r['bytes']:>9} B  schema={r['schema']}")
    typer.echo(f"{len(index)} files validated -> {out}")
