"""Keystone CLI. Orchestrators (GitHub Actions, Airflow) only call these commands."""

import typer

from keystone import __version__
from keystone.machine import measure
from keystone.paths import REPO_ROOT
from keystone.policy import load_policy

app = typer.Typer(no_args_is_help=True, add_completion=False)


def _version(value: bool) -> None:
    if value:
        typer.echo(f"keystone {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(False, "--version", callback=_version, is_eager=True),
) -> None:
    """Open-source supply-chain risk radar built on GH Archive."""


@app.command()
def info() -> None:
    """Show version, repo root and client policy."""
    policy = load_policy()
    typer.echo(f"keystone {__version__}")
    typer.echo(f"root: {REPO_ROOT}")
    typer.echo(f"user agent: {policy.user_agent}")


@app.command()
def machine(json: bool = typer.Option(False, "--json", help="Print JSON.")) -> None:
    """Measure disk, memory, CPUs and Docker against the disk budget."""
    report = measure(REPO_ROOT, load_policy().disk_budget_fraction)
    if json:
        typer.echo(report.model_dump_json(indent=2))
        return
    for key, value in report.model_dump().items():
        typer.echo(f"{key}: {value}")
