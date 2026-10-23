"""`keystone narrate brief`: the weekly risk brief (Phase 10, ADR-061).

Kept out of cli.py (one `add_typer` line there).
"""

import os
from pathlib import Path

import typer

from keystone import __version__
from keystone.export import envelope, site
from keystone.narrate import brief
from keystone.paths import DATA_DIR
from keystone.resolve.chooser import DEFAULT_MODEL
from keystone.telemetry.runlog import Stage

narrate_app = typer.Typer(no_args_is_help=True, help="Weekly risk brief (Phase 10).")


@narrate_app.command("brief")
def narrate_brief(
    site_dir: Path = typer.Option(site.OUT, "--site", help="`keystone export site` output."),
    root: Path = typer.Option(DATA_DIR, help="Data directory (run log)."),
    model: str = typer.Option(DEFAULT_MODEL, help="OpenRouter model id; must end in :free."),
    cache_dir: Path = typer.Option(DATA_DIR / "narrate" / "cache", help="Answer cache."),
) -> None:
    """Write brief.json from the exported facts and re-index the site directory. Skipped (and
    said so in the file) without OPENROUTER_API_KEY."""
    key = os.environ.get("OPENROUTER_API_KEY", "")
    ask, requests = brief.openrouter_ask(key, model) if key else (None, lambda: 0)
    service, version = ("openrouter", model) if ask else ("keystone", __version__)
    with Stage(root, "narrate", service, version) as st:
        row = brief.write_brief(
            site_dir,
            cache_dir,
            root=root,
            model=model,
            ask=ask,
            skip_reason="" if key else "OPENROUTER_API_KEY is not set",
        )
        st.detail["outcome"] = row["outcome"]
        st.detail["requests"] = requests()
        st.records_out = 1 if row["text"] else 0
        site.write_index(site_dir, envelope.recorded_profile(root))
    typer.echo(f"brief {row['week']}: {row['outcome']} ({row['detail']}); requests={requests()}")
    if row["text"]:
        typer.echo(row["text"])
