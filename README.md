# Keystone pipeline

Keystone asks: **which open-source projects does the world quietly depend on, and which are fragile?**

It will ingest GH Archive (the public GitHub event timeline), join OSV vulnerability data and a
package-to-repository mapping, and compute a transparent, versioned, rule-based fragility and risk
signal per critical project. Signals are descriptive, never predictions about any project or person.

**Status: Phase 0 (repo and tooling).** Nothing has been ingested or measured yet. This README will
only state scale, tools and results once they are measured; see `docs/PROGRESS.md`.

Not affiliated with GitHub, GH Archive, OSV or any project maintainer.

## Develop

```
uv sync
uv run pre-commit install
uv run keystone --help
uv run ruff check . ; uv run mypy ; uv run pytest
```

License: MIT.
