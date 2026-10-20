# Keystone pipeline

Keystone asks: **which open-source projects does the world quietly depend on, and which are fragile?**

It will ingest GH Archive (the public GitHub event timeline), join OSV vulnerability data and a
package-to-repository mapping, and compute a transparent, versioned, rule-based fragility and risk
signal per critical project. Signals are descriptive, never predictions about any project or person.

**Status: backend in progress (Phase 5 of 10, lakehouse and warehouse).** This README will state scale,
tools and results only once they are measured and published; see `docs/PROGRESS.md`.

Not affiliated with GitHub, GH Archive, OSV or any project maintainer.

## Develop

```
uv sync
uv run pre-commit install
uv run keystone --help
uv run ruff check . ; uv run mypy ; uv run pytest
```

License: MIT.
