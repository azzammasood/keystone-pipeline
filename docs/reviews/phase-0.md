# Phase 0 review

- Date: 2026-10-20. Reviewer: independent subagent, fresh context, read-only.
- Checklist: identity and remote, `.gitignore`/`.gitattributes`, tracked files (23, largest
  `uv.lock` 96 KB, no secrets: gitleaks, detect-private-key, grep), ruff, format, mypy strict,
  pytest (6 passed), pre-commit all hooks, actionlint clean, action SHAs match tags, CI permissions
  minimal, pip-audit flags valid (local run: no known vulnerabilities), Dependabot, docs, README
  claims, licence, `.env.example`, user agent and GH Archive limits, privacy. All PASS except below.

## Findings and fixes
1. Major: Scorecard on `schedule` has no `repository.private`, so publish/SARIF upload would run
   while private and fail. Fixed: gated on repo variable `KEYSTONE_PUBLIC == 'true'` (ADR-006).
2. Minor: pre-commit hooks not installed in `.git/hooks`. Fixed: `uv run pre-commit install`,
   added to CLAUDE.md and README.
3. Minor: `uv python install` without `.python-version` installs the latest Python. Fixed:
   `.python-version` = 3.12.
4. Minor: docs referenced files not yet created. Fixed: CLAUDE.md says which phase creates them.
5. Minor: README Develop section lacked lint/type commands. Fixed.

## Outstanding for the gate
- CI result on GitHub: not observable from here (brief forbids `gh` on Keystone repos); owner check.
