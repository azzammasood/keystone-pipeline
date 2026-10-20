# Decisions (ADRs)

Newest first. Each entry: context, options, choice, why, date. Scope changes are recorded only here.

## ADR-008 (2026-10-20) Subpackages created when a phase needs them
- Context: brief section 9c lists 15 subpackages. Options: create all now (empty), or on demand.
- Choice: on demand; CLAUDE.md lists the planned layout. Why: empty packages are noise and fail
  the tool-credibility spirit (nothing listed that does no work).

## ADR-007 (2026-10-20) Dependency audit via `uv export` + pip-audit
- Options: pip-audit on the runner env; `uv export` lockfile audit; OSV-Scanner.
- Choice: `uv export --all-groups` -> `pip-audit --strict -r`. Why: audits exactly the locked set,
  $0, no extra binary. OSV-Scanner may be added later since Keystone consumes OSV anyway.

## ADR-006 (2026-10-20) Scorecard while private
- Context: Scorecard publishing and code-scanning SARIF upload need a public repo (or paid GHAS).
- Choice: workflow runs now, keeps SARIF as an artifact; publish + upload switch on automatically
  when the repo becomes public (`github.event.repository.private`). Why: $0, no later edit needed.

## ADR-005 (2026-10-20) Type checker: mypy (strict on `src/`)
- Options: mypy, pyright, ty. Choice: mypy. Why: most widely known in data-engineering teams,
  stable, pydantic plugin available, runs via uv with no Node dependency.

## ADR-004 (2026-10-20) Actions pinned to commit SHAs
- Choice: every `uses:` pinned to a full SHA with the tag in a comment; Dependabot bumps them.
  Why: Scorecard "Pinned-Dependencies" and supply-chain hygiene (Keystone measures this in others).

## ADR-003 (2026-10-20) Commit identity
- Context: brief says ask for the commit email. Roshni's repo-local identity is
  `azzammasood <ahmaduzzammasood@gmail.com>`, the same account Keystone belongs to.
- Choice: use that identity, repo-local only. Owner to correct if a different email is wanted
  (history is still local, so it can be rewritten at no cost before the first push).

## ADR-002 (2026-10-20) SSH alias `github-azzammasood`
- Found: `~/.ssh/config` has `github-azzammasood` (id_ed25519) and `github-azzammasood-coder`
  (a second key). `ssh -T` answers `azzammasood` and `azzammasood-coder` respectively; Roshni's
  remote uses `github-azzammasood`. Choice: `git@github-azzammasood:azzammasood/keystone-pipeline.git`.
  Not ambiguous: the alias, account and Roshni precedent agree.
- Note: the brief says gh is logged into azzamverse; `gh auth status` shows the active account is
  **azzammasood**. Irrelevant to Keystone (gh is not used against these repos) but recorded.

## ADR-001 (2026-10-20) Local root and Python
- Root `C:\Users\ahmad.d\Documents\projects\keystone\` (exists, sibling of `roshni\`), branch `main`.
- System Python is 3.11; uv provides 3.12 (`requires-python >=3.12,<3.13`). Scripts are
  cross-platform (pathlib), so the PowerShell-vs-Git-Bash question does not block anything.
