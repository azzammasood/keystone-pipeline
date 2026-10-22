#!/usr/bin/env bash
# Open one issue per failing workflow, or comment on it while it stays open (the title is the
# key). Runs only in a workflow's `if: failure()` step with `issues: write` and GH_TOKEN set.
set -euo pipefail

title="[keystone] ${GITHUB_WORKFLOW} workflow failing"
run_url="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
body="Run ${run_url} failed (event \`${GITHUB_EVENT_NAME}\`, commit ${GITHUB_SHA}, attempt ${GITHUB_RUN_ATTEMPT}). Close this issue once a run passes."

number=$(gh issue list --repo "$GITHUB_REPOSITORY" --state open --limit 100 \
  --search "in:title \"${title}\"" --json number,title \
  --jq "map(select(.title == \"${title}\")) | .[0].number // empty")

if [ -n "$number" ]; then
  gh issue comment "$number" --repo "$GITHUB_REPOSITORY" --body "$body"
else
  gh issue create --repo "$GITHUB_REPOSITORY" --title "$title" --body "$body"
fi
