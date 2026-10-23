#!/usr/bin/env bash
# One open issue while scheduled jobs run in `free` because `full` was unhealthy (ADR-040,
# ADR-062): opened on the first fallback, its body replaced with the latest run on later ones
# (an edit, so a daily fallback does not notify anyone again). Reads pipeline_metrics.json;
# does nothing when the data was produced without a fallback. Needs `issues: write`, GH_TOKEN.
set -euo pipefail

metrics="${1:-data/metrics/pipeline_metrics.json}"
if [ "$(jq -r '.produced_by.fallback // false' "$metrics")" != "true" ]; then
  echo "no fallback recorded"
  exit 0
fi
reason=$(jq -r '.produced_by.reason // "unknown"' "$metrics")
title="[keystone] full profile unavailable: scheduled jobs ran in free"
run_url="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
body="Latest fallback: ${run_url} (${GITHUB_WORKFLOW}, $(date -u +%Y-%m-%dT%H:%MZ)).

Reason recorded by \`keystone profile check\`: ${reason}

The data was produced by the free profile (DuckDB) and says so in every export
(\`produced_by.fallback = true\`). GitHub-hosted runners have no full stack, so this issue
stays open while that holds; close it when a run uses full."

number=$(gh issue list --repo "$GITHUB_REPOSITORY" --state open --limit 100 \
  --search "in:title \"${title}\"" --json number,title \
  --jq "map(select(.title == \"${title}\")) | .[0].number // empty")

if [ -n "$number" ]; then
  gh issue edit "$number" --repo "$GITHUB_REPOSITORY" --body "$body"
else
  gh issue create --repo "$GITHUB_REPOSITORY" --title "$title" --body "$body"
fi
