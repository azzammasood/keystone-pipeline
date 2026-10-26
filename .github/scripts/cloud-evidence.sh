#!/usr/bin/env bash
# Upload cloud-rung evidence (ADR-067, ADR-068) to the pre-release `cloud-evidence`: the
# measured JSON and the stage records publish merges into pipeline_metrics.json. No secrets.
set -euo pipefail
repo="${GITHUB_REPOSITORY:?}"
if ! gh release view cloud-evidence --repo "$repo" > /dev/null 2>&1; then
  gh release create cloud-evidence --repo "$repo" --prerelease --latest=false \
    --title "Cloud rung evidence" \
    --notes "Measured runs of the Databricks Free Edition and BigQuery sandbox rungs (ADR-067, ADR-068): stage records and the numbers in docs/benchmarks.md. No data, no secrets."
fi
gh release upload cloud-evidence "$@" --repo "$repo" --clobber
