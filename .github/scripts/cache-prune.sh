#!/usr/bin/env bash
# Keep only the newest data-cache entries on main (ADR-089). Every ingest and publish saves a
# new entry of the whole data directory; GitHub's free 10 GB per repository then evicts the
# oldest, but only after the limit is passed. Deleting the older entries right after each save
# keeps the cache at about KEEP entries. Needs `actions: write`. Prints sizes, never contents.
# Usage: cache-prune.sh [KEEP]   (env GH_TOKEN, GH_REPO)
set -euo pipefail

keep="${1:-2}"
list() {
  gh cache list --key keystone-data- --ref refs/heads/main --sort created_at --order desc \
    --limit 100 --json id,key,sizeInBytes,createdAt "$@"
}
echo "data cache entries before pruning:"
list --jq '.[] | "  \(.key) \(.sizeInBytes) bytes \(.createdAt)"'
list --jq ".[${keep}:][].id" | while read -r id; do
  gh cache delete "$id"
done
total=$(list --jq 'map(.sizeInBytes) | add // 0')
n=$(list --jq length)
echo "data cache after pruning: $n entries, $total bytes"
echo "- data cache: $n entries, $total bytes after pruning (newest entry $(list --jq '.[0].sizeInBytes // 0') bytes)" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
