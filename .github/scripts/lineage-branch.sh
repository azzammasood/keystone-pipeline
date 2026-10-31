#!/usr/bin/env bash
# Push the compacted OpenLineage event log to the orphan data branch `lineage-log` (ADR-085).
# `keystone lineage compact` has already gzipped past days and summarised the oldest beyond
# its cap; this copies `events/*.jsonl.gz` and `summary.jsonl` and refuses a tree over the cap.
# One new commit per run, never a force push. Unchanged day files cost nothing in git. The
# events name jobs and datasets only (repository level, no people).
# Usage: lineage-branch.sh LINEAGE_DIR CAP_MB   (remote: LINEAGE_REMOTE, else the repository
#        over HTTPS with GH_TOKEN)
set -euo pipefail
src=${1:?lineage dir}
cap_mb=${2:?cap in MB}
branch=lineage-log
if [ -n "${LINEAGE_REMOTE:-}" ]; then
  url=$LINEAGE_REMOTE
else
  : "${GH_TOKEN:?GH_TOKEN is not set}" "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}"
  url="https://x-access-token:${GH_TOKEN}@github.com/${GITHUB_REPOSITORY}.git"
fi
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
git -C "$work" init -q
git -C "$work" config user.name "github-actions[bot]"
git -C "$work" config user.email "41898282+github-actions[bot]@users.noreply.github.com"
if git -C "$work" fetch -q --depth 1 "$url" "$branch" 2>/dev/null; then
  git -C "$work" checkout -q -B "$branch" FETCH_HEAD
else
  git -C "$work" checkout -q --orphan "$branch"
fi
rm -rf "$work/events" "$work/summary.jsonl"
mkdir -p "$work/events"
cp "$src"/events/*.jsonl.gz "$work/events/" 2>/dev/null || true
if [ -f "$src/summary.jsonl" ]; then cp "$src/summary.jsonl" "$work/"; fi
bytes=$(find "$work" -path "$work/.git" -prune -o -type f -printf '%s\n' | awk '{s += $1} END {print s + 0}')
if [ "$bytes" -gt $((cap_mb * 1000000)) ]; then
  echo "::error::lineage log is $bytes bytes, over the ${cap_mb} MB cap; nothing pushed"
  exit 1
fi
git -C "$work" add -A
if git -C "$work" diff --cached --quiet; then
  echo "lineage-log: no change ($bytes bytes)"
  exit 0
fi
git -C "$work" commit -q -m "data: OpenLineage log $(date -u +%F) ($bytes bytes)"
git -C "$work" push -q "$url" "HEAD:refs/heads/$branch"
echo "lineage-log: pushed $bytes bytes"
