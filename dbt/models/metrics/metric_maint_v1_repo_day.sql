-- maint_v1 inputs (era-map.md): E1 and E2 only, where the PR merged flag exists. Counts per
-- repository, UTC day, era and period; scoring and completeness checks come in Phase 6.
SELECT
    activity_date,
    repo_id,
    era,
    completeness_period,
    'maint_v1' AS metric_version,
    prs_opened,
    prs_closed,
    prs_merged,
    pr_merge_known,
    issues_opened,
    issues_closed,
    releases_published
FROM {{ ref('fct_repo_day') }}
WHERE era IN ('E1', 'E2')
