-- maint_v2 inputs (era-map.md): E3 only. PR payloads carry no merged flag after the trim, so
-- only closes are counted; never compare with maint_v1 without the version.
SELECT
    activity_date,
    repo_id,
    era,
    completeness_period,
    'maint_v2' AS metric_version,
    prs_opened,
    prs_closed,
    issues_opened,
    issues_closed,
    releases_published
FROM {{ ref('fct_repo_day') }}
WHERE era = 'E3'
