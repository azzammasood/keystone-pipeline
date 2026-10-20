-- Daily activity per repository (UTC day), schema era and completeness period. Era and period
-- are keys, so a day that crosses a boundary has one row per side and nothing is mixed.
-- prs_merged counts only where pr_merge_known does (E1, E2; ADR-024).
SELECT
    CAST(window_start AS DATE) AS activity_date,
    repo_id,
    era,
    completeness_period,
    CAST(SUM(events) AS BIGINT) AS events,
    CAST(SUM(human_events) AS BIGINT) AS human_events,
    CAST(SUM(CASE WHEN event_type = 'PushEvent' THEN events ELSE 0 END) AS BIGINT) AS pushes,
    CAST(SUM(CASE WHEN event_type = 'PullRequestEvent' THEN opened ELSE 0 END) AS BIGINT)
        AS prs_opened,
    CAST(SUM(CASE WHEN event_type = 'PullRequestEvent' THEN closed ELSE 0 END) AS BIGINT)
        AS prs_closed,
    CAST(SUM(CASE WHEN event_type = 'PullRequestEvent' THEN merged ELSE 0 END) AS BIGINT)
        AS prs_merged,
    CAST(SUM(CASE WHEN event_type = 'PullRequestEvent' THEN merge_known ELSE 0 END) AS BIGINT)
        AS pr_merge_known,
    CAST(SUM(CASE WHEN event_type = 'IssuesEvent' THEN opened ELSE 0 END) AS BIGINT)
        AS issues_opened,
    CAST(SUM(CASE WHEN event_type = 'IssuesEvent' THEN closed ELSE 0 END) AS BIGINT)
        AS issues_closed,
    CAST(SUM(CASE WHEN event_type = 'IssueCommentEvent' THEN events ELSE 0 END) AS BIGINT)
        AS issue_comments,
    CAST(SUM(CASE WHEN event_type = 'ReleaseEvent' THEN published ELSE 0 END) AS BIGINT)
        AS releases_published,
    CAST(SUM(CASE WHEN event_type = 'MemberEvent' THEN added ELSE 0 END) AS BIGINT)
        AS members_added,
    CAST(SUM(CASE WHEN event_type = 'WatchEvent' THEN events ELSE 0 END) AS BIGINT) AS stars,
    CAST(SUM(CASE WHEN event_type = 'ForkEvent' THEN events ELSE 0 END) AS BIGINT) AS forks,
    MAX(fetched_at_max) AS fetched_at_max
FROM {{ ref('stg_rollup__repo_hour') }}
WHERE repo_id IS NOT NULL
GROUP BY CAST(window_start AS DATE), repo_id, era, completeness_period
