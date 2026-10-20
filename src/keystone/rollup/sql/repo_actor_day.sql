-- keystone-rollup: repo_actor_day
-- window: 1 day
-- keys: repo_id, actor_key, actor_key_kind, era
-- max: fetched_at_max
-- Human activity per repository and salted actor key in tumbling event-time days (UTC). Feeds
-- maintenance-actor concentration and new versus departed contributors. Internal only: actor
-- keys are salted hashes and never published (CLAUDE.md privacy rules). Bots are excluded.
SELECT
    created_at,
    repo_id,
    actor_key,
    actor_key_kind,
    era,
    1 AS events,
    CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END AS pushes,
    CASE
        WHEN event_type = 'PullRequestEvent' AND action = 'opened' THEN 1 ELSE 0
    END AS prs_opened,
    CASE
        WHEN event_type = 'PullRequestEvent' AND action = 'closed' THEN 1 ELSE 0
    END AS prs_closed,
    CASE
        WHEN event_type = 'PullRequestEvent' AND action = 'closed' AND pr_merged THEN 1 ELSE 0
    END AS prs_merged,
    CASE
        WHEN event_type = 'IssuesEvent' AND action = 'opened' THEN 1 ELSE 0
    END AS issues_opened,
    CASE
        WHEN event_type = 'IssuesEvent' AND action = 'closed' THEN 1 ELSE 0
    END AS issues_closed,
    CASE
        WHEN event_type = 'ReleaseEvent' AND action = 'published' THEN 1 ELSE 0
    END AS releases,
    CASE
        WHEN event_type = 'MemberEvent' AND action = 'added' THEN 1 ELSE 0
    END AS members_added,
    CASE
        WHEN event_type IN (
            'IssueCommentEvent', 'PullRequestReviewCommentEvent',
            'PullRequestReviewEvent', 'CommitCommentEvent'
        ) THEN 1 ELSE 0
    END AS comments,
    fetched_at AS fetched_at_max
FROM rollup_input
WHERE actor_key IS NOT NULL AND repo_id IS NOT NULL AND NOT actor_is_bot
