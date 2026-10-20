-- keystone-rollup: repo_hour
-- window: 1 hour
-- keys: repo_id, era, event_type
-- max: fetched_at_max
-- Activity per repository, schema era and event type in tumbling event-time hours (UTC).
-- One row per input event; every other column is summed per window and key. Era is a key, so
-- eras are never mixed (era-map.md). `merged` is measured only where `merge_known` counts it
-- (E1 and E2): E3 PR payloads carry no merged flag.
SELECT
    created_at,
    repo_id,
    era,
    event_type,
    1 AS events,
    CASE WHEN actor_is_bot THEN 0 ELSE 1 END AS human_events,
    CASE WHEN action = 'opened' THEN 1 ELSE 0 END AS opened,
    CASE WHEN action = 'closed' THEN 1 ELSE 0 END AS closed,
    CASE WHEN action = 'reopened' THEN 1 ELSE 0 END AS reopened,
    CASE WHEN action = 'closed' AND pr_merged THEN 1 ELSE 0 END AS merged,
    CASE
        WHEN action = 'closed' AND pr_merged IS NOT NULL THEN 1 ELSE 0
    END AS merge_known,
    CASE WHEN action = 'published' THEN 1 ELSE 0 END AS published,
    CASE WHEN action = 'added' THEN 1 ELSE 0 END AS added,
    fetched_at AS fetched_at_max
FROM rollup_input
