-- GH Archive events with a repository name but no id (E1 2012, some E0) resolved by name
-- (ADR-028, ADR-037). One row per name and UTC hour with such events. Candidates are the ids
-- GH Archive saw with that name (dim_repo_name_history). Rules, first match wins:
--   review              a reviewed decision (seed resolve_decisions, subject repo_name_id)
--   spell_covers        exactly one id's spell of the name spans the hour: auto_exact
--   unique_name_near    one id ever carried the name, within 90 days of the hour: accepted
--   unique_name_id_existed  one id ever carried the name and the id already existed at the
--                       hour (GitHub allocates ids in creation order: it is no larger than an
--                       id seen in an event before the hour): suggested, since existing then
--                       says nothing about the name the repository had then
--   unique_name_far     one id ever carried the name, newer than every id seen by then: suggested
--   several_ids         more than one id carried it (a name taken again): suggested, nearest
-- Hours with no candidate are not rows; the match-rate report counts them.
WITH unresolved AS (
    SELECT
        LOWER(repo_name) AS repo_name_lower,
        DATE_TRUNC('hour', created_at) AS event_hour,
        MIN(era) AS era,
        CAST(COUNT(*) AS BIGINT) AS events
    FROM {{ ref('stg_gharchive__repo_names') }}
    WHERE
        repo_id IS NULL
        -- the overlap rule of ADR-020: E1 rows from 2015 on are copies of E2 events
        AND NOT (era = 'E1' AND created_at >= TIMESTAMP '2015-01-01 00:00:00')
    GROUP BY LOWER(repo_name), DATE_TRUNC('hour', created_at)
),

hourly_max_id AS (
    SELECT
        DATE_TRUNC('hour', created_at) AS seen_hour,
        MAX(repo_id) AS max_id
    FROM {{ ref('stg_gharchive__repo_names') }}
    WHERE repo_id IS NOT NULL
    GROUP BY DATE_TRUNC('hour', created_at)
),

id_ceiling AS (
    -- the largest id seen in any event before each hour that has unresolved events
    SELECT
        e.event_hour,
        MAX(m.max_id) AS max_id_seen
    FROM (SELECT DISTINCT event_hour FROM unresolved) AS e
    INNER JOIN hourly_max_id AS m ON m.seen_hour < e.event_hour
    GROUP BY e.event_hour
),

candidates AS (
    SELECT
        u.repo_name_lower,
        u.event_hour,
        h.repo_id,
        MAX(
            CASE
                WHEN
                    DATE_TRUNC('hour', h.valid_from) <= u.event_hour
                    AND h.last_seen_at >= u.event_hour
                    THEN 1
                ELSE 0
            END
        ) AS covers,
        MIN(
            CASE
                WHEN h.valid_from > u.event_hour
                    THEN {{ dbt.datediff('u.event_hour', 'h.valid_from', 'hour') }}
                WHEN h.last_seen_at < u.event_hour
                    THEN {{ dbt.datediff('h.last_seen_at', 'u.event_hour', 'hour') }}
                ELSE 0
            END
        ) AS gap_hours
    FROM unresolved AS u
    INNER JOIN {{ ref('dim_repo_name_history') }} AS h
        ON u.repo_name_lower = h.repo_name_lower
    GROUP BY u.repo_name_lower, u.event_hour, h.repo_id
),

ranked AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY repo_name_lower, event_hour) AS candidate_ids,
        SUM(covers) OVER (PARTITION BY repo_name_lower, event_hour) AS covering_ids,
        ROW_NUMBER() OVER (
            PARTITION BY repo_name_lower, event_hour ORDER BY covers DESC, gap_hours, repo_id
        ) AS nearest
    FROM candidates
),

decided AS (
    SELECT
        u.repo_name_lower,
        u.event_hour,
        u.era,
        u.events,
        r.repo_id,
        r.repo_id <= COALESCE(c.max_id_seen, -1) AS id_existed,
        CAST(r.candidate_ids AS BIGINT) AS candidate_ids,
        CAST(r.gap_hours AS BIGINT) AS gap_hours,
        d.decision,
        CASE
            WHEN d.decision IS NOT NULL THEN 'review'
            WHEN r.covering_ids = 1 THEN 'spell_covers'
            WHEN r.candidate_ids = 1 AND r.gap_hours <= 90 * 24 THEN 'unique_name_near'
            WHEN r.candidate_ids = 1 AND r.repo_id <= COALESCE(c.max_id_seen, -1)
                THEN 'unique_name_id_existed'
            WHEN r.candidate_ids = 1 THEN 'unique_name_far'
            ELSE 'several_ids'
        END AS rule
    FROM unresolved AS u
    INNER JOIN ranked AS r
        ON u.repo_name_lower = r.repo_name_lower AND u.event_hour = r.event_hour
    LEFT JOIN id_ceiling AS c ON u.event_hour = c.event_hour
    LEFT JOIN {{ ref('resolve_decisions') }} AS d
        ON
            d.subject = 'repo_name_id'
            AND u.repo_name_lower = d.subject_key
            AND CAST(r.repo_id AS STRING) = d.candidate
    WHERE r.nearest = 1
)

SELECT
    repo_name_lower,
    event_hour,
    era,
    events,
    repo_id,
    id_existed,
    candidate_ids,
    gap_hours,
    CASE
        WHEN rule = 'review' THEN decision
        WHEN rule = 'spell_covers' THEN 'auto_exact'
        WHEN rule = 'unique_name_near' THEN 'accepted'
        ELSE 'suggested'
    END AS status,
    rule,
    CASE
        WHEN rule IN ('review', 'spell_covers') THEN 'high'
        WHEN rule = 'unique_name_near' THEN 'medium'
        ELSE 'low'
    END AS confidence
FROM decided
