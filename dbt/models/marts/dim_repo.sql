-- One row per repository id seen in GH Archive, with its current (latest) name.
WITH names AS (
    SELECT
        repo_id,
        COUNT(DISTINCT repo_name_lower) AS distinct_names,
        COUNT(*) AS name_spells,
        MIN(valid_from) AS first_seen_at,
        MAX(last_seen_at) AS last_seen_at,
        MIN(first_era) AS first_era,
        MAX(last_era) AS last_era,
        SUM(events) AS events
    FROM {{ ref('dim_repo_name_history') }}
    GROUP BY repo_id
)

SELECT
    n.repo_id,
    c.repo_name,
    c.repo_name_lower,
    CAST(n.distinct_names AS BIGINT) AS distinct_names,
    CAST(n.name_spells AS BIGINT) AS name_spells,
    n.first_seen_at,
    n.last_seen_at,
    n.first_era,
    n.last_era,
    CAST(n.events AS BIGINT) AS events
FROM names AS n
INNER JOIN {{ ref('dim_repo_name_history') }} AS c
    ON n.repo_id = c.repo_id AND c.is_current
