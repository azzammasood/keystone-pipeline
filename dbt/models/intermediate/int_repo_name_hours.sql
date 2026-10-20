-- One row per repository id, name and UTC hour in which the name was seen. Rows without a
-- repository id (E1 2012, some E0) are left to Phase 7 (resolve by name, with a confidence).
SELECT
    repo_id,
    repo_name,
    DATE_TRUNC('hour', created_at) AS seen_hour,
    MIN(created_at) AS first_seen_at,
    MAX(created_at) AS last_seen_at,
    MIN(era) AS era_min,
    MAX(era) AS era_max,
    CAST(COUNT(*) AS BIGINT) AS events
FROM {{ ref('stg_gharchive__repo_names') }}
WHERE repo_id IS NOT NULL
GROUP BY repo_id, repo_name, DATE_TRUNC('hour', created_at)
