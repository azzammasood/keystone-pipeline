-- One row per repository id, name and UTC hour in which the name was seen. Rows without a
-- repository id (E1 2012, some E0) are left to Phase 7 (resolve by name, with a confidence).
-- The E1/E2 overlap rule is the rollups' own (ADR-020, rollup/sql/input.sql): E1 rows from
-- 2015-01-01 00:00Z on are copies of E2 events and are dropped, so nothing is counted twice.
-- Hours retired from silver (ADR-089) come from the rollup's repo_name_runs: their runs of one
-- exact name, placed at the hour of the first sighting. Merging consecutive sightings of one
-- name keeps the spells of dim_repo_name_history unchanged (minimum, maximum and sum carry).
WITH live AS (
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
    WHERE
        repo_id IS NOT NULL
        AND NOT (era = 'E1' AND created_at >= TIMESTAMP '2015-01-01 00:00:00')
    GROUP BY repo_id, repo_name, DATE_TRUNC('hour', created_at)
),

retired AS (
    SELECT
        repo_id,
        repo_name,
        DATE_TRUNC('hour', first_seen_at) AS seen_hour,
        first_seen_at,
        last_seen_at,
        era_min,
        era_max,
        events
    FROM {{ source('rollup', 'repo_name_runs') }}
),

both_sources AS (
    SELECT * FROM live
    UNION ALL
    SELECT * FROM retired
)

-- an hour can hold rows of a retired and a live source file (events near the hour's edge)
SELECT
    repo_id,
    repo_name,
    seen_hour,
    MIN(first_seen_at) AS first_seen_at,
    MAX(last_seen_at) AS last_seen_at,
    MIN(era_min) AS era_min,
    MAX(era_max) AS era_max,
    CAST(SUM(events) AS BIGINT) AS events
FROM both_sources
GROUP BY repo_id, repo_name, seen_hour
