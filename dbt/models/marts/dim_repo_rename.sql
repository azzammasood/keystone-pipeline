-- Repository renames (Phase 7, ADR-037): one row per repository id and name change. GH Archive
-- shows one id under consecutive names (spells of dim_repo_name_history): auto_exact, since
-- the id is the identity; the change lies between the old name's last sighting and the new
-- name's first. ecosyste.ms lists previous names of an id with no time: accepted by rule when
-- GH Archive did not see the change. A case change is not a rename; a name taken back is.
WITH spells AS (
    SELECT
        repo_id,
        name_seq,
        repo_name_lower,
        valid_from,
        last_seen_at,
        LAG(repo_name_lower) OVER (PARTITION BY repo_id ORDER BY name_seq) AS prev_name,
        LAG(last_seen_at) OVER (PARTITION BY repo_id ORDER BY name_seq) AS prev_last_seen_at
    FROM {{ ref('dim_repo_name_history') }}
),

gha AS (
    SELECT
        repo_id,
        prev_name AS from_name,
        repo_name_lower AS to_name,
        prev_last_seen_at AS renamed_after,
        valid_from AS renamed_before
    FROM spells
    WHERE prev_name IS NOT NULL
),

one_current AS (
    -- ids ecosyste.ms gives exactly one current name (two would be ambiguous)
    SELECT
        repo_id,
        MIN(repo_name_lower) AS repo_name_lower
    FROM {{ ref('stg_registry__repo_names') }}
    WHERE kind = 'current'
    GROUP BY repo_id
    HAVING COUNT(DISTINCT repo_name_lower) = 1
),

eco AS (
    SELECT DISTINCT
        p.repo_id,
        p.repo_name_lower AS from_name,
        c.repo_name_lower AS to_name
    FROM {{ ref('stg_registry__repo_names') }} AS p
    INNER JOIN one_current AS c ON p.repo_id = c.repo_id
    WHERE p.kind = 'previous' AND p.repo_name_lower <> c.repo_name_lower
),

gha_marked AS (
    SELECT
        g.*,
        e.repo_id IS NOT NULL AS by_ecosystems
    FROM gha AS g
    LEFT JOIN (SELECT DISTINCT repo_id, from_name FROM eco) AS e
        ON g.repo_id = e.repo_id AND g.from_name = e.from_name
)

SELECT
    repo_id,
    from_name,
    to_name,
    renamed_after,
    renamed_before,
    TRUE AS by_gharchive,
    by_ecosystems,
    'auto_exact' AS status,
    'same_id_new_name' AS rule,
    'high' AS confidence
FROM gha_marked

UNION ALL

SELECT
    e.repo_id,
    e.from_name,
    e.to_name,
    CAST(NULL AS TIMESTAMP) AS renamed_after,
    CAST(NULL AS TIMESTAMP) AS renamed_before,
    FALSE AS by_gharchive,
    TRUE AS by_ecosystems,
    'accepted' AS status,
    'ecosystems_previous_name' AS rule,
    'medium' AS confidence
FROM eco AS e
LEFT JOIN (SELECT DISTINCT repo_id, from_name FROM gha) AS g
    ON e.repo_id = g.repo_id AND e.from_name = g.from_name
WHERE g.repo_id IS NULL
