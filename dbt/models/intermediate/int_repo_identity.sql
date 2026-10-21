-- Which GitHub repository id a claimed name means today, one row per claimed github.com name.
-- Rules in order (ADR-037): ecosyste.ms says the name is the current name of exactly one id;
-- else ecosyste.ms knows it as the previous name (or the redirect) of exactly one id
-- (renamed); else GH Archive saw exactly one id with the name; else GH Archive saw several
-- and the latest holder is taken (a name taken again); else no id (not seen).
WITH names AS (
    SELECT DISTINCT repo_full_name AS repo_name_lower
    FROM {{ ref('int_package_repo_claims') }}
    WHERE repo_host = 'github.com'
),

eco_current AS (
    SELECT
        repo_name_lower,
        MIN(repo_id) AS repo_id,
        COUNT(DISTINCT repo_id) AS ids
    FROM {{ ref('stg_registry__repo_names') }}
    WHERE kind = 'current'
    GROUP BY repo_name_lower
),

eco_alias AS (
    -- previous names, and claimed URLs ecosyste.ms resolved to another current name
    SELECT
        repo_name_lower,
        MIN(repo_id) AS repo_id,
        COUNT(DISTINCT repo_id) AS ids
    FROM (
        SELECT repo_name_lower, repo_id
        FROM {{ ref('stg_registry__repo_names') }}
        WHERE kind = 'previous'
        UNION ALL
        SELECT repo_full_name AS repo_name_lower, ecosystems_repo_id AS repo_id
        FROM {{ ref('int_package_repo_claims') }}
        WHERE source = 'ecosystems' AND ecosystems_repo_id IS NOT NULL
    ) AS a
    GROUP BY repo_name_lower
),

gha_spells AS (
    SELECT
        repo_name_lower,
        repo_id,
        MAX(last_seen_at) AS last_seen_at
    FROM {{ ref('dim_repo_name_history') }}
    GROUP BY repo_name_lower, repo_id
),

gha AS (
    SELECT
        repo_name_lower,
        COUNT(*) AS ids,
        MAX(CASE WHEN latest = 1 THEN repo_id END) AS latest_repo_id
    FROM (
        SELECT
            repo_name_lower,
            repo_id,
            ROW_NUMBER() OVER (
                PARTITION BY repo_name_lower ORDER BY last_seen_at DESC, repo_id DESC
            ) AS latest
        FROM gha_spells
    ) AS s
    GROUP BY repo_name_lower
),

resolved AS (
    SELECT
        n.repo_name_lower,
        CASE
            WHEN c.ids = 1 THEN c.repo_id
            WHEN a.ids = 1 THEN a.repo_id
            WHEN g.ids >= 1 THEN g.latest_repo_id
        END AS repo_id,
        CASE
            WHEN c.ids = 1 THEN 'ecosystems_current'
            WHEN a.ids = 1 THEN 'ecosystems_previous_name'
            WHEN g.ids = 1 THEN 'gharchive_unique_name'
            WHEN g.ids > 1 THEN 'gharchive_latest_holder'
            WHEN c.ids > 1 OR a.ids > 1 THEN 'ambiguous'
            ELSE 'not_seen'
        END AS id_rule,
        COALESCE(g.ids, 0) AS gharchive_ids,
        g.latest_repo_id AS gharchive_latest_repo_id
    FROM names AS n
    LEFT JOIN eco_current AS c ON n.repo_name_lower = c.repo_name_lower
    LEFT JOIN eco_alias AS a ON n.repo_name_lower = a.repo_name_lower
    LEFT JOIN gha AS g ON n.repo_name_lower = g.repo_name_lower
),

current_names AS (
    SELECT
        repo_id,
        MIN(repo_name_lower) AS repo_name_lower
    FROM {{ ref('stg_registry__repo_names') }}
    WHERE kind = 'current'
    GROUP BY repo_id
    HAVING COUNT(DISTINCT repo_name_lower) = 1
)

SELECT
    r.repo_name_lower,
    r.repo_id,
    r.id_rule,
    COALESCE(cn.repo_name_lower, d.repo_name_lower, r.repo_name_lower) AS current_name,
    d.repo_id IS NOT NULL AS seen_in_gharchive,
    r.gharchive_ids,
    r.repo_id IS NOT NULL
    AND r.gharchive_latest_repo_id IS NOT NULL
    AND r.gharchive_latest_repo_id <> r.repo_id AS gharchive_disagrees
FROM resolved AS r
LEFT JOIN current_names AS cn ON r.repo_id = cn.repo_id
LEFT JOIN {{ ref('dim_repo') }} AS d ON r.repo_id = d.repo_id
