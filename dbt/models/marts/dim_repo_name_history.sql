-- Name history per repository id (type-2 style): one row per uninterrupted spell of a name.
-- A spell starts where the name differs (case-insensitively: GitHub names are, so a case
-- change is not a rename) from the previous sighting, hourly rows in time order; a name taken
-- again later opens a new spell. valid_to is the next spell's first sighting. Within one hour
-- old and new names can interleave, so last_seen_at may pass valid_to by less than an hour.
WITH ordered AS (
    SELECT
        *,
        LOWER(repo_name) AS name_lower,
        CASE
            WHEN
                LAG(LOWER(repo_name)) OVER (
                    PARTITION BY repo_id ORDER BY first_seen_at, repo_name
                )
                = LOWER(repo_name)
                THEN 0
            ELSE 1
        END AS starts_spell
    FROM {{ ref('int_repo_name_hours') }}
),

numbered AS (
    SELECT
        *,
        SUM(starts_spell) OVER (
            PARTITION BY repo_id ORDER BY first_seen_at, repo_name
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS spell
    FROM ordered
),

spells AS (
    SELECT
        repo_id,
        spell,
        -- the spelling of the latest sighting in the spell
        MAX_BY(repo_name, last_seen_at) AS repo_name,
        MIN(name_lower) AS repo_name_lower,
        MIN(first_seen_at) AS valid_from,
        MAX(last_seen_at) AS last_seen_at,
        MIN(era_min) AS first_era,
        MAX(era_max) AS last_era,
        CAST(SUM(events) AS BIGINT) AS events
    FROM numbered
    GROUP BY repo_id, spell
)

SELECT
    repo_id,
    CAST(spell AS BIGINT) AS name_seq,
    repo_name,
    repo_name_lower,
    valid_from,
    LEAD(valid_from) OVER (PARTITION BY repo_id ORDER BY spell) AS valid_to,
    last_seen_at,
    first_era,
    last_era,
    events,
    CASE
        WHEN LEAD(valid_from) OVER (PARTITION BY repo_id ORDER BY spell) IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current
FROM spells
