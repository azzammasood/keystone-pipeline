-- Every rollup event with a repository id lands in exactly one daily row.
WITH staged AS (
    SELECT SUM(events) AS events FROM {{ ref('stg_rollup__repo_hour') }} WHERE repo_id IS NOT NULL
),

fact AS (
    SELECT SUM(events) AS events FROM {{ ref('fct_repo_day') }}
),

source_rows AS (
    SELECT SUM(events) AS events FROM {{ source('rollup', 'repo_hour') }} WHERE repo_id IS NOT NULL
)

SELECT r.events AS rollup_events, f.events AS fact_events, p.events AS source_events
FROM staged AS r, fact AS f, source_rows AS p
WHERE r.events <> f.events OR p.events <> f.events
