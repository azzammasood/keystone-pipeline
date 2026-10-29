-- Every reviewed decision still names a candidate the marts list: a key that changed (a
-- name whose GitHub id became known) would silently orphan the decision. Decisions about a
-- package or name that is not in the loaded data (fixtures, a partial run) cannot be checked.
-- A name-keyed candidate is compared by id once the name's id is known (ADR-073).
WITH d AS (
    SELECT
        r.subject,
        r.subject_key,
        r.candidate,
        CASE
            WHEN r.subject = 'package_repo'
                THEN COALESCE('github.com#' || CAST(i.repo_id AS STRING), r.candidate)
            ELSE r.candidate
        END AS candidate_key
    FROM {{ ref('resolve_decisions') }} AS r
    LEFT JOIN {{ ref('int_repo_identity') }} AS i
        ON LOWER(r.candidate) = 'github.com/' || i.repo_name_lower
)

SELECT d.subject, d.subject_key, d.candidate
FROM d
WHERE
    (
        d.subject = 'package_repo'
        AND EXISTS (
            SELECT 1 FROM {{ ref('map_package_repo') }} AS p
            WHERE p.package_key = d.subject_key
        )
        AND NOT EXISTS (
            SELECT 1 FROM {{ ref('map_package_repo') }} AS m
            WHERE m.package_key = d.subject_key AND m.repo_key = d.candidate_key
        )
    )
    OR (
        d.subject = 'repo_name_id'
        AND EXISTS (
            SELECT 1 FROM {{ ref('map_repo_name_id') }} AS p
            WHERE p.repo_name_lower = d.subject_key
        )
        AND NOT EXISTS (
            SELECT 1 FROM {{ ref('map_repo_name_id') }} AS m
            WHERE m.repo_name_lower = d.subject_key AND CAST(m.repo_id AS STRING) = d.candidate
        )
    )
