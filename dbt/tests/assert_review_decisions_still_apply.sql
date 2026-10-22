-- Every reviewed decision still names a candidate the marts list: a key that changed (a
-- name whose GitHub id became known) would silently orphan the decision. Decisions about a
-- package or name that is not in the loaded data (fixtures, a partial run) cannot be checked.
SELECT d.subject, d.subject_key, d.candidate
FROM {{ ref('resolve_decisions') }} AS d
WHERE
    (
        d.subject = 'package_repo'
        AND EXISTS (
            SELECT 1 FROM {{ ref('map_package_repo') }} AS p
            WHERE p.package_key = d.subject_key
        )
        AND NOT EXISTS (
            SELECT 1 FROM {{ ref('map_package_repo') }} AS m
            WHERE m.package_key = d.subject_key AND m.repo_key = d.candidate
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
