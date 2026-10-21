-- Every reviewed decision still names a candidate the marts list: a key that changed (a
-- name whose GitHub id became known) would silently orphan the decision.
SELECT d.subject, d.subject_key, d.candidate
FROM {{ ref('resolve_decisions') }} AS d
WHERE
    (
        d.subject = 'package_repo'
        AND NOT EXISTS (
            SELECT 1 FROM {{ ref('map_package_repo') }} AS m
            WHERE m.package_key = d.subject_key AND m.repo_key = d.candidate
        )
    )
    OR (
        d.subject = 'repo_name_id'
        AND NOT EXISTS (
            SELECT 1 FROM {{ ref('map_repo_name_id') }} AS m
            WHERE m.repo_name_lower = d.subject_key AND CAST(m.repo_id AS STRING) = d.candidate
        )
    )
