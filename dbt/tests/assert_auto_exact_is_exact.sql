-- Only exact matches are auto-accepted: both independent source families (registry and an
-- OSV GIT range) name the repository and no other candidate still in play has one.
SELECT m.package_key, m.repo_key
FROM {{ ref('map_package_repo') }} AS m
WHERE
    m.status = 'auto_exact'
    AND (
        m.independent_sources < 2
        OR EXISTS (
            SELECT 1 FROM {{ ref('map_package_repo') }} AS o
            WHERE
                o.package_key = m.package_key
                AND o.repo_key <> m.repo_key
                AND o.independent_sources > 0
                AND NOT (o.rule = 'review' AND o.status = 'rejected')
        )
    )
