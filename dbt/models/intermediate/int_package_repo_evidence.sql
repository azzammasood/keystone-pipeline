-- Every piece of OSV evidence that a package lives in a repository. A GIT range belongs to
-- one affected package; a FIX reference belongs to the record, so it applies to each of its
-- packages and is weaker when the record names more than one.
WITH packages_per_vuln AS (
    SELECT
        vuln_id,
        COUNT(DISTINCT package_key) AS packages
    FROM {{ ref('stg_osv__affected') }}
    GROUP BY vuln_id
)

SELECT
    a.package_key,
    r.repo_host,
    r.repo_full_name,
    r.kind,
    r.vuln_id,
    p.packages AS packages_in_vuln
FROM {{ ref('stg_osv__repo_refs') }} AS r
INNER JOIN {{ ref('stg_osv__affected') }} AS a
    ON
        r.vuln_id = a.vuln_id
        AND (r.affected_index = a.affected_index OR r.affected_index IS NULL)
INNER JOIN packages_per_vuln AS p ON r.vuln_id = p.vuln_id
