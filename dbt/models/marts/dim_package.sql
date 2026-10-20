-- One row per package named in OSV (ecosystem + name as OSV spells it).
SELECT
    a.package_key,
    a.ecosystem,
    a.package_name,
    MAX(a.purl) AS purl,
    CAST(COUNT(DISTINCT a.vuln_id) AS BIGINT) AS vuln_count,
    CAST(COUNT(DISTINCT CASE WHEN v.is_malicious THEN a.vuln_id END) AS BIGINT)
        AS malicious_vuln_count,
    CAST(COUNT(DISTINCT CASE WHEN v.withdrawn_at IS NOT NULL THEN a.vuln_id END) AS BIGINT)
        AS withdrawn_vuln_count,
    MIN(v.published_at) AS first_vuln_published_at,
    MAX(v.modified_at) AS last_vuln_modified_at
FROM {{ ref('stg_osv__affected') }} AS a
INNER JOIN {{ ref('stg_osv__vulns') }} AS v ON a.vuln_id = v.vuln_id
GROUP BY a.package_key, a.ecosystem, a.package_name
