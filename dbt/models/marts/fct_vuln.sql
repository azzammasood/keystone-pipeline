-- One row per OSV record and affected package.
SELECT
    v.vuln_id,
    a.package_key,
    MAX(a.ecosystem) AS ecosystem,
    MAX(a.package_name) AS package_name,
    MAX(v.published_at) AS published_at,
    MAX(v.modified_at) AS modified_at,
    MAX(v.withdrawn_at) AS withdrawn_at,
    MAX(CASE WHEN v.is_malicious THEN 1 ELSE 0 END) = 1 AS is_malicious,
    MAX(v.severity_label) AS severity_label,
    MAX(v.cvss_v3_vector) AS cvss_v3_vector,
    MAX(v.cvss_v4_vector) AS cvss_v4_vector,
    MAX(CASE WHEN a.has_fixed THEN 1 ELSE 0 END) = 1 AS has_fixed,
    MAX(v.alias_count) AS alias_count,
    MAX(v.source_file) AS source_file,
    MAX(v.source_sha256) AS source_sha256,
    MAX(v.fetched_at) AS fetched_at
FROM {{ ref('stg_osv__vulns') }} AS v
INNER JOIN {{ ref('stg_osv__affected') }} AS a ON v.vuln_id = a.vuln_id
GROUP BY v.vuln_id, a.package_key
