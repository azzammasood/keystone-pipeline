-- package_key = ecosystem/name exactly as OSV spells it; name normalisation is Phase 7 (resolve).
-- Rows of the chosen copy of each record only (see stg_osv__vulns).
SELECT
    a.vuln_id,
    a.affected_index,
    a.ecosystem,
    a.package_name,
    a.ecosystem || '/' || a.package_name AS package_key,
    a.purl,
    a.range_count,
    a.version_count,
    a.has_fixed
FROM {{ source('silver', 'osv_affected') }} AS a
INNER JOIN {{ ref('stg_osv__vulns') }} AS v
    ON a.vuln_id = v.vuln_id AND a.source_file = v.source_file
