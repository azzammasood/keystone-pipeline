-- package_key = ecosystem/name exactly as OSV spells it; name normalisation is Phase 7 (resolve).
SELECT
    vuln_id,
    affected_index,
    ecosystem,
    package_name,
    ecosystem || '/' || package_name AS package_key,
    purl,
    range_count,
    version_count,
    has_fixed
FROM {{ source('silver', 'osv_affected') }}
