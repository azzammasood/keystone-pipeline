-- Packages Phase 7 resolves, one row per normalised key: OSV packages (dim_package, several
-- OSV spellings of one PyPI name collapse here) and the ecosyste.ms critical sets fetched by
-- `keystone resolve fetch`. Packages whose only OSV records are malware reports are kept
-- but out of scope: such packages are removed from their registry and have no repository.
WITH osv AS (
    SELECT
        {{ canonical_package_key('ecosystem', 'package_name') }} AS package_key,
        MIN(ecosystem) AS ecosystem,
        MIN(package_name) AS package_name,
        CAST(COUNT(*) AS BIGINT) AS osv_spellings,
        MAX(CASE WHEN vuln_count > malicious_vuln_count THEN 1 ELSE 0 END) AS has_advisory
    FROM {{ ref('dim_package') }}
    GROUP BY {{ canonical_package_key('ecosystem', 'package_name') }}
),

registry AS (
    SELECT
        package_key,
        MIN(ecosystem) AS ecosystem,
        MIN(package_name) AS package_name,
        MAX(CASE WHEN source = 'ecosystems' AND critical THEN 1 ELSE 0 END) AS critical
    FROM {{ ref('stg_registry__packages') }}
    GROUP BY package_key
)

SELECT
    COALESCE(o.package_key, r.package_key) AS package_key,
    COALESCE(o.ecosystem, r.ecosystem) AS ecosystem,
    COALESCE(r.package_name, o.package_name) AS package_name,
    o.package_key IS NOT NULL AS in_osv,
    COALESCE(o.osv_spellings, 0) AS osv_spellings,
    COALESCE(r.critical, 0) = 1 AS critical,
    CASE
        WHEN r.critical = 1 THEN 'critical'
        WHEN o.has_advisory = 1 THEN 'osv'
        WHEN o.package_key IS NOT NULL THEN 'osv_malware_only'
        ELSE 'registry_only'
    END AS package_scope
FROM osv AS o
FULL OUTER JOIN registry AS r ON o.package_key = r.package_key
