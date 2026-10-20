-- Package to repository candidates from OSV evidence (mapping_source = 'osv'). Confidence is a
-- rule, not a score: high = a GIT range of the package itself; medium = a FIX reference in a
-- record about this package only; low = a FIX reference in a record about several packages.
-- repo_id is filled only when exactly one GH Archive repository id ever carried the name;
-- registry metadata and ecosyste.ms join in Phase 7 (ADR-010).
WITH evidence AS (
    SELECT
        package_key,
        repo_host,
        repo_full_name,
        MAX(CASE WHEN kind = 'git_range' THEN 1 ELSE 0 END) AS has_git_range,
        MAX(CASE WHEN kind = 'fix_reference' AND packages_in_vuln = 1 THEN 1 ELSE 0 END)
            AS has_sole_fix,
        CAST(COUNT(DISTINCT vuln_id) AS BIGINT) AS evidence_vulns
    FROM {{ ref('int_package_repo_evidence') }}
    GROUP BY package_key, repo_host, repo_full_name
),

name_owners AS (
    SELECT
        repo_name_lower,
        MIN(repo_id) AS repo_id,
        COUNT(DISTINCT repo_id) AS repo_ids
    FROM {{ ref('dim_repo_name_history') }}
    GROUP BY repo_name_lower
)

SELECT
    e.package_key,
    e.repo_host,
    e.repo_full_name,
    CASE WHEN e.repo_host = 'github.com' AND o.repo_ids = 1 THEN o.repo_id END AS repo_id,
    CASE
        WHEN e.repo_host <> 'github.com' THEN 'not_github'
        WHEN o.repo_ids IS NULL THEN 'not_seen'
        WHEN o.repo_ids = 1 THEN 'unique_name'
        ELSE 'ambiguous_name'
    END AS repo_match,
    CASE
        WHEN e.has_git_range = 1 THEN 'high'
        WHEN e.has_sole_fix = 1 THEN 'medium'
        ELSE 'low'
    END AS confidence,
    e.evidence_vulns,
    'osv' AS mapping_source
FROM evidence AS e
LEFT JOIN name_owners AS o ON e.repo_full_name = o.repo_name_lower
