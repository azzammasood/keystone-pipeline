-- Rows of the chosen copy of each record only (see stg_osv__vulns).
SELECT
    r.vuln_id,
    r.affected_index,
    r.kind,
    r.url,
    r.repo_host,
    r.repo_full_name
FROM {{ source('silver', 'osv_repo_refs') }} AS r
INNER JOIN {{ ref('stg_osv__vulns') }} AS v
    ON r.vuln_id = v.vuln_id AND r.source_file = v.source_file
