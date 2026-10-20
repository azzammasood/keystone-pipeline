SELECT
    vuln_id,
    published_at,
    modified_at,
    withdrawn_at,
    alias_count,
    affected_count,
    is_malicious,
    severity_label,
    cvss_v3_vector,
    cvss_v4_vector,
    github_reviewed,
    source_file,
    source_sha256,
    fetched_at
FROM {{ source('silver', 'osv_vulns') }}
