-- One row per OSV id. OSV puts a record in the zip of every ecosystem it affects, so silver can
-- hold copies; the newest copy wins (latest modified_at, then source_file). The affected and
-- repo-pointer models keep only rows of the chosen copy.
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY vuln_id ORDER BY modified_at DESC, source_file DESC
        ) AS copy_rank
    FROM {{ source('silver', 'osv_vulns') }}
)

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
FROM ranked
WHERE copy_rank = 1
