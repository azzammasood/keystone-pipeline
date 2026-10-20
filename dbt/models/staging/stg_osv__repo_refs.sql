SELECT
    vuln_id,
    affected_index,
    kind,
    url,
    repo_host,
    repo_full_name
FROM {{ source('silver', 'osv_repo_refs') }}
