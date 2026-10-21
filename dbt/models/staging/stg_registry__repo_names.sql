-- Names a GitHub repository id has (current) or had (previous), per ecosyste.ms, from the
-- newest fetch of each id only (a rename between refreshes must not leave two current names).
-- A name may be the previous name of one id and the current name of another (taken again).
WITH latest AS (
    SELECT
        repo_github_id,
        MAX(fetched_at) AS fetched_at
    FROM {{ source('silver', 'registry_repo_names') }}
    GROUP BY repo_github_id
)

SELECT
    n.repo_github_id AS repo_id,
    n.repo_name_lower,
    n.kind,
    MAX(n.fetched_at) AS fetched_at
FROM {{ source('silver', 'registry_repo_names') }} AS n
INNER JOIN latest AS l
    ON n.repo_github_id = l.repo_github_id AND n.fetched_at = l.fetched_at
GROUP BY n.repo_github_id, n.repo_name_lower, n.kind
