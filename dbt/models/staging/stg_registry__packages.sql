-- The newest claim per package and source (`keystone resolve fetch` appends a file per
-- batch and refreshes weekly). The key is recomputed from ecosystem and name with the
-- warehouse's own rule, so rows written under an older normalisation fold into one key.
WITH keyed AS (
    SELECT
        source,
        ecosystem,
        package_name,
        {{ canonical_package_key('ecosystem', 'package_name') }} AS package_key,
        found,
        critical,
        repository_url,
        repo_host,
        repo_full_name,
        repo_subpath,
        url_rejected,
        repo_github_id,
        repo_current_name,
        repo_fork,
        repo_archived,
        dependent_packages,
        dependent_repos,
        downloads,
        downloads_period,
        latest_release_at,
        source_file,
        source_sha256,
        fetched_at
    FROM {{ source('silver', 'registry_packages') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY source, package_key ORDER BY fetched_at DESC, source_file DESC
        ) AS rn
    FROM keyed
)

SELECT
    source,
    ecosystem,
    package_name,
    package_key,
    found,
    critical,
    repository_url,
    repo_host,
    repo_full_name,
    repo_subpath,
    url_rejected,
    repo_github_id,
    repo_current_name,
    repo_fork,
    repo_archived,
    dependent_packages,
    dependent_repos,
    downloads,
    downloads_period,
    latest_release_at,
    source_file,
    source_sha256,
    fetched_at
FROM ranked
WHERE rn = 1
