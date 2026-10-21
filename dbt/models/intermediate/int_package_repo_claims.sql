-- Every claim that a package lives in a repository, one row per package, source and named
-- repository. Sources: OSV (a GIT range of the package; a FIX reference in a record about
-- this package only, or about several), ecosyste.ms (repository_url) and the npm registry
-- (`repository`, the field the maintainers published). Names are lower case owner/name.
WITH evidence AS (
    SELECT
        e.*,
        {{ canonical_package_key('p.ecosystem', 'p.package_name') }} AS canonical_key
    FROM {{ ref('int_package_repo_evidence') }} AS e
    INNER JOIN {{ ref('dim_package') }} AS p ON e.package_key = p.package_key
),

packages_per_vuln AS (
    -- packages a record names, counted after normalisation (two spellings are one package)
    SELECT
        vuln_id,
        COUNT(DISTINCT canonical_key) AS packages
    FROM evidence
    GROUP BY vuln_id
),

osv AS (
    SELECT
        e.canonical_key AS package_key,
        CASE
            WHEN e.kind = 'git_range' THEN 'osv_git_range'
            WHEN v.packages = 1 THEN 'osv_fix_sole'
            ELSE 'osv_fix_shared'
        END AS source,
        e.repo_host,
        e.repo_full_name,
        e.vuln_id
    FROM evidence AS e
    INNER JOIN packages_per_vuln AS v ON e.vuln_id = v.vuln_id
),

registry AS (
    SELECT
        package_key,
        source,
        repo_host,
        repo_full_name,
        repo_subpath IS NOT NULL AS in_subpath,
        repo_github_id,
        repo_current_name
    FROM {{ ref('stg_registry__packages') }}
    WHERE found AND repo_full_name IS NOT NULL
)

SELECT
    package_key,
    source,
    repo_host,
    repo_full_name,
    FALSE AS in_subpath,
    CAST(NULL AS BIGINT) AS ecosystems_repo_id,
    CAST(COUNT(DISTINCT vuln_id) AS BIGINT) AS evidence
FROM osv
GROUP BY package_key, source, repo_host, repo_full_name

UNION ALL

SELECT
    package_key,
    source,
    repo_host,
    repo_full_name,
    in_subpath,
    repo_github_id AS ecosystems_repo_id,
    CAST(1 AS BIGINT) AS evidence
FROM registry
