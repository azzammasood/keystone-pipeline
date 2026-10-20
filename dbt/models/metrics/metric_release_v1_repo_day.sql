-- Release and membership inputs. era-map.md lists both for E1 to E3 only, so E0 is left out.
-- Only days with a release or a member added.
SELECT
    activity_date,
    repo_id,
    era,
    completeness_period,
    'release_v1' AS metric_version,
    releases_published,
    members_added
FROM {{ ref('fct_repo_day') }}
WHERE era IN ('E1', 'E2', 'E3') AND (releases_published > 0 OR members_added > 0)
