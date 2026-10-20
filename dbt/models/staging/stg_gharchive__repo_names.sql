-- Repository identity per event: the only silver columns the name history needs.
SELECT
    repo_id,
    repo_name,
    era,
    created_at
FROM {{ source('silver', 'gharchive_events') }}
WHERE repo_name IS NOT NULL
