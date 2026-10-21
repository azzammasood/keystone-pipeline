-- A rename changes the name: never a row from a name to itself.
SELECT repo_id, from_name
FROM {{ ref('dim_repo_rename') }}
WHERE from_name = to_name
