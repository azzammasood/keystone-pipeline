-- Name history: one current spell per repository, and spells never run backwards.
SELECT repo_id
FROM {{ ref('dim_repo_name_history') }}
GROUP BY repo_id
HAVING
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
    OR MIN(CASE WHEN valid_to IS NOT NULL AND valid_to < valid_from THEN 0 ELSE 1 END) = 0
