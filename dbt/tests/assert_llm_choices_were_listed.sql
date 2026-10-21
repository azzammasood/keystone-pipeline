-- A language-model choice is one of the candidates it was shown and one the rules list.
SELECT l.package_key
FROM {{ ref('stg_resolve__llm_choices') }} AS l
WHERE
    l.outcome = 'chosen'
    AND (
        l.repo_key IS NULL
        OR ('|' || l.candidates || '|') NOT LIKE ('%|' || l.repo_key || '|%')
        OR NOT EXISTS (
            SELECT 1 FROM {{ ref('map_package_repo') }} AS m
            WHERE m.package_key = l.package_key AND m.repo_key = l.repo_key
        )
    )
