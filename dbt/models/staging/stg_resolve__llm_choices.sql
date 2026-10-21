-- The latest validated answer of the language-model rung per package. `refused` answers
-- (a candidate that was not listed) and `declined` ones never choose anything.
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY package_key ORDER BY decided_at DESC) AS rn
    FROM {{ source('silver', 'resolve_llm_choices') }}
)

SELECT
    package_key,
    candidate AS repo_key,
    confidence,
    outcome,
    model,
    candidates,
    decided_at
FROM ranked
WHERE rn = 1
