-- Schema eras (docs/era-map.md) as half-open UTC ranges [start_at, end_at) of file hours.
SELECT
    era,
    label,
    CAST(start_at AS TIMESTAMP) AS start_at,
    LEAD(CAST(start_at AS TIMESTAMP)) OVER (ORDER BY start_at) AS end_at
FROM {{ ref('schema_eras') }}
