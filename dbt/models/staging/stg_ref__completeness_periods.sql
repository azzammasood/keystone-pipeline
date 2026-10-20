-- Completeness periods (ADR-012) as half-open UTC ranges [start_at, end_at).
SELECT
    period AS completeness_period,
    label,
    CAST(start_at AS TIMESTAMP) AS start_at,
    LEAD(CAST(start_at AS TIMESTAMP)) OVER (ORDER BY start_at) AS end_at
FROM {{ ref('completeness_periods') }}
