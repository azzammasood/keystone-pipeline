-- Hourly rollup with the completeness period of its hour (a key, like era: never mixed).
SELECT
    h.window_start,
    h.repo_id,
    h.era,
    p.completeness_period,
    h.event_type,
    h.events,
    h.human_events,
    h.opened,
    h.closed,
    h.reopened,
    h.merged,
    h.merge_known,
    h.published,
    h.added,
    h.fetched_at_max
FROM {{ source('rollup', 'repo_hour') }} AS h
INNER JOIN {{ ref('stg_ref__completeness_periods') }} AS p
    ON
        h.window_start >= p.start_at
        AND (p.end_at IS NULL OR h.window_start < p.end_at)
