-- Lakeflow Declarative Pipeline over the dbt marts on Databricks (ADR-067). One materialized
-- view: weekly activity per repository and era, with an expectation that drops impossible rows.
-- Repository level only; counts are signals, not predictions.
CREATE OR REFRESH MATERIALIZED VIEW repo_week (
    CONSTRAINT events_not_negative EXPECT (events >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Weekly repository activity from marts.fct_repo_day (Keystone, Lakeflow rung).'
AS SELECT
    DATE_TRUNC('WEEK', activity_date) AS week_start,
    repo_id,
    era,
    CAST(SUM(events) AS BIGINT) AS events,
    CAST(SUM(human_events) AS BIGINT) AS human_events,
    CAST(SUM(pushes) AS BIGINT) AS pushes,
    CAST(SUM(releases_published) AS BIGINT) AS releases_published,
    COUNT(*) AS active_days
FROM marts.fct_repo_day  -- the pipeline's catalog (DATABRICKS_CATALOG)
GROUP BY DATE_TRUNC('WEEK', activity_date), repo_id, era;
